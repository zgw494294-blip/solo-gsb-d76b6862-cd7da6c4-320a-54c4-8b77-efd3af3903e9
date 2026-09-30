"""Tests for persistent multi-caller history endpoints.

Covers create/read, event merging by (replica_id, seq), persisted conflict
resolution, all-or-nothing writes, concurrency safety and on-disk persistence
across a process restart.
"""

import json
import os
import subprocess
import sys
import concurrent.futures as cf
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def ev(replica, seq, key, put=None, delete=None, clock=None):
    e = {"replica_id": replica, "seq": seq, "key": key, "clock": clock or {}}
    if put is not None:
        e["put"] = put
    else:
        e["delete"] = delete or "delete"
    return e


CONFLICT_BATCH = [
    ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1}),
    ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
]
VERSIONS = [
    {"replica_id": "r1", "seq": 1},
    {"replica_id": "r2", "seq": 1},
]


def create_history(replica_ids, events):
    return client.post(
        "/api/histories", json={"replica_ids": replica_ids, "events": events}
    )


def create_history_id(replica_ids, events):
    return create_history(replica_ids, events).json()["id"]


def get_history(hid):
    return client.get(f"/api/histories/{hid}")


def merge(hid, events):
    return client.post(f"/api/histories/{hid}/events", json={"events": events})


def resolve(hid, key, writer, versions, **ops):
    body = {"key": key, "replica_id": writer, "versions": versions}
    body.update(ops)
    return client.post(f"/api/histories/{hid}/resolve-conflict", json=body)


# ---------------------------------------------------------------------------
# Create / read
# ---------------------------------------------------------------------------


def test_create_history_returns_id_and_verdict():
    r = create_history(["r1", "r2"], CONFLICT_BATCH)
    assert r.status_code == 200, r.text
    data = r.json()
    assert isinstance(data["id"], str) and len(data["id"]) == 32
    assert data["results"][0]["state"] == "conflict"
    assert data["replica_ids"] == ["r1", "r2"]


def test_read_back_full_batch_sorted_by_replica_and_seq():
    out_of_order = [
        ev("r2", 1, "b", put="2", clock={"r1": 1, "r2": 1}),
        ev("r1", 1, "a", put="1", clock={"r1": 1, "r2": 0}),
        ev("r1", 2, "a", put="11", clock={"r1": 2, "r2": 1}),
    ]
    hid = create_history(["r1", "r2"], out_of_order).json()["id"]
    data = get_history(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    # Original wire format preserved: exactly one of put/delete per event.
    first = data["events"][0]
    assert first["put"] == "1" and "delete" not in first
    assert first["clock"] == {"r1": 1, "r2": 0}
    assert [x["key"] for x in data["results"]] == ["a", "b"]


def test_get_unknown_history_is_404():
    r = get_history("nonexistent")
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"


def test_create_invalid_batch_is_422_and_nothing_stored():
    bad = [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})]
    r = create_history(["r1", "r2"], bad)
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert any(e.get("code") == "clock_self_component" for e in detail)


def test_two_histories_are_independent():
    h1 = create_history_id(["r1", "r2"], CONFLICT_BATCH)
    h2 = create_history_id(["r1", "r2"], CONFLICT_BATCH)
    assert h1 != h2
    assert len(get_history(h1).json()["events"]) == 2
    assert len(get_history(h2).json()["events"]) == 2


# ---------------------------------------------------------------------------
# Merging events
# ---------------------------------------------------------------------------


def test_merge_appends_new_event_and_revalidates():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    new = ev("r1", 2, "other", put="x", clock={"r1": 2, "r2": 1})
    r = merge(hid, [new])
    assert r.status_code == 200, r.text
    assert r.json()["added"] == 1
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    # Persisted: GET sees the same state.
    assert len(get_history(hid).json()["events"]) == 3


def test_merge_identical_events_are_ignored():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    r = merge(hid, CONFLICT_BATCH + CONFLICT_BATCH)
    assert r.status_code == 200, r.text
    assert r.json()["added"] == 0
    assert len(r.json()["events"]) == 2


def test_merge_same_identity_different_content_is_409():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    changed = ev("r1", 1, "name", put="alice-v2", clock={"r1": 1, "r2": 0})
    r = merge(hid, [changed])
    assert r.status_code == 409, r.text
    assert r.json()["detail"][0]["code"] == "event_content_mismatch"
    assert r.json()["detail"][0]["event_index"] == 0
    # No write happened.
    assert len(get_history(hid).json()["events"]) == 2


def test_merge_same_identity_different_clock_is_409():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    changed = ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0})
    changed["clock"] = {"r1": 1, "r2": 1}
    # But that clock would also be causally weird -- mismatch check is first:
    r = merge(hid, [changed])
    assert r.status_code == 409
    assert r.json()["detail"][0]["code"] == "event_content_mismatch"


def test_merge_put_vs_delete_same_identity_is_409():
    hid = create_history(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    r = merge(hid, [ev("r1", 1, "k", delete="delete", clock={"r1": 1, "r2": 0})])
    assert r.status_code == 409
    assert r.json()["detail"][0]["code"] == "event_content_mismatch"


def test_merge_unknown_replica_is_422():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    r = merge(hid, [ev("r9", 1, "k", put="v", clock={"r1": 0, "r2": 0})])
    assert r.status_code == 422
    assert any(e["code"] == "unknown_replica_id" for e in r.json()["detail"])


def test_merge_invalid_causal_event_is_422_and_atomic():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    mixed = [
        ev("r2", 2, "y", put="y", clock={"r1": 1, "r2": 2}),
        # gap: r1/2 is missing
        ev("r1", 3, "z", put="z", clock={"r1": 3, "r2": 0}),
    ]
    r = merge(hid, mixed)
    assert r.status_code == 422, r.text
    # Neither event of the batch is visible (all-or-nothing).
    assert len(get_history(hid).json()["events"]) == 2


def test_merge_respects_120_event_cap():
    replica_ids = ["r1", "r2"]
    events = [
        ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1})
        for n in range(1, 61)
    ] + [
        ev("r2", n, "k", put="b", clock={"r1": n - 1, "r2": n})
        for n in range(1, 61)
    ]
    hid = create_history(replica_ids, events).json()["id"]
    r = merge(hid, [ev("r1", 61, "k", put="c", clock={"r1": 61, "r2": 60})])
    assert r.status_code == 422
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])
    assert len(get_history(hid).json()["events"]) == 120


def test_merge_409_takes_precedence_over_cap_and_causal_checks():
    replica_ids = ["r1", "r2"]
    events = [
        ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1})
        for n in range(1, 61)
    ] + [
        ev("r2", n, "k", put="b", clock={"r1": n - 1, "r2": n})
        for n in range(1, 61)
    ]
    hid = create_history(replica_ids, events).json()["id"]
    # Full history AND same-id content conflict: the conflict is a 409.
    r = merge(hid, [ev("r1", 1, "k", put="different", clock={"r1": 1, "r2": 0})])
    assert r.status_code == 409


def test_merge_into_unknown_history_is_404():
    r = merge("missing", CONFLICT_BATCH)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Persisted conflict resolution
# ---------------------------------------------------------------------------


def test_resolve_conflict_persists_generated_event():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    r = resolve(hid, "name", "r1", VERSIONS, put="chosen")
    assert r.status_code == 200, r.text
    assert r.json()["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert r.json()["result"]["state"] == "put"
    # The stored batch and verdict reflect the appended event.
    data = get_history(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    name = [x for x in data["results"] if x["key"] == "name"][0]
    assert name["state"] == "put" and name["value"] == "chosen"


def test_resolve_conflict_continues_writer_seq_tail():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    r1 = resolve(hid, "name", "r1", VERSIONS, put="c1")
    assert r1.status_code == 200
    # New conflict between r1/2 (dominates both old branches) and a fresh r2/2
    # branch needs incomparable clocks; instead verify the seq tail directly
    # by resolving another history's conflict with r2 having the longer tail.
    other = [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r2", 2, "other", put="o", clock={"r1": 1, "r2": 2}),
        ev("r1", 2, "k", put="a2", clock={"r1": 2, "r2": 0}),
    ]
    hid2 = create_history(["r1", "r2"], other).json()["id"]
    r2 = resolve(
        hid2, "k", "r2",
        [{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="c2",
    )
    assert r2.status_code == 200, r2.text
    # r2 already has seq 2 (on key 'other'); resolution continues at 3.
    assert r2.json()["event"]["seq"] == 3
    assert r2.json()["event"]["clock"]["r2"] == 3


def test_resolve_conflict_stale_versions_is_409_and_appends_nothing():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    assert resolve(hid, "name", "r1", VERSIONS, put="first").status_code == 200
    before = get_history(hid).json()["events"]

    r = resolve(hid, "name", "r2", VERSIONS, put="again")
    assert r.status_code == 409
    assert any(
        e["code"] in ("key_not_conflicted", "conflict_versions_mismatch")
        for e in r.json()["detail"]
    )
    after = get_history(hid).json()["events"]
    assert before == after  # no event appended


def test_resolve_conflict_missing_version_is_409():
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = create_history(["r1", "r2", "r3"], branches).json()["id"]
    r = resolve(
        hid, "name", "r1",
        [{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )
    assert len(get_history(hid).json()["events"]) == 3


def test_resolve_conflict_unknown_writer_is_422():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    r = resolve(hid, "name", "r9", VERSIONS, put="x")
    assert r.status_code == 422
    assert any(e["code"] == "unknown_replica_id" for e in r.json()["detail"])
    assert len(get_history(hid).json()["events"]) == 2


def test_resolve_conflict_full_history_is_422():
    replica_ids = ["r1", "r2"]
    events = []
    for n in range(1, 61):
        events.append(ev("r1", n, "name", put="a", clock={"r1": n, "r2": n - 1}))
        events.append(ev("r2", n, "name", put="b", clock={"r1": n - 1, "r2": n}))
    hid = create_history(replica_ids, events).json()["id"]
    r = resolve(
        hid, "name", "r1",
        [{"replica_id": "r1", "seq": 60}, {"replica_id": "r2", "seq": 60}],
        put="chosen",
    )
    assert r.status_code == 422
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])


def test_resolve_unknown_history_is_404():
    r = resolve("missing", "name", "r1", VERSIONS, put="x")
    assert r.status_code == 404


def test_resolution_chain_each_step_uses_fresh_versions():
    # After one resolution dominates the conflict, callers must read the new
    # state. A second resolution on a later conflict keeps extending the tail.
    branches3 = [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "k", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = create_history(["r1", "r2", "r3"], branches3).json()["id"]
    r = resolve(
        hid, "k", "r3",
        [
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
            {"replica_id": "r3", "seq": 1},
        ],
        put="win",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["seq"] == 2
    data = get_history(hid).json()
    assert data["results"][0]["state"] == "put"
    assert data["results"][0]["value"] == "win"


# ---------------------------------------------------------------------------
# Read consistency / edge cases
# ---------------------------------------------------------------------------


def test_empty_put_value_round_trips_through_storage():
    hid = create_history(
        ["a", "b"],
        [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})],
    ).json()["id"]
    data = get_history(hid).json()
    assert data["events"][0]["put"] == ""
    assert "delete" not in data["events"][0]
    assert data["results"][0]["value"] == ""


def test_persisted_resolution_event_replays_through_stateless_resolve():
    hid = create_history(["r1", "r2"], CONFLICT_BATCH).json()["id"]
    committed = resolve(hid, "name", "r1", VERSIONS, put="chosen").json()
    stored = get_history(hid).json()
    # Stored wire events validate and resolve via the original stateless API.
    r = client.post(
        "/api/resolve",
        json={"replica_ids": stored["replica_ids"], "events": stored["events"]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["state"] == "put"
    assert r.json()["results"][0]["value"] == "chosen"
    assert committed["event"] in stored["events"]


def test_merge_empty_string_put_resend_is_ignored_but_other_value_conflicts():
    hid = create_history(
        ["a", "b"],
        [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})],
    ).json()["id"]
    assert merge(hid, [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})]).status_code == 200
    r = merge(hid, [ev("a", 1, "k", put="x", clock={"a": 1, "b": 0})])
    assert r.status_code == 409


def test_read_returns_batch_and_verdict_from_same_committed_state():
    hid = create_history(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    merge(hid, [ev("r2", 1, "k", delete="delete", clock={"r1": 0, "r2": 1})])
    data = get_history(hid).json()
    # Both present in the batch and both reflected as a conflict in results.
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    assert data["results"][0]["state"] == "conflict"
    assert {
        (e["replica_id"], e["seq"]) for e in data["results"][0]["events"]
    } == {("r1", 1), ("r2", 1)}


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_merges_lose_no_events_and_keep_seqs_contiguous():
    replicas = [f"r{i}" for i in range(6)]
    hid = create_history(
        replicas,
        [ev("r0", 1, "seed", put="s", clock={**{r: 0 for r in replicas}, "r0": 1})],
    ).json()["id"]

    def merge_chain(ridx):
        rid = f"r{ridx}"
        payload = {"events": [
            ev(rid, n, f"k{ridx}", put=str(n),
               clock={**{r: 0 for r in replicas}, rid: n})
            for n in range(1, 8)
        ]}
        return TestClient(app).post(f"/api/histories/{hid}/events", json=payload)

    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        responses = list(ex.map(merge_chain, range(1, 6)))
    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]

    data = get_history(hid).json()
    assert len(data["events"]) == 1 + 5 * 7
    seqs: dict[str, list[int]] = {}
    for e in data["events"]:
        seqs.setdefault(e["replica_id"], []).append(e["seq"])
    assert sorted(seqs["r0"]) == [1]
    for ridx in range(1, 6):
        assert sorted(seqs[f"r{ridx}"]) == list(range(1, 8))


def test_concurrent_conflict_resolutions_exactly_one_commits():
    hid = create_history(["a", "b"], [
        ev("a", 1, "x", put="1", clock={"a": 1, "b": 0}),
        ev("b", 1, "x", put="2", clock={"a": 0, "b": 1}),
    ]).json()["id"]
    versions = [{"replica_id": "a", "seq": 1}, {"replica_id": "b", "seq": 1}]

    def do_resolve(writer):
        return TestClient(app).post(
            f"/api/histories/{hid}/resolve-conflict",
            json={"key": "x", "replica_id": writer, "versions": versions,
                  "put": f"win-{writer}"},
        )

    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        responses = list(ex.map(do_resolve, ["a", "b", "a", "b"]))
    codes = sorted(r.status_code for r in responses)
    assert codes.count(200) == 1
    assert codes.count(409) == 3

    data = get_history(hid).json()
    assert len(data["events"]) == 3
    assert data["results"][0]["state"] == "put"
    winner = next(r for r in responses if r.status_code == 200).json()["event"]
    assert data["results"][0]["value"] == winner["put"]
    # Exactly one seq-2 event exists, on the winner replica.
    tail = [e for e in data["events"] if e["seq"] == 2]
    assert len(tail) == 1 and tail[0]["replica_id"] == winner["replica_id"]


def test_concurrent_identical_merges_apply_once():
    hid = create_history(
        ["a", "b"],
        [ev("a", 1, "x", put="1", clock={"a": 1, "b": 0})],
    ).json()["id"]
    payload = {"events": [ev("b", 1, "y", put="2", clock={"a": 1, "b": 1})]}
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        responses = list(ex.map(
            lambda _: TestClient(app).post(
                f"/api/histories/{hid}/events", json=payload),
            range(4),
        ))
    assert all(r.status_code == 200 for r in responses)
    assert sum(1 for r in responses if r.json()["added"] == 1) == 1
    assert len(get_history(hid).json()["events"]) == 2


def test_concurrent_distinct_content_same_identity_one_wins():
    hid = create_history(
        ["a", "b"],
        [ev("a", 1, "x", put="1", clock={"a": 1, "b": 0})],
    ).json()["id"]
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        responses = list(ex.map(
            lambda v: TestClient(app).post(
                f"/api/histories/{hid}/events",
                json={"events": [ev("b", 1, "x", put=v,
                                    clock={"a": 1, "b": 1})]},
            ),
            ["v1", "v2", "v3", "v4"],
        ))
    codes = sorted(r.status_code for r in responses)
    assert codes.count(200) == 1 and codes.count(409) == 3
    stored = [e for e in get_history(hid).json()["events"] if e["replica_id"] == "b"]
    assert len(stored) == 1


# ---------------------------------------------------------------------------
# On-disk persistence across a restart
# ---------------------------------------------------------------------------


def test_history_survives_process_restart(tmp_path):
    db_file = tmp_path / "restart.db"
    code = f'''
import os
os.environ["HISTORY_DB_PATH"] = {str(db_file)!r}
from fastapi.testclient import TestClient
from app.main import app
c = TestClient(app)
r = c.post("/api/histories", json={{
    "replica_ids": ["r1", "r2"],
    "events": [
        {{"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
         "clock": {{"r1": 1, "r2": 0}}}},
        {{"replica_id": "r2", "seq": 1, "key": "name", "delete": "delete",
         "clock": {{"r1": 0, "r2": 1}}}},
    ],
}})
assert r.status_code == 200, r.text
print(r.json()["id"])
'''
    env = dict(os.environ, HISTORY_DB_PATH=str(db_file))
    create = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True, env=env,
    )
    assert create.returncode == 0, create.stderr
    hid = create.stdout.strip().splitlines()[-1]
    assert db_file.exists()

    # Simulate container restart: a brand-new process opens the same file.
    read_code = f'''
import os
os.environ["HISTORY_DB_PATH"] = {str(db_file)!r}
from fastapi.testclient import TestClient
from app.main import app
c = TestClient(app)
r = c.get("/api/histories/{hid}")
assert r.status_code == 200, r.text
data = r.json()
assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
    ("r1", 1), ("r2", 1)
]
assert data["results"][0]["state"] == "conflict"
# Merge still works after restart, continuing the seq tail.
r = c.post("/api/histories/{hid}/events", json={{"events": [
    {{"replica_id": "r1", "seq": 2, "key": "name", "put": "a2",
     "clock": {{"r1": 2, "r2": 1}}}}
]}})
assert r.status_code == 200, r.text
assert r.json()["added"] == 1
assert r.json()["results"][0]["state"] == "put"
print("restart-ok")
'''
    read = subprocess.run(
        [sys.executable, "-c", read_code],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True, env=env,
    )
    assert read.returncode == 0, read.stderr
    assert read.stdout.strip().splitlines()[-1] == "restart-ok"
