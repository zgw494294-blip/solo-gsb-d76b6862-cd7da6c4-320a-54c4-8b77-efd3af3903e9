# 多副本离线写入核对 API

接收 2～8 个副本离线产生的 1～120 条 put/delete 事件（可乱序），校验
向量时钟因果关系，并按向量时钟偏序解析出每个 key 的有效版本：

- 唯一极大事件是 put → 返回其值
- 唯一极大事件是 delete → 标记为已删除
- 存在多个极大事件（互不可比，即使值相同）→ 报告冲突

此外，`/api/catchup` 接口在同一批事件之上，为目标副本生成**可直接重放的
增量补传计划**：传入目标对各来源已连续接收的最高 seq（`known_clock`），
缺失事件按依赖分轮返回。

`/api/resolve-conflict` 接口把**已确认的冲突处理结果**写成一条新的、
可重放的事件：在写入副本上追加 put/delete，其时钟并入所有冲突版本及该副本
此前见过的历史；返回新事件与追加后该 key 的裁决。

以上三个接口完全无状态，**请求数据不落库**。

此外，本服务提供**持久化历史**接口，让多个调用方把离线事件汇入同一份
保存在本地 SQLite 中的历史，并对该历史提交冲突处理：

- `POST /api/histories`：用与 `/api/resolve` 完全相同的规则校验一个合法
  批次并落库，返回历史 ID；
- `GET /api/histories/{id}`：读取完整原格式批次（事件按 `replica_id`、
  `seq` 排序）及该状态下的裁决，二者来自同一个已提交快照；
- `POST /api/histories/{id}/events`：按 `(replica_id, seq)` 汇入更多事件，
  内容完全相同的事件视为重发直接忽略，同一标识但内容不同返回 **409**；
  合并后仍沿用原校验规则与 120 条上限，违例返回 **422**；
- `POST /api/histories/{id}/resolve-conflict`：只提交原冲突处理参数
  （不再传整个批次）；提交时核对**当前** versions、接续写入副本序号生成并
  保存事件；versions 过期返回 **409**。

每次写入都是单条 SQLite 事务，**全部生效或全部不生效**；写事务串行化，
并发汇入/提交不会丢失事件、重复占用序号，也不可能用过期 versions 覆盖冲突。
数据库文件位于 Compose 命名卷，**容器重启后历史仍可读取**。

## 启动

需要 Docker 与 Docker Compose v2（`docker compose` 子命令）。

```bash
docker compose up --build -d
```

停止：

```bash
docker compose down
```

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| 健康检查 | <http://localhost:8000/health> |
| 裁决接口 | `POST http://localhost:8000/api/resolve` |
| 冲突处理事件接口 | `POST http://localhost:8000/api/resolve-conflict` |
| 增量补传接口 | `POST http://localhost:8000/api/catchup` |
| 创建持久化历史 | `POST http://localhost:8000/api/histories` |
| 读取历史（批次+裁决） | `GET http://localhost:8000/api/histories/{id}` |
| 汇入事件 | `POST http://localhost:8000/api/histories/{id}/events` |
| 提交冲突处理 | `POST http://localhost:8000/api/histories/{id}/resolve-conflict` |
| Swagger UI | <http://localhost:8000/docs> |
| OpenAPI 文档 | <http://localhost:8000/openapi.json> |

容器内监听 `0.0.0.0:8000`，映射到宿主机 **8000** 端口。

## 请求格式

```json
{
  "replica_ids": ["r1", "r2"],
  "events": [
    {
      "replica_id": "r1",
      "seq": 1,
      "key": "name",
      "put": "alice",
      "clock": {"r1": 1, "r2": 0}
    },
    {
      "replica_id": "r2",
      "seq": 1,
      "key": "name",
      "delete": "delete",
      "clock": {"r1": 0, "r2": 1}
    }
  ]
}
```

约束：

- `replica_ids`：2～8 个非空且唯一的副本 ID。
- 每个事件属于其中某个副本；`seq` 从 1 开始，同一副本内不重复且必须连续
  （事件在请求中可乱序）。
- `put`（字符串，允许空串）与 `delete`（值必须是字符串 `"delete"`）二选一。
- `clock` 必须覆盖**全部**副本且无多余分量，分量为非负整数；
  本副本分量必须等于本事件 `seq`。
- 时钟任何分量都不得指向不存在的事件（0 表示尚不知道该副本的事件）。
- 本副本上一事件（`seq - 1`）以及其他副本分量所指的最新事件，其时钟必须
  逐分量 `<=` 当前时钟且至少一个分量严格更小。
- 任何违例整批返回 **HTTP 422**，错误详情中带 `event_index`（请求 `events`
  数组下标）指出具体事件。

## 可执行 HTTP 示例

### 1. 冲突：两个副本并发改同一个 key（值相同也冲突）

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

响应（结果按 key 排序；冲突事件按 `replica_id`、`seq` 排序）：

```json
{
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 2. 同步后再写入：put 生效、旧 delete 被遮蔽

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "session", "delete": "delete",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice2",
       "clock": {"r1": 3, "r2": 1}}
    ]
  }'
```

响应：

```json
{
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice2",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice2",
                "clock": {"r1": 3, "r2": 1}}
    },
    {
      "key": "session",
      "state": "deleted",
      "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                "clock": {"r1": 2, "r2": 0}}
    }
  ]
}
```

### 3. 非法批次：时钟本副本分量不等于 seq → 422

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "v",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body 例如：

```json
{
  "detail": [
    {
      "code": "clock_self_component",
      "message": "clock['r1'] = 2 must equal seq = 1",
      "event_index": 0
    }
  ]
}
```

## 冲突处理事件接口 `POST /api/resolve-conflict`

把确认过的冲突处理结果写成一条**可重放事件**。请求沿用裁决接口的
`replica_ids`、`events`（先按原规则整批校验），并额外携带：

- `key`：要消解的冲突 key（非空字符串）；
- `replica_id`：写入副本，必须属于 `replica_ids`；
- `versions`：当时看到的冲突版本列表，每项含 `replica_id`、`seq`；
  列表须无重复且**恰好等于**该 key 当前全部极大事件，且至少两项
  （列表顺序任意）；
- 处理结果二选一：`put`（字符串，允许空串）或 `delete`（值必须为
  字符串 `"delete"`）。

接口行为：

- 新事件的 `seq` 接续写入副本现有末尾（该副本此前无事件时为 `1`）；
- 新事件时钟逐分量取所有冲突版本时钟与写入副本此前历史的最大值，
  再把写入副本自身分量推进到新 `seq`——因此新事件严格支配全部冲突版本；
- 响应包含完整新事件 `event`（与事件请求线格式一致：`put` 或
  `delete` 二选一），以及追加后的该 key 裁决 `result`
  （形状与 `/api/resolve` 的单条结果一致）；
- 即使各冲突版本的值相同，冲突也必须经此接口显式消解；其他 key 的
  裁决不受影响；
- 新事件并入 `events` 后可直接通过原批次校验，并能由 `/api/catchup`
  按依赖安排补传。

状态码：

- **200**：冲突已按确认结果消解；
- **409**：`versions` 已过期/不完整（`conflict_versions_mismatch`），
  或该 key 当前已不冲突/批次中不存在（`key_not_conflicted`）；
  返回 409 时不生成事件；
- **422**：原批次校验失败，或写入副本不在 `replica_ids`
  （`unknown_replica_id`）、版本列表重复
  （`duplicate_conflict_version`）、原批次已满 120 条（`batch_full`）
  等结构性错误。

### 4. 消解并发冲突：追加支配两分支的 put

`r1/1` 与 `r2/1` 并发写入 `name`（两分支互不可比），确认采用 `r1` 写入
的 `"alice2"`；版本列表可以乱序：

```bash
curl -sS -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "alice2"
  }'
```

响应（新事件为 `r1/2`，时钟并入两个冲突分支）：

```json
{
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "alice2",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "alice2",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "alice2",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

把返回的 `event` 追加到原批次后再请求 `/api/resolve`，`name` 即变为
普通 `put`；请求 `/api/catchup` 时它会排在所依赖的冲突版本之后补传。

### 5. 冲突已被新写入遮蔽（版本列表过期）→ 409

批次中 `r1/2` 因果上支配 `r2/1`，`name` 已不再冲突。仍按旧冲突提交时
返回 `409 Conflict`，且不生成事件：

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice",
       "clock": {"r1": 2, "r2": 1}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r1", "seq": 2},
      {"replica_id": "r2", "seq": 1}
    ],
    "put": "alice2"
  }'
```

```json
{
  "detail": [
    {
      "code": "key_not_conflicted",
      "message": "key 'name' is no longer in conflict"
    }
  ]
}
```

若 key 仍冲突但列表缺少某个极大版本、列入了非极大版本，或指向不存在的
`(replica_id, seq)`，返回 `409` 且错误码为 `conflict_versions_mismatch`。

## 增量补传接口 `POST /api/catchup`

请求沿用裁决接口的 `replica_ids`、`events`（按同样规则整批校验），额外携带
`known_clock`：目标对**每个**来源已连续接收的最高 seq。

```json
{
  "replica_ids": ["r1", "r2"],
  "known_clock": {"r1": 1, "r2": 0},
  "events": [ ... ]
}
```

`known_clock` 规则（违例返回 **422**）：

- 必须**恰好覆盖**所有 `replica_ids`，无缺失、无多余键；
- 每个分量为非负整数；
- 分量不得超过该来源在本批次中的最高 seq（批次中没有事件的来源只能填 `0`）；
- 若某个**已接收事件**（自身 `seq <= known_clock[本副本]`）的时钟任一分量
  超过对应的 `known_clock`，返回 `clock_exceeds_known_clock` 并附该事件的
  `event_index`。

缺失事件（`seq > known_clock[本副本]`）按**轮次**返回：

- 每一轮纳入当时所有依赖已满足的事件——其本副本上一事件（`seq - 1`）以及
  其他副本时钟分量所指事件，均已在目标端或在更早轮次中传输；
- 轮内事件按 `replica_id`、`seq` 排序；
- 事件保留原操作（`op`）、`value`（delete 时为 `null`）、`key` 和 `clock`；
- `progress` 为补传完成后目标对各来源的进度（即批次中各来源最高 seq）；
- 全部事件均已接收时 `rounds` 为空数组；
- 并发写入即使值相同也分别补传，不做去重或冲突合并。

### 6. 分轮补传：跨副本依赖逐轮解锁

目标已接收 `r1/1`（时钟 `{r1:1, r2:0}`），缺失 `r2/1`（依赖 `r1/1`）与
`r1/2`（依赖 `r2/1`），因此分为两轮：

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

响应（第 1 轮先补 `r2/1`，第 2 轮再补依赖它的 `r1/2`）：

```json
{
  "rounds": [
    [
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 1}}
    ],
    [
      {"replica_id": "r1", "seq": 2, "key": "name", "op": "put",
       "value": "alice2", "clock": {"r1": 2, "r2": 1}}
    ]
  ],
  "progress": {"r1": 2, "r2": 1}
}
```

### 7. 并发同值写入也分别补传

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 0, "r2": 0},
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

两个事件互不依赖，进入同一轮（按 `replica_id` 排序），即使值相同也都补传：

```json
{
  "rounds": [
    [
      {"replica_id": "r1", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 0, "r2": 1}}
    ]
  ],
  "progress": {"r1": 1, "r2": 1}
}
```

### 8. 全部已接收：轮次为空

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 1},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}}
    ]
  }'
```

```json
{
  "rounds": [],
  "progress": {"r1": 1, "r2": 1}
}
```

### 9. 非法请求：已接收事件的时钟超出 known_clock → 422

目标声称只连续收到 `r1/1` 且不知道 `r2` 的任何事件，但该事件时钟里
`r2 = 1`，与目标认知矛盾：

```bash
curl -sS -i -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body：

```json
{
  "detail": [
    {
      "code": "clock_exceeds_known_clock",
      "message": "event already received (replica 'r1', seq 1 <= known_clock 1) but its clock {'r1': 1, 'r2': 1} exceeds known_clock; components beyond target knowledge: {'r2': 1}",
      "event_index": 0
    }
  ]
}
```

其余 `known_clock` 违例的错误码：`known_clock_coverage`（缺失/多余分量）、
`known_clock_ahead_of_batch`（分量超过批次最高 seq）。

## 持久化历史（多调用方汇入 + 提交冲突处理）

历史保存在本地 SQLite（默认文件 `data/history.db`，可用环境变量
`HISTORY_DB_PATH` 覆盖；Docker Compose 把命名卷 `history-data` 挂载到
`/data`），容器重启或重新创建后历史依然存在。

持久化接口的请求/响应约定：

- 创建历史的请求体与 `/api/resolve` **完全一致**（`replica_ids` +
  `events`），先按原规则整批校验，非法批次返回 **422** 且不落库；
- 成功后返回 `id`，以及 `replica_ids`、按 `(replica_id, seq)` 排序的
  `events`、当前裁决 `results`；
- 之后所有操作只传 `id`，不再重复上传整个批次；
- 汇入接口请求体只有 `events`（1～120 条）；冲突处理接口请求体只有
  `key`、`replica_id`、`versions` 与 `put`/`delete`（形状与无状态
  `/api/resolve-conflict` 除 `replica_ids`、`events` 外完全相同）。

### 10. 创建历史并拿到 ID

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

响应（`events` 已按 `replica_id`、`seq` 排序；`results` 是同一次提交状态
下计算出的裁决）：

```json
{
  "id": "f3bee1617eb1463db19f0e3e466b30c7",
  "replica_ids": ["r1", "r2"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0}},
    {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
     "clock": {"r1": 0, "r2": 1}}
  ],
  "results": [
    {"key": "name", "state": "conflict", "events": [
      {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "op": "put", "value": "bob",
       "clock": {"r1": 0, "r2": 1}}
    ]}
  ]
}
```

把 `id` 记下来（下文以 `$HID` 表示）。

### 11. 按 ID 读取：完整原格式批次 + 同一状态的裁决

```bash
curl -sS http://localhost:8000/api/histories/$HID
```

返回与创建接口相同的 `replica_ids`、`events`（按 `replica_id`、`seq`
排序）、`results`，二者来自同一个已提交快照。ID 不存在返回
**404**（`history_not_found`）。

### 12. 汇入事件：相同内容忽略，同标识异内容 409

另一个调用方把离线产生的新事件按 `(replica_id, seq)` 汇入。汇入会先与
历史中已有事件比对：

- 同标识（`replica_id` + `seq`）且 `key`、操作、值、时钟完全一致 →
  视为重发，忽略（批次内重复也忽略），不计入新增；
- 同标识但任一内容不同 → **409** `event_content_mismatch`，整批不写入；
- 新标识事件合并后，整份历史重新跑一遍原校验（时钟因果、seq 连续等），
  并保持 120 条上限；违例 **422**，整批不写入（全部或全无）。

```bash
curl -sS -X POST http://localhost:8000/api/histories/$HID/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r2", "seq": 2, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 2}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}}
    ]
  }'
```

第二条是历史中已存在的相同事件，被忽略；第一条是新事件。响应中
`added` 为实际新增条数，随后是合并后的完整批次与裁决：

```json
{
  "id": "f3bee1617eb1463db19f0e3e466b30c7",
  "added": 1,
  "replica_ids": ["r1", "r2"],
  "events": [ ... 合并后全部事件，按 replica_id、seq 排序 ... ],
  "results": [ ... 合并后状态下的裁决 ... ]
}
```

若把 `r1/1` 用不同内容（如 `"put": "alice-v2"`）再发一次，返回
`409 Conflict`：

```json
{"detail": [{"code": "event_content_mismatch",
  "message": "event with replica_id 'r1', seq 1 already exists in this history with different content; the same identity cannot be reused for another event",
  "event_index": 0}]}
```

### 13. 提交冲突处理：核对当前 versions、接续序号、生成并保存事件

只提交原冲突处理参数（不需要 `replica_ids`/`events`）。服务在串行化写
事务内重新读取当前状态，校验 `versions` 是否**恰好等于**该 key 当前全部
极大事件，然后在写入副本序号末尾生成新事件并保存——检查与写入在同一事务
内完成：

```bash
curl -sS -X POST http://localhost:8000/api/histories/$HID/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "alice2"
  }'
```

响应含生成的 `event`（接续 `r1` 的末尾，即 `r1/2`）、该 key 的新裁决
`result`，以及更新后的完整批次与裁决：

```json
{
  "id": "f3bee1617eb1463db19f0e3e466b30c7",
  "event": {"replica_id": "r1", "seq": 2, "key": "name",
            "put": "alice2", "clock": {"r1": 2, "r2": 1}},
  "result": {"key": "name", "state": "put", "value": "alice2",
             "event": {"replica_id": "r1", "seq": 2, "op": "put",
                       "value": "alice2", "clock": {"r1": 2, "r2": 1}}},
  "replica_ids": ["r1", "r2"],
  "events": [ ... 含新保存的 r1/2 ... ],
  "results": [ ... 与保存状态一致的裁决 ... ]
}
```

此时 key 已不冲突。再拿旧的 versions（`r1/1`、`r2/1`）提交会返回
**409**（`key_not_conflicted` 或 `conflict_versions_mismatch`），且不
生成、不保存任何事件——并发提交时只有一个调用方成功，其余全部收到 409，
不会重复占用 `r1/2` 这个序号，也不会用过期 versions 覆盖已生效的处理。

状态码汇总（持久化接口）：

| 状态 | 场景 |
| --- | --- |
| 200 | 创建 / 读取 / 汇入（`added` 可能为 0）/ 冲突处理成功 |
| 404 | ID 不存在（`history_not_found`） |
| 409 | 同标识异内容（`event_content_mismatch`）；versions 过期/不完整（`conflict_versions_mismatch`）；key 已不冲突（`key_not_conflicted`） |
| 422 | 原批次/合并后批次校验失败、超过 120 条（`batch_full`）、写入副本未知（`unknown_replica_id`）、版本列表重复（`duplicate_conflict_version`）等 |

### 14. 容器重启后历史仍在

数据保存在命名卷 `history-data` 中，重启不丢：

```bash
docker compose restart api
curl -sS http://localhost:8000/api/histories/$HID   # 仍返回完整批次与裁决
```

`docker compose down`（不加 `-v`）同样保留卷；`docker compose down -v`
才会删除历史数据。

## 本地开发（不使用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

持久化历史默认写入工作目录下的 `data/history.db`（首次启动自动创建），
可用 `HISTORY_DB_PATH` 环境变量指定其他位置：

```bash
HISTORY_DB_PATH=/tmp/history.db uvicorn app.main:app --reload --port 8000
```
