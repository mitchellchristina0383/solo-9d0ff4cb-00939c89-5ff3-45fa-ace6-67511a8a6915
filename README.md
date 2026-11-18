# HTTP 入口网关（在途请求合并 / 优雅排空 / 健康探测 / 轮询转发）

滚动更新与热点数据场景下的入口网关：

* **在途请求合并（singleflight）**：多名客户端同时取同一份数据时，只有选中
  **同一节点、同一路径、完整查询串完全一致** 的 GET 才共用一次上游调用，
  响应状态、头和正文仍分别返回给每个客户端；调用结束即清除合并状态，**不缓存**。
* **优雅排空**：运维人员先排空待停机实例，使新请求不再进入（也不能加入该节点的
  旧在途调用），同时保证已在途的上游调用完整跑完，全部结束后才报告排空完成。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个可模拟
慢请求 / 故障、并统计**实际调用次数**的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2
├── gateway/
│   ├── app.py              # 网关：在途合并、仅转发 GET、轮询、探测、排空/恢复/状态
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式 / 实际调用计数
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    └── self_test.py        # 本地端到端自测（20 组场景，92 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 同节点+同路径+同完整查询串的在途请求合并为一次上游调用
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
                       # 响应头 X-Gateway-Coalesced 标记共用本次调用的客户端数
```

管理：

```
GET  /admin/status                     # 健康/排空/在途/实际调用计数/合并计数/最近探测
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流（含禁止加入旧调用），
                                       #   阻塞到实际在途调用清零
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式、在途数与累计实际调用次数（含按路径分项）
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断、合并不被客户端断开取消）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 三个并发相同请求只产生 1 次上游调用（看 X-Gateway-Coalesced 与 /control/state）
for i in 1 2 3; do curl -s "http://localhost:18080/proxy/slow?seconds=5&share=1" & done
curl -s http://localhost:18081/control/state     # path_calls./slow 只 +1

# 2. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 3. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 4. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流
```

## 关键设计与并发正确性

### 在途请求合并

- **合并键严格**：`(节点, 路径, 原始查询串)` 三者逐字节一致才合并；查询串参数
  顺序不同、值不同、路径不同均视为不同调用。选路与合并判定在同一把
  `asyncio.Condition` 锁内原子完成，不存在“判定后被改状态”的窗口。
- **合并优先于轮询**：请求先在可接流节点上查找同键在途调用；命中则作为等待者
  加入，**不新增上游调用、不计入在途**；未命中才按原有轮询选路并创建新调用。
- **调用与客户端生命周期解耦**：上游调用运行在独立 `asyncio.Task` 中，客户端
  只通过 `asyncio.shield(Event.wait())` 等待。因此**首位客户端断开不会中断其他
  等待者**；即使所有客户端都断开，已发出的上游调用仍会跑完并完成收尾。
- **响应各自独立**：一次调用得到的状态码、头、正文被复制到每个客户端自己的
  `Response`；调用一结束就从合并表删除，**不做任何缓存**，后续相同请求是新调用。
- **在途只按实际调用计数**：`inflight` 只在发起新调用时 +1，合并等待者仅累计到
  `merged_requests`；因此排空等待的是真实上游调用，**不会因等待者消失而提前
  回报**。
- **排空 / 故障后不加入旧调用**：排空或探测故障的节点 `can_serve()=False`，
  合并扫描与轮询都跳过它，新请求按原有选路去其他节点，绝不可能加入其旧调用。
- **失败如实返回且状态可重试**：超时（504）、连接错误（502）、5xx 都分发给
  全部等待者并即时降级；合并状态随调用结束被清除，后续请求可立即发起新调用重试。

### 健康探测与排空

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **轮询 + 原子登记**：在同一把 `asyncio.Condition` 锁内完成选节点与
  `inflight += 1`，保证“选中即可接流、计数即生效”。
- **在途计数可靠**：实际调用在独立任务的 `try/finally` 中归还计数，成功、5xx、
  超时、连接错误、全部客户端取消、进程关闭都会准确递减；排空等待在条件变量上，
  调用一结束即被唤醒。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py
```

覆盖 20 组场景、92 项断言。原有 15 组（轮询、探测/排空交错、503/504/502、
在途计数、405、/docs 等）保持通过，新增 5 组：

- **16 在途合并**：3 个相同请求只产生 1 次上游调用，响应各自完整，结束不缓存；
- **17 合并键严格性**：查询串不同 / 路径不同 / 参数顺序不同均不合并；
- **18 取消语义**：首位客户端断开不影响等待者；全部断开上游调用仍完整收尾；
- **19 合并 + 排空**：在途只按实际调用计 1，排空阻塞到调用结束，
  排空后新请求按原有选路去别处；
- **20 失败分发与重试**：500 如实分发给所有等待者，合并状态清除，重试产生新调用。
