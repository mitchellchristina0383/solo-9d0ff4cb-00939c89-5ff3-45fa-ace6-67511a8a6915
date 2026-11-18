"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 在途请求合并（singleflight / coalescing）：只有选中同一节点、路径与完整查询串
    完全一致的请求才能共用一次上游调用；响应状态、头和正文仍分别返回给各客户端，
    调用结束即清除合并状态，不做任何缓存；
  * 上游调用运行在独立任务中，与客户端连接解耦：首位（乃至全部）客户端断开都不会
    中断已发出的上游调用，其他等待者仍能拿到完整响应；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，也不能加入该节点的旧在途调用；已在途的实际
    上游调用继续跑完，在途计数只按实际调用计数，排空不会因等待者消失而提前完成；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态、清除合并状态，
    使后续请求可以发起新的调用进行重试；在途计数靠 try/finally 保证准确。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


# ----------------------------- 数据结构 -----------------------------

@dataclass
class UpstreamResult:
    """一次实际上游调用的收尾结果，供所有合并等待者各自独立组装响应。"""

    kind: str  # response | timeout | request_error | unexpected
    status: int
    # 用 list[tuple] 保留重复头；错误响应时为 None
    headers: Optional[list[tuple[str, str]]]
    content: bytes
    detail: str = ""


class CoalescedCall:
    """一次实际的上游调用及其合并等待者。

    调用本身在独立 asyncio.Task 中执行，与任何客户端请求任务解耦：
    客户端只等待 ``event``，客户端断开只取消它自己的等待，不会触碰本对象。
    """

    def __init__(self, node: "NodeState", key: tuple[str, str]) -> None:
        self.node = node
        self.key = key
        self.event = asyncio.Event()
        self.result: Optional[UpstreamResult] = None
        self.task: Optional[asyncio.Task] = None
        # 共用本次调用的客户端请求总数（含首位发起者）
        self.joined: int = 0


@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    # 在途计数：只统计实际发出的上游调用数，合并等待者不计入
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    # 因合并而免去的上游调用数（每加入一个等待者记 1）
    merged_requests: int = 0
    # 本节点在途的合并调用表：(路径, 原始查询串) -> CoalescedCall
    calls: dict[tuple[str, str], CoalescedCall] = field(default_factory=dict)
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "active_calls": len(self.calls),
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "merged_requests": self.merged_requests,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态与合并调用表；
        # 请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None
        # 所有在途上游调用任务，关闭时统一收尾
        self._tasks: set[asyncio.Task] = set()

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def acquire_call(
        self, path: str, query: str
    ) -> Optional[tuple[CoalescedCall, bool]]:
        """选路并返回 (合并调用, 是否为首位发起者)；无可用节点返回 None。

        在同一把锁内完成两步，保证判定与登记原子：
          1. 合并优先：扫描所有「健康且未排空」节点，若已存在同
             (路径, 完整查询串) 的在途调用则加入，不新增上游调用、不计在途；
          2. 否则按原有轮询选路，登记在途计数并创建新的合并调用。
        排空 / 探测故障的节点 can_serve()=False，既不会被扫描合并，
        也不会被轮询选中，新请求因此绝不可能加入其旧调用。
        """
        key = (path, query)
        async with self.cond:
            for node in self.nodes:
                if not node.can_serve():
                    continue
                existing = node.calls.get(key)
                if existing is not None:
                    existing.joined += 1
                    node.merged_requests += 1
                    return existing, False

            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    # 在途只按实际调用计数：只有新调用增计数
                    node.inflight += 1
                    node.rr_hits += 1
                    call = CoalescedCall(node, key)
                    call.joined = 1
                    node.calls[key] = call
                    call.task = asyncio.create_task(
                        self._run_call(call, path, query)
                    )
                    self._tasks.add(call.task)
                    return call, True
            return None

    def _build_url(self, node: NodeState, path: str, query: str) -> str:
        # 保留完整原始查询串：合并身份与转发内容必须逐字节一致
        url = f"{node.base_url}/{path}"
        return f"{url}?{query}" if query else url

    async def _invoke_upstream(
        self, node: NodeState, path: str, query: str
    ) -> UpstreamResult:
        """执行一次实际上游调用；任何失败都转换为 UpstreamResult，不抛出。"""
        target_url = self._build_url(node, path, query)
        assert self.client is not None
        try:
            upstream_resp = await self.client.request(
                "GET",
                target_url,
                timeout=REQUEST_TIMEOUT,
            )
            # httpx 默认缓冲完整正文；在同一 try 内读取，读身失败也按请求错误处理
            content = upstream_resp.content
        except httpx.TimeoutException as exc:
            await self.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return UpstreamResult(
                kind="timeout",
                status=504,
                headers=None,
                content=b'{"error":"upstream_timeout"}',
                detail=type(exc).__name__,
            )
        except httpx.RequestError as exc:
            await self.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return UpstreamResult(
                kind="request_error",
                status=502,
                headers=None,
                content=b'{"error":"upstream_unreachable"}',
                detail=f"{type(exc).__name__}: {str(exc)[:100]}",
            )

        # 5xx 视为上游故障：如实保留状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await self.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        headers = [
            (k, v)
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        ]
        return UpstreamResult(
            kind="response",
            status=upstream_resp.status_code,
            headers=headers,
            content=content,
        )

    async def _run_call(
        self, call: CoalescedCall, path: str, query: str
    ) -> None:
        """独立任务中的上游调用：与客户端生命周期完全解耦。

        即使所有客户端都断开，本任务仍会把上游调用跑完并收尾（清合并状态、
        归还在途计数、唤醒排空等待），保证排空不能因等待者消失提前回报。
        """
        node = call.node
        ok = False
        try:
            try:
                result = await self._invoke_upstream(node, path, query)
            except asyncio.CancelledError:
                # 仅发生在进程关闭时：仍走 finally 释放计数与合并状态
                raise
            except Exception as exc:  # 兜底：任何意外都不得让任务烂尾
                result = UpstreamResult(
                    kind="unexpected",
                    status=502,
                    headers=None,
                    content=b'{"error":"gateway_internal_error"}',
                    detail=f"{type(exc).__name__}: {str(exc)[:100]}",
                )
            call.result = result
            ok = result.kind == "response" and result.status < 500
        finally:
            async with self.cond:
                # 清除合并状态：调用结束后不缓存，后续同键请求会发起新调用（可重试）
                node.calls.pop(call.key, None)
                node.inflight -= 1
                if ok:
                    node.success_count += 1
                else:
                    node.failure_count += 1
                # 先放结果再置事件；事件置位后等待者即可读取 result
                call.event.set()
                # 在途归零可能让 drain 等待者满足条件
                self.cond.notify_all()
                if call.task is not None:
                    self._tasks.discard(call.task)

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)

    async def aclose(self) -> None:
        """关闭期收尾：取消并等待所有在途上游调用任务结束。"""
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass
            # 即使所有客户端都已断开，已发出的上游调用也要在关闭前收尾
            await gateway.aclose()


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持在途请求合并、"
        "排空/恢复/状态管理，无可接流节点时返回 503。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "coalescing": "同节点 + 同路径 + 同完整查询串的在途 GET 共用一次上游调用，结束不缓存",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


def _error_response(result: UpstreamResult, node: NodeState, call: CoalescedCall):
    payload = {
        "error": {
            "timeout": "upstream_timeout",
            "request_error": "upstream_unreachable",
        }.get(result.kind, "upstream_error"),
        "node": node.name,
        "detail": result.detail,
    }
    return JSONResponse(
        status_code=result.status,
        content=payload,
        headers={"X-Gateway-Coalesced": str(call.joined)},
    )


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    # 完整原始查询串参与合并身份，保证逐字节一致才合并
    query = request.url.query

    acquired = await gateway.acquire_call(path, query)
    if acquired is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    call, _is_leader = acquired
    try:
        # shield：客户端断开只取消这里的等待，绝不传播到独立的上游调用任务，
        # 因此首位客户端断开不会影响其他等待者，全部断开上游调用也照常收尾
        await asyncio.shield(call.event.wait())
    except asyncio.CancelledError:
        # 本客户端已断开；无需任何清理——合并状态与在途计数由调用任务负责
        raise

    result = call.result
    assert result is not None  # event 置位前 result 必已赋值

    if result.kind != "response":
        # 上游失败如实返回给每一个等待者；合并状态已由任务清除，后续请求可重试
        return _error_response(result, call.node, call)

    # 每个客户端拿到独立的 Response 对象；头与正文来自同一次上游调用
    headers: dict[str, str] = {}
    for k, v in result.headers or []:
        # 同名头取首个值即可（模拟上游不产生需要保留的重复响应头）
        headers.setdefault(k, v)
    headers["X-Gateway"] = "http-entry-gateway"
    headers["X-Upstream-Node"] = call.node.name
    # 共用本次调用的客户端总数，便于从外部观测合并效果
    headers["X-Gateway-Coalesced"] = str(call.joined)
    media_type = None
    for k, v in result.headers or []:
        if k.lower() == "content-type":
            media_type = v
            break
    return Response(
        content=result.content,
        status_code=result.status,
        headers=headers,
        media_type=media_type,
    )


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求（含禁止加入旧在途调用），并阻塞等待
    实际上游调用全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途调用时返回 202，
    drained=false 且节点继续保持排空状态。等待者断开不影响在途计数，
    排空绝不会因等待者消失而提前回报。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
