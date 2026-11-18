"""模拟上游服务：可产生慢请求、可被手动置为故障模式，并统计实际调用次数。

管理接口（仅用于测试/演练，不经过网关）：
  GET  /control/state            查看当前模式、在途请求数与累计实际调用次数
  POST /control/mode?mode=...    切换模式: healthy | slow | error | hang

业务接口:
  GET  /slow?seconds=N            延迟 N 秒后返回（慢请求）
  GET  /echo                      回显请求信息
  GET  /flaky                     按当前模式返回 200 或 500
  GET  /                          健康信息

调用次数说明：
  * 网关的在途请求合并应让多个相同请求只产生 1 次实际调用，本服务如实计数；
  * ``calls_total`` 统计所有业务/健康接口的实际进入次数，
    ``path_calls`` 按路径分项（/control 自身不计入），便于测试按路径核对。
"""
from __future__ import annotations

import asyncio
import time

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# 节点运行模式（演示用的全局状态）
_mode = "healthy"
_inflight = 0
_started = time.time()
_node_id = "unknown"

# 实际调用计数（每次请求真正进入业务/健康接口时累加，合并在网关侧生效）
_calls_total = 0
_path_calls: dict[str, int] = {}


def _count(path: str) -> None:
    global _calls_total
    _calls_total += 1
    _path_calls[path] = _path_calls.get(path, 0) + 1


@asynccontextmanager
async def lifespan(app: FastAPI):
    import os

    global _node_id
    _node_id = os.environ.get("NODE_ID", "unknown")
    yield


app = FastAPI(title="mock-upstream", version="1.1.0", lifespan=lifespan)


@app.get("/")
async def root() -> JSONResponse:
    """健康端点：如实反映节点模式。

    error -> 503（探测据此判定节点不健康）；hang -> 延迟（探测超时判定）；
    其余 -> 200。注意控制面状态请查 /control/state，该接口不受模式影响。
    """
    _count("/")
    if _mode == "error":
        return JSONResponse(
            status_code=503,
            content={"node": _node_id, "mode": _mode, "inflight": _inflight,
                     "error": "upstream in error mode"},
        )
    if _mode == "hang":
        await asyncio.sleep(120)
    return JSONResponse(
        status_code=200,
        content={
            "node": _node_id,
            "mode": _mode,
            "inflight": _inflight,
            "uptime": round(time.time() - _started, 2),
        },
    )


@app.get("/echo")
async def echo(request: Request) -> dict:
    _count("/echo")
    return {
        "node": _node_id,
        "path": "/echo",
        "query_params": dict(request.query_params),
        "headers": {k: v for k, v in request.headers.items()},
    }


@app.get("/slow")
async def slow(seconds: float = Query(2.0, ge=0, le=60)) -> JSONResponse:
    """慢请求：用于验证排空期间在途请求不被截断、合并调用不被客户端断开取消。"""
    global _inflight
    _count("/slow")
    _inflight += 1
    started = time.time()
    try:
        await asyncio.sleep(seconds)
        if _mode == "error":
            return JSONResponse(
                status_code=500,
                content={"node": _node_id, "error": "upstream in error mode"},
            )
        return JSONResponse(
            status_code=200,
            content={
                "node": _node_id,
                "path": "/slow",
                "slept": round(time.time() - started, 3),
                "requested": seconds,
            },
        )
    finally:
        _inflight -= 1


@app.get("/flaky")
async def flaky() -> JSONResponse:
    """按模式返回：error -> 500；hang -> 挂起（由网关超时兜底）；其余 -> 200。"""
    _count("/flaky")
    if _mode == "error":
        return JSONResponse(
            status_code=500,
            content={"node": _node_id, "error": "upstream in error mode"},
        )
    if _mode == "hang":
        # 挂足够久，让网关的请求超时先触发
        await asyncio.sleep(120)
    return JSONResponse(status_code=200, content={"node": _node_id, "ok": True})


@app.get("/control/state")
async def control_state() -> dict:
    return {
        "node": _node_id,
        "mode": _mode,
        "inflight": _inflight,
        "calls_total": _calls_total,
        "path_calls": dict(_path_calls),
    }


@app.post("/control/mode")
async def set_mode(mode: str = Query(..., pattern="^(healthy|slow|error|hang)$")) -> dict:
    global _mode
    old = _mode
    _mode = mode
    return {"node": _node_id, "previous_mode": old, "mode": _mode}
