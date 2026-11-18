#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多个并发在途请求（不同查询串不合并）：排空须等全部结束，在途计数全程准确
  15. 未知节点管理接口返回 404
  16. 在途请求合并：同节点+同路径+同完整查询串只产生 1 次上游调用，结束不缓存
  17. 合并键严格性：查询串不同 / 路径不同 / 参数顺序不同均不合并
  18. 取消语义：首位客户端断开不影响等待者；全部断开上游调用仍收尾
  19. 合并 + 排空：在途只按实际调用计数，等待者消失不提前回报，排空后不加入旧调用
  20. 上游失败：合并调用的失败如实分发给所有等待者，合并状态清除，后续可重试
"""
from __future__ import annotations

import http.client as http_client
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def hget(headers: dict, name: str) -> str | None:
    """大小写不敏感地取响应头（urllib 会把头名归一化为小写）。"""
    if not headers:
        return None
    lowered = {k.lower(): v for k, v in headers.items()}
    return lowered.get(name.lower())


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def upstream_state(port: int) -> dict:
    status, body = jget(f"http://127.0.0.1:{port}/control/state")
    assert status == 200, body
    return body


def path_calls(port: int, path: str) -> int:
    return upstream_state(port)["path_calls"].get(path, 0)


def concurrent_get(paths: list[str], cancel: set[int] | None = None,
                   timeout: float = 20.0) -> list[dict]:
    """并发发起若干 GET；cancel 中的下标在请求发出后立刻断开连接。

    返回每个线程的结果字典：{status, headers, body, cancelled, error}。
    cancel 场景使用底层 http.client 主动关闭 socket，模拟客户端断开。
    """
    cancel = cancel or set()
    barrier = threading.Barrier(len(paths))
    results: list[dict] = [None] * len(paths)  # type: ignore[list-item]

    def worker(idx: int, url_path: str) -> None:
        barrier.wait()
        if idx in cancel:
            conn = http_client.HTTPConnection("127.0.0.1", GW_PORT, timeout=timeout)
            try:
                conn.request("GET", url_path)
                # 请求已发出、上游仍在处理时立即断开：模拟首位客户端取消
                conn.sock.shutdown(socket.SHUT_RDWR)
                conn.close()
                results[idx] = {"cancelled": True}
            except Exception as exc:  # 断连本身可能抛错，均视为已取消
                try:
                    conn.close()
                except Exception:
                    pass
                results[idx] = {"cancelled": True, "error": repr(exc)}
            return
        try:
            status, headers, body = http("GET", f"http://127.0.0.1:{GW_PORT}{url_path}",
                                         timeout=timeout)
            results[idx] = {"status": status, "headers": headers, "body": body}
        except Exception as exc:
            results[idx] = {"error": repr(exc)}

    threads = [threading.Thread(target=worker, args=(i, p)) for i, p in enumerate(paths)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=timeout + 10)
    assert all(r is not None for r in results), "存在线程未结束"
    return results


def main() -> int:
    procs = []
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs = [u1, u2]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理与转发接口",
              "/admin/nodes/{name}/drain" in paths and "/proxy/{path}" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        # 注意：相同 (路径,查询串) 现在会被合并，故每个请求带不同查询串，
        # 以保持「4 个独立实际调用」这一原始测试意图
        print("== 14. 多并发在途请求排空（不同查询串不合并） ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []
        calls_before14 = path_calls(U1_PORT, "/slow")

        def slow_call14(idx: int):
            s, _, b = http(
                "GET",
                f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3&tag={idx}",
                timeout=20,
            )
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确（在途只按实际调用计）", st["inflight"] == 4, str(st))
        check("4 个不同查询串不产生合并", st["merged_requests"] == 0, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)
        check("上游确实收到 4 次实际调用",
              path_calls(U1_PORT, "/slow") - calls_before14 == 4,
              str(upstream_state(U1_PORT)))

        # 15. 未知节点管理接口返回 404
        print("== 15. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)

        # 16. 在途请求合并：同节点 + 同路径 + 同完整查询串 -> 1 次上游调用；结束不缓存
        print("== 16. 在途请求合并 ==")
        # 当前两节点均在排空（承接场景 14/15）：恢复 node1 并排空 node2，
        # 把流量确定性地集中到 node1，便于核对合并计数
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        assert node_status("node1")["eligible"], "node1 恢复后应可接流"
        calls_before16 = path_calls(U1_PORT, "/slow")
        results16 = concurrent_get(
            ["/proxy/slow?seconds=3&share=same"] * 3, timeout=20
        )
        check("3 个相同请求全部拿到 200",
              all(r.get("status") == 200 for r in results16), str(results16))
        bodies16 = [json.loads(r["body"]) for r in results16]
        check("3 个响应均来自 node1 且正文一致",
              all(b["node"] == "node1" and b["requested"] == 3 for b in bodies16)
              and len({json.dumps(b, sort_keys=True) for b in bodies16}) == 1,
              str(bodies16))
        check("上游只收到 1 次实际调用（合并生效）",
              path_calls(U1_PORT, "/slow") - calls_before16 == 1,
              str(upstream_state(U1_PORT)))
        check("响应头标记共用调用的客户端数 X-Gateway-Coalesced=3",
              all(hget(r["headers"], "X-Gateway-Coalesced") == "3" for r in results16),
              str([r.get("headers") for r in results16]))
        st = node_status("node1")
        check("在途只按实际调用计：结束后 inflight=0", st["inflight"] == 0, str(st))
        check("合并免去 2 次上游调用 merged_requests=2",
              st["merged_requests"] == 2, str(st))
        # 不缓存：调用结束后再发一次相同请求，必须产生新的上游调用
        calls_before_retry = path_calls(U1_PORT, "/slow")
        status, body = jget("http://127.0.0.1:19080/proxy/slow?seconds=0&share=same")
        check("调用结束后不缓存，重发产生新的上游调用",
              status == 200 and path_calls(U1_PORT, "/slow") - calls_before_retry == 1,
              str(upstream_state(U1_PORT)))

        # 17. 合并键严格性：查询串 / 路径 / 参数顺序不同均不合并
        print("== 17. 合并键严格性 ==")
        calls_slow17 = path_calls(U1_PORT, "/slow")
        calls_flaky17 = path_calls(U1_PORT, "/flaky")
        calls_echo17 = path_calls(U1_PORT, "/echo")
        results17 = concurrent_get([
            "/proxy/slow?seconds=2&merge=key",          # 同查询串 x2 -> 合并
            "/proxy/slow?seconds=2&merge=key",
            "/proxy/slow?seconds=2&merge=different",    # 查询串不同 -> 独立
            "/proxy/flaky?merge=key",                    # 路径不同 -> 独立
            "/proxy/echo?b=2&a=1",                       # 参数顺序不同 x2
            "/proxy/echo?a=1&b=2",
        ], timeout=20)
        check("6 个请求全部成功", all(r.get("status") == 200 for r in results17), str(results17))
        check("/slow 合并组只产生 2 次实际调用（2+1）",
              path_calls(U1_PORT, "/slow") - calls_slow17 == 2,
              str(upstream_state(U1_PORT)["path_calls"]))
        check("/flaky 不同路径独立计数 1 次",
              path_calls(U1_PORT, "/flaky") - calls_flaky17 == 1)
        check("/echo 参数顺序不同不合并，产生 2 次实际调用",
              path_calls(U1_PORT, "/echo") - calls_echo17 == 2,
              str(upstream_state(U1_PORT)["path_calls"]))
        check("相同查询串的合并响应 coalesced=2",
              hget(results17[0]["headers"], "X-Gateway-Coalesced") == "2",
              str(results17[0]["headers"]))
        check("独立调用的响应 coalesced=1",
              hget(results17[2]["headers"], "X-Gateway-Coalesced") == "1",
              str(results17[2]["headers"]))
        st = node_status("node1")
        check("在途全程按实际调用计数，结束后归零", st["inflight"] == 0, str(st))

        # 18. 取消语义：首位客户端断开不影响等待者；全部断开上游调用仍须收尾
        print("== 18. 客户端取消语义 ==")
        calls_before18 = path_calls(U1_PORT, "/slow")
        results18 = concurrent_get(
            ["/proxy/slow?seconds=3&cancel=group"] * 3,
            cancel={0},  # 首位客户端在请求发出后立即断开
            timeout=20,
        )
        check("首位客户端确认已断开", results18[0].get("cancelled") is True, str(results18[0]))
        check("其余两个等待者不受影响，仍拿到完整 200",
              all(r.get("status") == 200 for r in results18[1:]), str(results18))
        check("上游只产生 1 次实际调用（断开没有触发重发）",
              path_calls(U1_PORT, "/slow") - calls_before18 == 1,
              str(upstream_state(U1_PORT)))
        check("等待者响应仍标记 3 个客户端共用",
              all(hget(r["headers"], "X-Gateway-Coalesced") == "3" for r in results18[1:]))
        st = node_status("node1")
        check("调用收尾后在途归零、合并状态清除",
              st["inflight"] == 0 and st["active_calls"] == 0, str(st))

        # 全部客户端都断开：已发出的上游调用仍须完成并准确收尾
        calls_before18b = path_calls(U1_PORT, "/slow")
        inflight_before = node_status("node1")["inflight"]
        results18b = concurrent_get(
            ["/proxy/slow?seconds=3&cancel=all"] * 2,
            cancel={0, 1},
            timeout=20,
        )
        check("两个客户端均确认断开", all(r.get("cancelled") for r in results18b), str(results18b))
        # 上游视角：调用确实发出且完整执行（slow 的 _inflight 先升后降）
        deadline = time.time() + 10
        while time.time() < deadline:
            us = upstream_state(U1_PORT)
            if us["path_calls"].get("/slow", 0) - calls_before18b == 1 and us["inflight"] == 0:
                break
            time.sleep(0.2)
        us = upstream_state(U1_PORT)
        check("全部断开后上游调用仍完整跑完 1 次",
              us["path_calls"].get("/slow", 0) - calls_before18b == 1 and us["inflight"] == 0,
              str(us))
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != inflight_before:
            time.sleep(0.1)
        st = node_status("node1")
        check("网关侧在途计数仍准确归零，无残留合并状态",
              st["inflight"] == 0 and st["active_calls"] == 0, str(st))

        # 19. 合并 + 排空：在途只按实际调用计数；排空后新请求不加入旧调用
        print("== 19. 合并场景下的排空 ==")
        # 承接场景 14/15：两节点均在排空中，先全部恢复，保证两个候选节点都可接流
        for nm in ("node1", "node2"):
            status, _, _ = http(
                "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{nm}/resume"
            )
            assert status == 200
        deadline = time.time() + 10
        while time.time() < deadline and not all(
            node_status(n)["eligible"] for n in ("node1", "node2")
        ):
            time.sleep(0.2)
        check("19 前置：两节点均恢复可接流",
              all(node_status(n)["eligible"] for n in ("node1", "node2")),
              str([node_status(n) for n in ("node1", "node2")]))
        # 手工编排：三个线程经 barrier 同时发起相同慢请求，保持调用在途以便观测排空
        barrier19 = threading.Barrier(3)
        merged19: list[dict] = []
        calls_before19 = {
            "node1": path_calls(U1_PORT, "/slow"),
            "node2": path_calls(U2_PORT, "/slow"),
        }

        def coalesced_worker19():
            barrier19.wait()
            try:
                s, h, b = http(
                    "GET", "http://127.0.0.1:19080/proxy/slow?seconds=3&drain=group",
                    timeout=20,
                )
                merged19.append({"status": s, "headers": h, "body": b})
            except Exception as exc:
                merged19.append({"error": repr(exc)})

        workers19 = [threading.Thread(target=coalesced_worker19) for _ in range(3)]
        for th in workers19:
            th.start()
        # 等到合并组确实形成：3 个客户端、1 个实际调用（落点由轮询游标决定，动态确认）
        deadline = time.time() + 5
        owner19 = None
        while time.time() < deadline:
            for nm in ("node1", "node2"):
                if node_status(nm)["active_calls"] == 1:
                    owner19 = nm
                    break
            if owner19:
                break
            time.sleep(0.1)
        check("合并组在某节点形成且在途只算 1 个实际调用",
              owner19 is not None and node_status(owner19)["inflight"] == 1,
              f"owner={owner19} " + str([node_status(n) for n in ("node1", "node2")]))
        other19 = "node2" if owner19 == "node1" else "node1"
        owner_port = U1_PORT if owner19 == "node1" else U2_PORT
        drain_results: dict = {}

        def drain_call19():
            drain_results["status"], _, drain_results["body"] = http(
                "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{owner19}/drain?timeout=15",
                timeout=20,
            )

        t0 = time.time()
        dt = threading.Thread(target=drain_call19)
        dt.start()
        time.sleep(0.5)
        check("排空被实际调用阻塞（等待者数不影响在途计数）",
              "status" not in drain_results, str(drain_results))
        # 排空后新请求不得加入承接节点的旧调用：应按原有轮询选路去另一节点
        status, body = jget("http://127.0.0.1:19080/proxy/echo?after=drain")
        check("排空后新请求不加入旧调用，按原有选路去另一节点",
              status == 200 and body["node"] == other19,
              f"owner={owner19} status={status} body={body}")
        dt.join(timeout=20)
        for th in workers19:
            th.join(timeout=20)
        elapsed19 = time.time() - t0
        b19 = json.loads(drain_results["body"])
        check("排空阻塞到实际调用结束（约 3s）且 drained=true",
              elapsed19 >= 2.5 and drain_results["status"] == 200
              and b19.get("drained") is True,
              f"elapsed={elapsed19:.1f}s body={drain_results.get('body')}")
        check("3 个合并客户端全部拿到完整 200",
              len(merged19) == 3 and all(r.get("status") == 200 for r in merged19),
              str(merged19))
        check(f"上游 {owner19} 只产生 1 次实际调用",
              path_calls(owner_port, "/slow") - calls_before19[owner19] == 1,
              str(upstream_state(owner_port)))
        check("排空完成后在途归零", node_status(owner19)["inflight"] == 0)

        # 20. 上游失败如实分发并清除合并状态，后续请求可以重试
        print("== 20. 上游失败分发与重试 ==")
        # 承接场景 19：owner19 处于排空中、另一节点可接流。
        # 为保证失败请求的落点可观测，先恢复 owner19 并排空另一节点，把流量集中过去。
        fail_node = owner19
        fail_port = owner_port
        peer_node = other19
        set_mode(fail_port, "error")
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{fail_node}/resume"
        )
        assert status == 200
        # resume 会立即探测一次并置 healthy=false；先让上游恢复使节点经探测变健康，
        # 再立刻切回 error，保证合并请求真正撞上 5xx（而非直接 503 选路失败）
        set_mode(fail_port, "healthy")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status(fail_node)["eligible"]:
            time.sleep(0.2)
        check(f"{fail_node} 重新可接流", node_status(fail_node)["eligible"] is True)
        # 排空对端，把全部流量集中到即将置故障的节点
        status, _, _ = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{peer_node}/drain?timeout=5"
        )
        assert status == 200
        set_mode(fail_port, "error")  # 不等周期探测，让合并请求先撞上 5xx
        calls_before20 = path_calls(fail_port, "/flaky")
        results20 = concurrent_get(
            ["/proxy/flaky?fail=group"] * 3, timeout=15
        )
        check("3 个合并等待者都如实收到 500",
              all(r.get("status") == 500 for r in results20), str(results20))
        check("失败也只产生 1 次实际上游调用",
              path_calls(fail_port, "/flaky") - calls_before20 == 1,
              str(upstream_state(fail_port)))
        time.sleep(0.3)
        st = node_status(fail_node)
        check("失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后合并状态清除且在途归零",
              st["inflight"] == 0 and st["active_calls"] == 0, str(st))
        # 节点已不健康且对端在排空 -> 无可用节点，按原有选路处理返回 503
        status, _, body = http("GET", "http://127.0.0.1:19080/proxy/echo?retry=route")
        check("故障节点摘除且对端排空中 -> 503，绝不进入旧调用",
              status == 503, f"status={status} body={body}")
        # 上游恢复 -> 经探测重新接流；重试相同请求必须发起新调用（证明状态已清除、可重试）
        set_mode(fail_port, "healthy")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status(fail_node)["eligible"]:
            time.sleep(0.2)
        check(f"{fail_node} 探测恢复后重新可接流", node_status(fail_node)["eligible"] is True)
        calls_before20b = path_calls(fail_port, "/flaky")
        status, _, body = http("GET", "http://127.0.0.1:19080/proxy/flaky?fail=group")
        check("重试相同请求成功 200，且为全新上游调用（无缓存/无残留合并）",
              status == 200 and path_calls(fail_port, "/flaky") - calls_before20b == 1,
              f"status={status} {upstream_state(fail_port)}")
        set_mode(fail_port, "healthy")

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
