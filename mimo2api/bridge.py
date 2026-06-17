import asyncio, websockets, httpx, json, os, urllib.parse

KEY = os.getenv("MIMO_API_KEY", "")
BASE_URL = os.getenv("MIMO_API_BASE_URL", "").rstrip("/")
WS_URL = "__WS_URL__"

# 桥接归属账号 uid 占位符；manager.py 会用 json.dumps(uid) 整体替换下面 BRIDGE_UID 赋值那一行
# 带双引号的字面量。
# 服务端 ws_tunnel 收到 ?uid=... 后会把 (id(ws) -> uid) 映射记入 client_uid_map，
# 用于「累计冷却 N 次自动单账号重建」时把信号精准下发到对应的 AccountManager，
# 而不是无差别全局重建拖累其他健康账号。
#
# ⚠️ 同样限定：整个文件只能在「下面 BRIDGE_UID 赋值那一行」出现这一处带双引号的占位符，
#    其他位置不能再以带引号形式写出该字面量，避免 manager.py 全局 str.replace 误伤注释。
BRIDGE_UID = "__BRIDGE_UID__"

# 会话注册码占位符；manager.py 会用 json.dumps(session_token) 整体替换下面 BRIDGE_SESSION 赋值那一行
# 带双引号的字面量。
# 每次创建/重建实例时 manager 生成一个唯一 session_token 并注册到 gateway 的 valid_sessions 表，
# bridge 连接 /ws 时通过 ?session=... 提交，gateway 验证后才 accept。
# 禁用/删除账号时 session 从 valid_sessions 中撤销 → bridge 无论如何重连都被拒绝。
#
# ⚠️ 同样限定：整个文件只能在「下面 BRIDGE_SESSION 赋值那一行」出现这一处带双引号的占位符，
#    其他位置不能再以带引号形式写出该字面量，避免 manager.py 全局 str.replace 误伤注释。
BRIDGE_SESSION = "__BRIDGE_SESSION__"


def _build_ws_url() -> str:
    """把 uid / session 以 query 参数的形式拼到 WS_URL 上；为空则跳过对应字段。"""
    qs: list[str] = []
    if BRIDGE_UID:
        qs.append(f"uid={urllib.parse.quote(BRIDGE_UID, safe='')}")
    if BRIDGE_SESSION:
        qs.append(f"session={urllib.parse.quote(BRIDGE_SESSION, safe='')}")
    if not qs:
        return WS_URL
    sep = "&" if "?" in WS_URL else "?"
    return f"{WS_URL}{sep}{'&'.join(qs)}"


async def safe_send(ws, lock, data):
    async with lock:
        await ws.send(json.dumps(data))

async def handle_request(ws, req, client, lock):
    req_id = req.get("req_id")
    path = req.get("path", "/v1/chat/completions")
    # 拼接完整的转发 URL，确保不会出现双斜杠或重复 /v1
    base = BASE_URL.rstrip("/")
    path = path if path.startswith("/") else f"/{path}"
    # 如果 BASE_URL 已经包含 /v1 而 path 也以 /v1 开头，去掉重复
    if base.endswith("/v1") and path.startswith("/v1"):
        path = path[3:]  # 去掉 path 开头的 /v1
    target_url = f"{base}{path}"

    # 使用 miclaw 容器本身的 headers 伪装
    forward_headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Origin": "https://aistudio.xiaomimimo.com",
        "Referer": "https://aistudio.xiaomimimo.com/",
        "x-timezone": "Asia/Shanghai",
    }
    if KEY:
        forward_headers["Authorization"] = f"Bearer {KEY}"

    try:
        async with client.stream(
            method=req.get("method", "POST"),
            url=target_url,
            headers=forward_headers,
            content=req.get("body", "")
        ) as r:
            await safe_send(ws, lock, {
                "req_id": req_id, "type": "start",
                "status": r.status_code, "headers": dict(r.headers)
            })
            async for chunk in r.aiter_text():
                if chunk:
                    await safe_send(ws, lock, {
                        "req_id": req_id, "type": "chunk", "body": chunk
                    })
            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})

    except Exception as e:
        await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})

async def heartbeat_loop(ws, lock, interval=10):
    """每 interval 秒向 gateway 发送心跳，让 gateway 知道本 bridge 仍然存活。"""
    try:
        while True:
            await asyncio.sleep(interval)
            await safe_send(ws, lock, {"type": "heartbeat"})
    except Exception:
        pass  # ws 关闭时自然退出

async def main():
    ws_url = _build_ws_url()
    async with httpx.AsyncClient(timeout=None) as client:
        while True:
            try:
                async with websockets.connect(ws_url, max_size=10**8) as ws:
                    send_lock = asyncio.Lock()
                    hb_task = asyncio.create_task(heartbeat_loop(ws, send_lock))
                    try:
                        async for msg in ws:
                            asyncio.create_task(handle_request(ws, json.loads(msg), client, send_lock))
                    finally:
                        hb_task.cancel()
            except websockets.exceptions.ConnectionClosedError as e:
                # 4001 = 账号已禁用，gateway 明确告知不要重连
                if e.code == 4001:
                    break
                await asyncio.sleep(3)
            except websockets.exceptions.ConnectionClosed as e:
                if e.code == 4001:
                    break
                await asyncio.sleep(3)
            except Exception:
                await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
