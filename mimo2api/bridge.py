import asyncio, websockets, httpx, json, os, urllib.parse

KEY = os.getenv("MIMO_API_KEY")
URL = os.getenv("MIMO_API_ENDPOINT")
BASE = URL.split("/v1/")[0] if "/v1/" in URL else URL
WS_URL = "__WS_URL__"
# 桥接共享密钥占位符；manager.py 会用 json.dumps(token) 整体替换下面 BRIDGE_TOKEN 赋值那一行
# 带双引号的字面量。
# - 网关未配置 MIMO_WS_BRIDGE_TOKEN → 替换为空串 ""，下面 `if not BRIDGE_TOKEN` 为真 → 按未鉴权直连。
# - 网关配置了 MIMO_WS_BRIDGE_TOKEN  → 替换为 token 明文，自动以 ?token=... 形式拼接到 WS_URL。
#
# ⚠️ 安全 / 正确性双重约束：整个文件只能在「下面 BRIDGE_TOKEN 赋值那一行」出现这一处带双引号的占位符，
#    其他位置（包括本注释段）绝对不能再以带引号形式写出占位符字面量。
#    因为 manager.py 用的是全局 str.replace，多处出现会被同时替换：
#      1. 控制流被破坏（历史上的"BRIDGE_TOKEN 与占位符自身比较"兜底分支就是因此自我矛盾，
#         导致下发的 bridge 永远不带 token、被服务端 1008 拒绝、节点上不了线）；
#      2. 真实 token 会被注入到注释里，随 bridge 源码一同作为 prompt 投递到 Claw 容器内的 LLM，
#         泄漏到对话上下文与平台日志（这是真实发生过的安全事故）。
BRIDGE_TOKEN = "__BRIDGE_TOKEN__"

# 桥接归属账号 uid 占位符；manager.py 会用 json.dumps(uid) 整体替换下面 BRIDGE_UID 赋值那一行
# 带双引号的字面量。
# 服务端 ws_tunnel 收到 ?uid=... 后会把 (id(ws) -> uid) 映射记入 client_uid_map，
# 用于「累计冷却 N 次自动单账号重建」时把信号精准下发到对应的 AccountManager，
# 而不是无差别全局重建拖累其他健康账号。
#
# ⚠️ 同样限定：整个文件只能在「下面 BRIDGE_UID 赋值那一行」出现这一处带双引号的占位符，
#    其他位置不能再以带引号形式写出该字面量，避免 manager.py 全局 str.replace 误伤注释。
BRIDGE_UID = "__BRIDGE_UID__"


def _build_ws_url() -> str:
    """把 token / uid 以 query 参数的形式拼到 WS_URL 上；为空则跳过对应字段。"""
    qs: list[str] = []
    if BRIDGE_TOKEN:
        qs.append(f"token={urllib.parse.quote(BRIDGE_TOKEN, safe='')}")
    if BRIDGE_UID:
        qs.append(f"uid={urllib.parse.quote(BRIDGE_UID, safe='')}")
    if not qs:
        return WS_URL
    sep = "&" if "?" in WS_URL else "?"
    return f"{WS_URL}{sep}{'&'.join(qs)}"


async def safe_send(ws, lock, data):
    async with lock:
        await ws.send(json.dumps(data))

async def handle_request(ws, req, client, lock):
    req_id = req.get("req_id") 
    try:
        async with client.stream(
            method=req.get("method", "GET"), 
            url=f"{BASE}/anthropic/v1/messages" if "/anthropic/" in req.get("path", "") else URL, 
            headers={"api-key": KEY, "Content-Type": "application/json"}, 
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
            except Exception:
                await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
