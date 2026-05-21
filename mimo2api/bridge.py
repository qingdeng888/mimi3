import asyncio, websockets, httpx, json, os, urllib.parse

KEY = os.getenv("MIMO_API_KEY")
URL = os.getenv("MIMO_API_ENDPOINT")
BASE = URL.split("/v1/")[0] if "/v1/" in URL else URL
WS_URL = "__WS_URL__"
# 桥接共享密钥占位符；manager.py 会用 json.dumps(token) 整体替换 ``"__BRIDGE_TOKEN__"`` 字面量。
# - 网关未配置 MIMO_WS_BRIDGE_TOKEN → 替换为空串 ""，下面 `if not BRIDGE_TOKEN` 为真 → 按未鉴权直连。
# - 网关配置了 MIMO_WS_BRIDGE_TOKEN  → 替换为 "<token>"，自动以 ?token=... 形式拼接到 WS_URL。
#
# ⚠️ 这里只能出现这一处 ``"__BRIDGE_TOKEN__"`` 字面量，不要在其他地方再写一次！
#    因为 manager.py 用的是全局 str.replace，多处出现会被同时替换，从而破坏控制流
#    （历史上「BRIDGE_TOKEN == "__BRIDGE_TOKEN__"」的兜底分支就是因此自我矛盾，
#     导致下发的 bridge 永远不带 token、被服务端 1008 直接拒绝、节点上不了线）。
BRIDGE_TOKEN = "__BRIDGE_TOKEN__"


def _build_ws_url() -> str:
    """把 token 以 query 参数的形式拼到 WS_URL 上；token 为空时原样返回。"""
    if not BRIDGE_TOKEN:
        return WS_URL
    sep = "&" if "?" in WS_URL else "?"
    return f"{WS_URL}{sep}token={urllib.parse.quote(BRIDGE_TOKEN, safe='')}"


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

async def main():
    ws_url = _build_ws_url()
    async with httpx.AsyncClient(timeout=None) as client:
        while True:
            try:
                async with websockets.connect(ws_url, max_size=10**8) as ws:
                    send_lock = asyncio.Lock()
                    async for msg in ws:
                        asyncio.create_task(handle_request(ws, json.loads(msg), client, send_lock))
            except Exception:
                await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
