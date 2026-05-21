import asyncio, websockets, httpx, json, os, urllib.parse

KEY = os.getenv("MIMO_API_KEY")
URL = os.getenv("MIMO_API_ENDPOINT")
BASE = URL.split("/v1/")[0] if "/v1/" in URL else URL
WS_URL = "__WS_URL__"
# 桥接共享密钥占位符；manager.py 在下发前会替换成 MIMO_WS_BRIDGE_TOKEN 的值，
# 留空（"" 或字面占位符）则按未鉴权直连原 WS_URL。
BRIDGE_TOKEN = "__BRIDGE_TOKEN__"


def _build_ws_url() -> str:
    """把 token 以 query 参数的形式拼到 WS_URL 上；token 为空时原样返回。"""
    if not BRIDGE_TOKEN or BRIDGE_TOKEN == "__BRIDGE_TOKEN__":
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
