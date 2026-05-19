import os
import json
import re
import time
import asyncio
import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from .auth import (
    create_webui_session_token,
    get_webui_cookie_name,
    get_webui_session_ttl,
    get_webui_username,
    is_ai_auth_enabled,
    is_web_auth_enabled,
    is_webui_authenticated,
    verify_webui_login,
    webui_cookie_secure,
)
from .gateway_state import state

router = APIRouter()

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USERS_DIR = os.path.join(ROOT_DIR, "users")


@router.get("/")
async def root_page():
    return RedirectResponse(url="/webui", status_code=307)

@router.get("/webui")
async def webui_page():
    ui_path = os.path.join(os.path.dirname(__file__), "webui.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return Response("webui.html not found", status_code=404)

@router.get("/api/system/status")
async def api_status():
    return JSONResponse({"active_clients": len(state.active_clients)})


@router.get("/api/auth/session")
async def api_auth_session(request: Request):
    auth_enabled = is_web_auth_enabled()
    authenticated = is_webui_authenticated(request)
    return JSONResponse({
        "enabled": auth_enabled,
        "authenticated": authenticated,
        "username": get_webui_username(),
        "ai_auth_enabled": is_ai_auth_enabled(),
    })


@router.post("/api/auth/login")
async def api_auth_login(request: Request):
    if not is_web_auth_enabled():
        return JSONResponse({"ok": True, "enabled": False, "username": get_webui_username()})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "请求体不是合法 JSON"}, status_code=400)

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))
    if not verify_webui_login(username, password):
        return JSONResponse({"detail": "用户名或密码错误"}, status_code=401)

    response = JSONResponse({"ok": True, "enabled": True, "username": get_webui_username()})
    response.set_cookie(
        key=get_webui_cookie_name(),
        value=create_webui_session_token(get_webui_username()),
        max_age=get_webui_session_ttl(),
        httponly=True,
        samesite="lax",
        secure=webui_cookie_secure(),
        path="/",
    )
    return response


@router.post("/api/auth/logout")
async def api_auth_logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie(key=get_webui_cookie_name(), path="/")
    return response

async def fetch_user_status(data: dict) -> dict:
    uid = data.get("userId")
    cookies = {
        "serviceToken": data.get("serviceToken", ""),
        "userId": uid,
        "xiaomichatbot_ph": data.get("xiaomichatbot_ph", "")
    }
    url = "https://aistudio.xiaomimimo.com/open-apis/user/mimo-claw/status"
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://aistudio.xiaomimimo.com",
        "Referer": "https://aistudio.xiaomimimo.com/",
        "User-Agent": "Mozilla/5.0"
    }
    try:
        from .manager import _get_proxy_url
        async with httpx.AsyncClient(proxy=_get_proxy_url(), timeout=5) as c:
            r = await c.get(url, cookies=cookies, headers=headers, timeout=5)
            if r.status_code == 401:
                return {**data, "claw_status": "EXPIRED(401)", "remain_sec": 0}
            r_data = r.json()
            st = r_data.get("data", {}).get("status", "UNKNOWN")
            expire_ms = r_data.get("data", {}).get("expireTime")
            remain_sec = max(0, int(int(expire_ms) / 1000 - time.time())) if expire_ms else 0
            return {**data, "claw_status": st, "remain_sec": remain_sec}
    except Exception:
        return {**data, "claw_status": "ERROR", "remain_sec": 0}

@router.get("/api/users/list")
async def api_users_list():
    raw_users = []
    if os.path.exists(USERS_DIR):
        for fn in os.listdir(USERS_DIR):
            if fn.startswith("user_") and fn.endswith(".json"):
                try:
                    with open(os.path.join(USERS_DIR, fn), "r", encoding="utf-8") as f:
                        raw_users.append(json.load(f))
                except:
                    pass

    # 并发查询所有用户的实例状态
    tasks = [fetch_user_status(rd) for rd in raw_users]
    results = await asyncio.gather(*tasks) if raw_users else []

    users = []
    for data in results:
        users.append({
            "userId": data.get("userId"),
            "name": data.get("name"),
            "serviceToken": data.get("serviceToken"),
            "claw_status": data.get("claw_status", "UNKNOWN"),
            "remain_sec": data.get("remain_sec", 0)
        })
    return JSONResponse({"users": users})

@router.post("/api/users/add")
async def api_users_add(request: Request):
    try:
        body = await request.json()
        raw_text = body.get("raw_text", "")
        # 解析正则提取
        parsed = {}
        for match in re.finditer(r'([a-zA-Z0-9_]+)="?([^;"]+)"?', raw_text):
            parsed[match.group(1)] = match.group(2)
            
        uid = parsed.get("userId")
        st = parsed.get("serviceToken")
        ph = parsed.get("xiaomichatbot_ph")
        
        if not uid or not st or not ph:
            return JSONResponse({"detail": "缺少必要字段 userId, serviceToken 或 xiaomichatbot_ph"}, status_code=400)
            
        os.makedirs(USERS_DIR, exist_ok=True)
        target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
        
        user_data = {
            "userId": uid,
            "serviceToken": st,
            "xiaomichatbot_ph": ph,
            "name": f"Imported_{uid}"
        }
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(user_data, f, ensure_ascii=False, indent=2)
            
        return JSONResponse({"status": "ok", "userId": uid})
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)

@router.post("/api/users/recreate/{uid}")
async def api_users_recreate(uid: str):
    """手动触发单个账号的销毁 + 创建流程"""
    from urllib.parse import quote
    from .manager import _get_proxy_url, make_claw_action_http_client

    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if not os.path.exists(target_file):
        return JSONResponse({"detail": "User not found"}, status_code=404)

    try:
        with open(target_file, "r", encoding="utf-8") as f:
            user_data = json.load(f)
    except Exception as e:
        return JSONResponse({"detail": f"读取账号文件失败: {e}"}, status_code=500)

    ph = user_data.get("xiaomichatbot_ph", "")
    cookies = {
        "serviceToken": user_data.get("serviceToken", ""),
        "userId": user_data.get("userId", ""),
        "xiaomichatbot_ph": ph,
    }
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://aistudio.xiaomimimo.com",
        "Referer": "https://aistudio.xiaomimimo.com/",
        "x-timezone": "Asia/Shanghai",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    base = "https://aistudio.xiaomimimo.com"

    # === 动作阶段：协议签署 + 销毁 + 创建 → 一条携趣短效代理走完 ===
    async with await make_claw_action_http_client(timeout=30) as client:
        # 0. 签署用户协议（首次创建必须，后续调也无副作用）
        try:
            agree_url = f"{base}/open-apis/agreement/user/mimo-claw?xiaomichatbot_ph={quote(ph)}"
            await client.post(agree_url, cookies=cookies, headers=headers, timeout=15)
        except Exception:
            pass

        # 1. 销毁旧实例
        try:
            destroy_url = f"{base}/open-apis/user/mimo-claw/destroy?xiaomichatbot_ph={quote(ph)}"
            await client.post(destroy_url, cookies=cookies, headers=headers, timeout=15)
            await asyncio.sleep(3)
        except Exception:
            pass

        # 2. 创建新实例
        create_url = f"{base}/open-apis/user/mimo-claw/create?xiaomichatbot_ph={quote(ph)}"
        try:
            r = await client.post(create_url, cookies=cookies, headers=headers, timeout=20)
            if r.status_code == 401:
                return JSONResponse({"detail": "凭证已过期 (401)，请重新导入 Cookie"}, status_code=401)
        except Exception as e:
            return JSONResponse({"detail": f"创建请求异常: {e}"}, status_code=502)

    # === 轮询阶段（非动作）：静态代理 / 直连，避免 30 秒短效代理过期 ===
    async with httpx.AsyncClient(proxy=_get_proxy_url(), timeout=30) as client:
        # 3. 轮询等待状态（最多 60 秒）
        status_url = f"{base}/open-apis/user/mimo-claw/status"
        deadline = time.time() + 60
        last_status = ""
        while time.time() < deadline:
            try:
                sr = await client.get(status_url, cookies=cookies, headers=headers, timeout=10)
                if sr.status_code == 401:
                    return JSONResponse({"detail": "凭证已过期 (401)"}, status_code=401)
                d = sr.json()
                st = (d.get("data") or {}).get("status", "")
                if st:
                    last_status = st
                if st == "AVAILABLE":
                    # 创建成功后触发全局重建信号让 Manager 感知并注入 bridge
                    from .manager import trigger_rebuild
                    trigger_rebuild()
                    return JSONResponse({"status": "ok", "claw_status": "AVAILABLE", "message": "环境创建成功，已触发桥接注入"})
                if st in ("FAILED", "CREATE_FAILED", "ERROR"):
                    return JSONResponse({"detail": f"创建失败，状态: {st}"}, status_code=502)
            except Exception:
                pass
            await asyncio.sleep(3)

        return JSONResponse({"detail": f"创建超时，最后状态: {last_status}"}, status_code=504)


@router.delete("/api/users/delete/{uid}")
async def api_users_delete(uid: str):
    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if os.path.exists(target_file):
        os.remove(target_file)
        return JSONResponse({"status": "ok"})
    return JSONResponse({"detail": "User not found"}, status_code=404)


# ----------------- 代理配置 API -----------------

PROXY_CONFIG_FILE = os.path.join(ROOT_DIR, "proxy_config.json")


@router.get("/api/proxy")
async def api_get_proxy():
    """读取当前代理配置"""
    proxy_url = ""
    source = "none"
    if os.path.exists(PROXY_CONFIG_FILE):
        try:
            with open(PROXY_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                proxy_url = data.get("proxy_url", "").strip()
                if proxy_url:
                    source = "webui"
        except Exception:
            pass
    if not proxy_url:
        proxy_url = os.getenv("MIMO_PROXY_URL", "").strip()
        if proxy_url:
            source = "env"
    return JSONResponse({"proxy_url": proxy_url, "source": source})


@router.put("/api/proxy")
async def api_set_proxy(request: Request):
    """设置代理（写入 proxy_config.json，立即热生效）"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "请求体不是合法 JSON"}, status_code=400)

    proxy_url = str(body.get("proxy_url", "")).strip()
    if proxy_url and not any(proxy_url.startswith(p) for p in ("http://", "https://", "socks5://", "socks4://")):
        return JSONResponse({"detail": "代理格式不正确，需以 http:// / https:// / socks5:// 开头"}, status_code=400)

    with open(PROXY_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({"proxy_url": proxy_url}, f, ensure_ascii=False, indent=2)

    return JSONResponse({"status": "ok", "proxy_url": proxy_url, "message": "代理已保存，即时生效"})


@router.delete("/api/proxy")
async def api_delete_proxy():
    """清除代理配置（删除 proxy_config.json，回退到环境变量或直连）"""
    if os.path.exists(PROXY_CONFIG_FILE):
        os.remove(PROXY_CONFIG_FILE)
    return JSONResponse({"status": "ok", "message": "代理已清除，将回退到环境变量或直连"})


# ----------------- 携趣 IP 短效代理 API -----------------

XIEQU_CONFIG_FILE = os.path.join(ROOT_DIR, "xiequ_config.json")


@router.get("/api/xiequ")
async def api_get_xiequ():
    """读取当前携趣 API 配置"""
    api_url = ""
    source = "none"
    if os.path.exists(XIEQU_CONFIG_FILE):
        try:
            with open(XIEQU_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                api_url = (data.get("api_url") or "").strip()
                if api_url:
                    source = "webui"
        except Exception:
            pass
    if not api_url:
        api_url = os.getenv("MIMO_XIEQU_API_URL", "").strip()
        if api_url:
            source = "env"
    return JSONResponse({"api_url": api_url, "source": source, "enabled": bool(api_url)})


@router.put("/api/xiequ")
async def api_set_xiequ(request: Request):
    """设置携趣 API 提取地址（写入 xiequ_config.json，立即热生效）"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "请求体不是合法 JSON"}, status_code=400)

    api_url = str(body.get("api_url", "")).strip()
    if api_url and not api_url.startswith(("http://", "https://")):
        return JSONResponse({"detail": "携趣 API 地址需以 http:// 或 https:// 开头"}, status_code=400)

    with open(XIEQU_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({"api_url": api_url}, f, ensure_ascii=False, indent=2)

    return JSONResponse({
        "status": "ok",
        "api_url": api_url,
        "enabled": bool(api_url),
        "message": "携趣 API 已保存，下次创建/销毁动作即时生效",
    })


@router.delete("/api/xiequ")
async def api_delete_xiequ():
    """清除携趣 API 配置（删除 xiequ_config.json，回退到环境变量或不启用）"""
    if os.path.exists(XIEQU_CONFIG_FILE):
        os.remove(XIEQU_CONFIG_FILE)
    return JSONResponse({"status": "ok", "message": "携趣 API 已清除，创建/销毁将回退到静态代理 / 直连"})


@router.post("/api/xiequ/test")
async def api_test_xiequ():
    """实时调用携趣 API 测试一次提取，返回拿到的 ip:port（不会缓存）"""
    from .manager import fetch_xiequ_proxy, _get_xiequ_api_url

    if not _get_xiequ_api_url():
        return JSONResponse({"detail": "尚未配置携趣 API 地址"}, status_code=400)

    proxy, err = await fetch_xiequ_proxy()
    if not proxy:
        return JSONResponse(
            {"detail": f"提取失败: {err or '未知错误，请查看后端日志'}"},
            status_code=502,
        )
    return JSONResponse({"status": "ok", "proxy": proxy, "message": f"成功提取一条短效代理: {proxy}"})
