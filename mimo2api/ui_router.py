import os
import json
import re
import secrets
import time
import asyncio
import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from .auth import (
    AI_AUTH_ENV,
    AI_KEYS_CONFIG_FILE,
    _load_extra_ai_keys,
    _save_extra_ai_keys,
    create_webui_session_token,
    get_all_ai_api_keys,
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

# ----------------- uid -> 备注名 缓存（仅供 /api/clients 高频展示用） -----------------
# WebUI 节点列表会以 ~3s 间隔轮询 /api/clients，每次都全量 listdir + 多次 open 较浪费 IO。
# 这里加一个进程内 TTL 缓存（10s 即可，备注名变更后下一次轮询就能感知，无需主动 invalidate）。
# 注意：仅对**展示**生效；状态查询、禁用、重建等关键路径仍直接从磁盘读，避免缓存延迟带来副作用。
_USER_ALIAS_CACHE_TTL = 10.0  # seconds
_user_alias_cache: dict[str, str] = {}
_user_alias_cache_loaded_at: float = 0.0


def _load_user_aliases_cached() -> dict[str, str]:
    """返回 uid -> 备注名（name 字段）的字典；进程内 TTL 缓存。

    任何 IO 失败一律静默退化为上次的缓存或空 dict，保证调用方（/api/clients）的可用性
    永远高于"展示是否好看"。
    """
    global _user_alias_cache, _user_alias_cache_loaded_at
    now = time.monotonic()
    if _user_alias_cache_loaded_at and (now - _user_alias_cache_loaded_at) < _USER_ALIAS_CACHE_TTL:
        return _user_alias_cache
    fresh: dict[str, str] = {}
    try:
        if os.path.isdir(USERS_DIR):
            for fn in os.listdir(USERS_DIR):
                if fn.startswith("user_") and fn.endswith(".json"):
                    fp = os.path.join(USERS_DIR, fn)
                    try:
                        with open(fp, "r", encoding="utf-8") as f:
                            udata = json.load(f) or {}
                        u = str(udata.get("userId", "")).strip()
                        n = str(udata.get("name", "") or "").strip()
                        if u:
                            fresh[u] = n
                    except Exception:
                        # 损坏的单个 user_*.json 不影响其他账号显示
                        continue
    except Exception:
        # 整个目录扫描失败 → 保留旧缓存（兜底优先）
        return _user_alias_cache
    _user_alias_cache = fresh
    _user_alias_cache_loaded_at = now
    return _user_alias_cache


def _invalidate_user_alias_cache() -> None:
    """让下一次 /api/clients 立即重新读盘（用于 rename / add / delete 后保证 UI 即时反映）。"""
    global _user_alias_cache_loaded_at
    _user_alias_cache_loaded_at = 0.0


def _revoke_sessions_for_uid(uid: str) -> int:
    """撤销 gateway 中该 uid 对应的所有会话注册码，使 bridge 无法再通过 session 验证重连。

    返回被撤销的 session 数量。
    """
    to_remove = [token for token, token_uid in list(state.valid_sessions.items()) if token_uid == uid]
    for token in to_remove:
        state.valid_sessions.pop(token, None)
    return len(to_remove)


# ----------------- 后台 close 任务集合（fire-and-forget） -----------------
# 网关侧主动关闭 ws 的场景（disconnect API、cooldown 升级踢除等）不应该让 await close
# 把请求 worker 卡死——半死 ws 的关闭帧可能要走完 TCP 重传定时器才能返回。
# 用全局集合持强引用，避免 Py 3.11+ 出现 "Task was destroyed but it is pending"。
_ws_close_tasks: set[asyncio.Task] = set()


def _fire_and_forget_close(ws, code: int = 1000) -> None:
    """异步发起 ws.close(code) 但不等待，调用方立即返回。"""
    async def _do_close():
        try:
            await ws.close(code=code)
        except Exception:
            # 已关闭 / 半死状态都吞掉，调用方关心的语义只是"我已经把信号发出去了"
            pass
    try:
        task = asyncio.create_task(_do_close())
        _ws_close_tasks.add(task)
        task.add_done_callback(_ws_close_tasks.discard)
    except Exception:
        pass


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
    from .manager import load_disabled_accounts

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

    disabled = load_disabled_accounts()

    # 获取各账号的随机延迟重建状态
    from .manager import _account_managers
    now_ts = time.time()

    users = []
    for data in results:
        uid = data.get("userId", "")
        disabled_info = disabled.get(str(uid))

        # 检查该账号是否正在随机延迟等待重建
        rebuild_delay_remaining = 0
        mgr = _account_managers.get(str(uid))
        if mgr and mgr._rebuild_delay_until > now_ts:
            rebuild_delay_remaining = int(mgr._rebuild_delay_until - now_ts)

        # 当实例状态为非运行态（NOT_CREATED/DESTROYED/空）且正在随机延迟中，
        # 覆写 claw_status 显示给前端
        claw_status = data.get("claw_status", "UNKNOWN")
        if rebuild_delay_remaining > 0 and claw_status in ("NOT_CREATED", "DESTROYED", "", "UNKNOWN"):
            minutes = rebuild_delay_remaining // 60
            seconds = rebuild_delay_remaining % 60
            if minutes > 0:
                time_str = f"{minutes}分{seconds}秒"
            else:
                time_str = f"{seconds}秒"
            claw_status = f"NOT_CREATED已过期/无环境（剩余{time_str}后创建）"

        # 计算自动禁用账号的冷却恢复倒计时
        auto_reenable_remaining = 0
        if disabled_info and disabled_info.get("auto", False):
            from datetime import datetime
            from .manager import AUTO_DISABLE_COOLDOWN_SECONDS
            disabled_at_str = disabled_info.get("disabled_at", "")
            if disabled_at_str:
                try:
                    disabled_at_dt = datetime.strptime(disabled_at_str, "%Y-%m-%d %H:%M:%S")
                    elapsed = (datetime.now() - disabled_at_dt).total_seconds()
                    remaining = AUTO_DISABLE_COOLDOWN_SECONDS - elapsed
                    if remaining > 0:
                        auto_reenable_remaining = int(remaining)
                except (ValueError, TypeError):
                    pass

        # 构造禁用原因显示文本（自动禁用附带倒计时）
        disabled_reason_display = None
        if disabled_info:
            reason = disabled_info.get("reason", "")
            if disabled_info.get("auto", False) and auto_reenable_remaining > 0:
                hours = auto_reenable_remaining // 3600
                mins = (auto_reenable_remaining % 3600) // 60
                secs = auto_reenable_remaining % 60
                if hours > 0:
                    countdown_str = f"{hours}时{mins}分{secs}秒"
                elif mins > 0:
                    countdown_str = f"{mins}分{secs}秒"
                else:
                    countdown_str = f"{secs}秒"
                disabled_reason_display = f"{reason}（剩余{countdown_str}后自动恢复）"
            else:
                disabled_reason_display = reason

        users.append({
            "userId": uid,
            "name": data.get("name"),
            "serviceToken": data.get("serviceToken"),
            "claw_status": claw_status,
            "remain_sec": data.get("remain_sec", 0),
            "rebuild_delay_remaining": rebuild_delay_remaining,
            "disabled": disabled_info is not None,
            "disabled_reason": disabled_reason_display,
            "disabled_at": disabled_info.get("disabled_at") if disabled_info else None,
            "disabled_auto": disabled_info.get("auto", False) if disabled_info else False,
            "auto_reenable_remaining": auto_reenable_remaining,
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

        # 已存在该 uid → 保留原有的备注名等可编辑字段（仅刷新凭证），避免覆盖用户手工设置的 name。
        existing_name = None
        if os.path.exists(target_file):
            try:
                with open(target_file, "r", encoding="utf-8") as _f:
                    _old = json.load(_f) or {}
                existing_name = (_old.get("name") or "").strip() or None
            except Exception:
                existing_name = None

        user_data = {
            "userId": uid,
            "serviceToken": st,
            "xiaomichatbot_ph": ph,
            "name": existing_name or f"Imported_{uid}",
        }
        from .manager import atomic_write_json
        atomic_write_json(target_file, user_data)
        _invalidate_user_alias_cache()

        return JSONResponse({"status": "ok", "userId": uid})
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)

@router.post("/api/users/destroy/{uid}")
async def api_users_destroy(uid: str):
    """手动销毁单个账号的 miclaw 容器（仅销毁，不重建）"""
    from urllib.parse import quote
    from .manager import make_claw_action_http_client

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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    destroy_url = f"https://aistudio.xiaomimimo.com/open-apis/user/mimo-claw/destroy?xiaomichatbot_ph={quote(ph)}"

    try:
        async with await make_claw_action_http_client(timeout=15) as client:
            r = await client.post(destroy_url, cookies=cookies, headers=headers, timeout=15)
            if r.status_code == 401:
                return JSONResponse({"detail": "凭证已过期 (401)，请重新导入 Cookie"}, status_code=401)
            data = r.json()
            status = data.get("data", {}).get("status", "")
    except Exception as e:
        return JSONResponse({"detail": f"销毁请求异常: {e}"}, status_code=502)

    return JSONResponse({
        "status": "ok",
        "message": f"账号 {uid} 的 miclaw 容器已销毁",
        "claw_status": status,
    })


@router.patch("/api/users/rename/{uid}")
async def api_users_rename(uid: str, request: Request):
    """修改账号备注名（仅更新 name 字段，不影响凭证与生命周期）"""
    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if not os.path.exists(target_file):
        return JSONResponse({"detail": "User not found"}, status_code=404)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "请求体不是合法 JSON"}, status_code=400)

    new_name = str(body.get("name", "")).strip()
    if not new_name:
        return JSONResponse({"detail": "备注名不能为空"}, status_code=400)

    try:
        with open(target_file, "r", encoding="utf-8") as f:
            user_data = json.load(f)
        user_data["name"] = new_name
        from .manager import atomic_write_json
        atomic_write_json(target_file, user_data)
        _invalidate_user_alias_cache()
    except Exception as e:
        return JSONResponse({"detail": f"保存失败: {e}"}, status_code=500)

    return JSONResponse({"status": "ok", "name": new_name, "message": f"备注名已更新为「{new_name}」"})


@router.delete("/api/users/delete/{uid}")
async def api_users_delete(uid: str):
    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if not os.path.exists(target_file):
        return JSONResponse({"detail": "User not found"}, status_code=404)

    os.remove(target_file)
    _invalidate_user_alias_cache()

    # 撤销该 uid 的所有会话注册码 → bridge 无法再重连
    revoked = _revoke_sessions_for_uid(uid)

    # 主动踢除该 uid 当前在线的 ws 连接
    for ws in list(state.active_clients):
        if state.client_uid_map.get(id(ws)) == uid:
            try:
                await ws.close(code=4001)
            except Exception:
                pass

    return JSONResponse({"status": "ok", "revoked_sessions": revoked})


@router.post("/api/users/disable/{uid}")
async def api_users_disable(uid: str):
    """手动禁用账号：标记禁用，不触发销毁 mimo-claw"""
    from .manager import disable_account, is_account_disabled, load_disabled_accounts

    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if not os.path.exists(target_file):
        return JSONResponse({"detail": "User not found"}, status_code=404)

    # 如果已经是手动禁用状态，则无需重复操作
    disabled_accounts = load_disabled_accounts()
    disabled_info = disabled_accounts.get(str(uid))
    if disabled_info and not disabled_info.get("auto", False):
        return JSONResponse({"status": "ok", "message": "该账号已处于手动禁用状态"})

    # 标记为手动禁用（如果之前是自动禁用，会被覆盖为手动禁用）
    disable_account(uid, reason="WebUI 手动禁用", auto=False)

    # 主动从 gateway 侧关闭该 uid 对应的所有 WebSocket 连接。
    # 先撤销该 uid 的所有会话注册码 → 即使 close 失败，bridge 重连也会被 gateway 拒绝
    revoked = _revoke_sessions_for_uid(uid)

    evicted_count = 0
    stale_ws_list = [
        ws for ws in list(state.active_clients)
        if state.client_uid_map.get(id(ws)) == uid
    ]
    for ws in stale_ws_list:
        try:
            await ws.close(code=4001)  # 4001 = 账号已禁用，bridge 收到后停止重连
            evicted_count += 1
        except Exception:
            pass

    return JSONResponse({
        "status": "ok",
        "message": f"账号 {uid} 已禁用（不触发容器销毁）",
        "evicted_ws_nodes": evicted_count,
        "revoked_sessions": revoked,
    })


@router.post("/api/users/enable/{uid}")
async def api_users_enable(uid: str):
    """解除禁用账号：移除禁用标记，下一轮热加载会自动拉起任务"""
    from .manager import enable_account, is_account_disabled

    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if not os.path.exists(target_file):
        return JSONResponse({"detail": "User not found"}, status_code=404)

    if not is_account_disabled(uid):
        return JSONResponse({"status": "ok", "message": "该账号未被禁用"})

    enable_account(uid)
    return JSONResponse({"status": "ok", "message": f"账号 {uid} 已启用，将在数秒内自动拉起生命周期任务"})


# ----------------- 代理配置 API -----------------

# 复用 manager.py 中的 PROXY_CONFIG_FILE，避免两侧路径解析逻辑出现分歧
# （否则 WebUI 写到一处、_get_proxy_url 读到另一处，配置看似保存却不生效）
from .manager import PROXY_CONFIG_FILE


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

    try:
        from .manager import atomic_write_json
        atomic_write_json(PROXY_CONFIG_FILE, {"proxy_url": proxy_url})
    except Exception as e:
        return JSONResponse({"detail": f"保存失败: {e}"}, status_code=500)

    return JSONResponse({"status": "ok", "proxy_url": proxy_url, "message": "代理已保存，即时生效"})


@router.delete("/api/proxy")
async def api_delete_proxy():
    """清除代理配置（删除 proxy_config.json，回退到环境变量或直连）"""
    if os.path.exists(PROXY_CONFIG_FILE):
        os.remove(PROXY_CONFIG_FILE)
    return JSONResponse({"status": "ok", "message": "代理已清除，将回退到环境变量或直连"})


# ----------------- 携趣 IP 短效代理 API -----------------

# 同上，复用 manager.py 的 XIEQU_CONFIG_FILE，让两侧读写完全一致
from .manager import XIEQU_CONFIG_FILE


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

    try:
        from .manager import atomic_write_json
        atomic_write_json(XIEQU_CONFIG_FILE, {"api_url": api_url})
    except Exception as e:
        return JSONResponse({"detail": f"保存失败: {e}"}, status_code=500)

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



# ----------------- AI API Key 管理（多 Key + WebUI 热加载） -----------------


def _mask_api_key(raw_key: str) -> str:
    """把 Key 显示为 `sk-abcd...wxyz` 样式，避免在 WebUI 中明文回显。"""
    if not raw_key:
        return ""
    if len(raw_key) <= 10:
        return raw_key[:2] + "***"
    return f"{raw_key[:6]}...{raw_key[-4:]}"


@router.get("/api/keys")
async def api_keys_list():
    """列出当前所有 AI API Key（环境变量 + WebUI 文件）。返回都是脱敏 preview。

    每条 Key 还会附带按 Key 维度统计的实时用量数据：
      - ``usage.requests_total / requests_succeeded / requests_failed``
      - ``usage.prompt_tokens / completion_tokens / total_tokens``
      - ``usage.last_used_at``（最近一次命中该 Key 的 Unix 时间戳，未使用则为 None）

    若网关曾在 "未启用鉴权" 模式下处理过流量，会额外返回一条 ``id="anonymous"``、
    ``source="anonymous"`` 的合计行，便于运维感知未鉴权直通的情况。
    """
    items: list[dict] = []
    keys_metrics = state.metrics.get("keys", {}) or {}

    def _usage_for(key_id: str) -> dict:
        kv = keys_metrics.get(key_id) or {}
        return {
            "requests_total": int(kv.get("requests_total", 0)),
            "requests_succeeded": int(kv.get("requests_succeeded", 0)),
            "requests_failed": int(kv.get("requests_failed", 0)),
            "prompt_tokens": int(kv.get("prompt_tokens", 0)),
            "completion_tokens": int(kv.get("completion_tokens", 0)),
            "total_tokens": int(kv.get("total_tokens", 0)),
            "last_used_at": int(kv.get("last_used_at", 0)) or None,
        }

    env_key = os.getenv(AI_AUTH_ENV, "").strip()
    if env_key:
        items.append({
            "id": "env",
            "name": f"环境变量 ({AI_AUTH_ENV})",
            "masked": _mask_api_key(env_key),
            "source": "env",
            "deletable": False,
            "created_at": None,
            "usage": _usage_for("env"),
        })

    file_keys = _load_extra_ai_keys()
    for k in file_keys:
        items.append({
            "id": k.get("id"),
            "name": k.get("name") or "",
            "masked": _mask_api_key(k.get("key") or ""),
            "source": "file",
            "deletable": True,
            "created_at": k.get("created_at") or None,
            "usage": _usage_for(k.get("id") or ""),
        })

    # 仅在 anonymous 通道实际产生过流量时才加入合计行，避免空列污染 UI
    anonymous_usage = _usage_for("anonymous")
    if anonymous_usage["requests_total"] > 0:
        items.append({
            "id": "anonymous",
            "name": "未鉴权直通流量 (anonymous)",
            "masked": "—",
            "source": "anonymous",
            "deletable": False,
            "created_at": None,
            "usage": anonymous_usage,
        })

    return JSONResponse({
        "ai_auth_enabled": bool(env_key) or bool(file_keys),
        "env_key_set": bool(env_key),
        "keys": items,
        "config_file": AI_KEYS_CONFIG_FILE,
    })


@router.post("/api/keys")
async def api_keys_add(request: Request):
    """添加一条新的 AI API Key。

    请求体（均可选）：
      - ``name``: 备注名
      - ``key``:  自定义 Key，留空则服务器随机生成 ``sk-<token_urlsafe(32)>``

    响应中会一次性返回完整 ``key`` 明文，前端需让用户立即复制保存——
    后续所有列表 API 只会返回脱敏 preview。
    """
    try:
        body = await request.json()
    except Exception:
        body = {}

    name = str((body or {}).get("name") or "").strip()
    explicit_key = str((body or {}).get("key") or "").strip()

    if explicit_key:
        if len(explicit_key) < 8:
            return JSONResponse({"detail": "自定义 Key 至少需要 8 个字符"}, status_code=400)
        if any(ch.isspace() for ch in explicit_key):
            return JSONResponse({"detail": "Key 不能包含空白字符"}, status_code=400)
        new_key = explicit_key
    else:
        new_key = "sk-" + secrets.token_urlsafe(32)

    # 防止与已有 Key（环境变量 / 文件）重复
    if new_key in get_all_ai_api_keys():
        return JSONResponse({"detail": "该 Key 已存在"}, status_code=400)

    keys = _load_extra_ai_keys()
    new_id = "k_" + secrets.token_hex(8)
    item = {
        "id": new_id,
        "key": new_key,
        "name": name or f"key-{new_id[2:8]}",
        "created_at": int(time.time()),
    }
    keys.append(item)
    _save_extra_ai_keys(keys)

    return JSONResponse({
        "status": "ok",
        "id": new_id,
        "key": new_key,            # 仅本次返回明文，后续只能拿到 masked
        "name": item["name"],
        "masked": _mask_api_key(new_key),
        "created_at": item["created_at"],
        "message": "API Key 已添加，立即生效",
    })


@router.post("/api/keys/{key_id}/reset-usage")
async def api_keys_reset_usage(key_id: str):
    """重置某条 Key 的用量统计（归零请求数 / Token 数 / 最近使用时间）。

    适用于所有来源的 Key（环境变量 / WebUI 文件 / anonymous），不影响 Key 本身是否有效。
    """
    keys_bucket = state.metrics.get("keys")
    if not keys_bucket or key_id not in keys_bucket:
        return JSONResponse({"detail": "该 Key 尚无任何用量记录"}, status_code=404)

    keys_bucket[key_id] = {
        "requests_total": 0,
        "requests_succeeded": 0,
        "requests_failed": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "last_used_at": 0,
    }
    return JSONResponse({"status": "ok", "message": f"Key {key_id} 的用量统计已重置为零"})


@router.delete("/api/keys/{key_id}")
async def api_keys_delete(key_id: str):
    """删除一条 WebUI 添加的 Key（环境变量 Key 不允许从 WebUI 删除）。"""
    if not key_id or key_id == "env":
        return JSONResponse(
            {"detail": "环境变量配置的 Key 不能在 WebUI 删除，请修改 .env 后重启"},
            status_code=400,
        )

    keys = _load_extra_ai_keys()
    new_keys = [k for k in keys if k.get("id") != key_id]
    if len(new_keys) == len(keys):
        return JSONResponse({"detail": "Key 不存在"}, status_code=404)

    _save_extra_ai_keys(new_keys)
    return JSONResponse({"status": "ok", "message": "API Key 已删除，立即生效"})



# ----------------- 内网通信节点连接管理 -----------------


@router.get("/api/clients")
async def api_clients_list():
    """列出当前所有保持长连接的内网通信节点（active /ws 客户端）的实时状态。

    每条返回：
      - ``id``: 字符串形式的 ``id(ws)``，用于断开 API 定位 WS 对象。
      - ``uid``: 该连接归属的账号 userId（bridge.py 通过 ?uid=... 上报；
        未上报时为空字符串）。
      - ``name``: 该 uid 在 ``users/user_<uid>.json`` 中持久化的备注名（WebUI 可点击编辑）。
        无备注名 / uid 缺失时为空字符串，由前端决定显示占位。
      - ``host`` / ``port``: 节点的源 IP 与端口
      - ``connected_at``: Unix 时间戳；``duration_seconds`` 已运行秒数
      - ``in_cooldown`` / ``cooldown_remaining_seconds``: 冷却状态（如 401 触发的临时跳过）
      - ``cooldown_count``: 该节点累计进入冷却的次数（重连后清零；
        达到 ``MIMO_NODE_COOLDOWN_REBUILD_THRESHOLD`` 会自动按 uid 触发该账号定向重建，
        uid 缺失时 fallback 全局重建）
      - ``pending_requests``: 当前正由该节点处理中的请求队列数
      - ``current``: 是否是下一次 round-robin 命中的节点（仅作展示）
    """
    import time as _time

    now = _time.time()

    # uid -> 备注名 映射（10s TTL 缓存，避免 ~3s 一次的 /api/clients 轮询每次都全量扫盘）。
    # 备注名变更后下一轮自然刷新；rename / add / delete 已显式 invalidate 缓存做即时反映。
    uid_to_name = _load_user_aliases_cached()

    items: list[dict] = []
    # 注意：state.active_clients 列表索引会随删除变化，不能依赖；用 id(ws) 当稳定 key
    for index, ws in enumerate(state.active_clients):
        ws_id = id(ws)
        connected_at = state.client_connected_at.get(ws_id, 0)
        cooldown_until = state.client_cooldowns.get(ws_id, 0)
        in_cooldown = cooldown_until > now
        cooldown_count = state.client_cooldown_counts.get(ws_id, 0)
        bridge_uid = state.client_uid_map.get(ws_id, "")
        bridge_name = uid_to_name.get(bridge_uid, "") if bridge_uid else ""
        host = ws.client.host if ws.client else "Unknown"
        port = ws.client.port if ws.client else 0
        items.append({
            "id": str(ws_id),
            "uid": bridge_uid,
            "name": bridge_name,
            "index": index,
            "host": host,
            "port": port,
            "address": f"{host}:{port}",
            "connected_at": int(connected_at) if connected_at else None,
            "duration_seconds": int(now - connected_at) if connected_at else None,
            "in_cooldown": in_cooldown,
            "cooldown_until": int(cooldown_until) if in_cooldown else None,
            "cooldown_remaining_seconds": max(0, int(cooldown_until - now)) if in_cooldown else 0,
            "cooldown_count": cooldown_count,
            "pending_requests": len(state.ws_to_req_ids.get(ws_id, set())),
            "current": index == state.current_client_index,
        })

    return JSONResponse({
        "total": len(items),
        "available": sum(1 for x in items if not x["in_cooldown"]),
        "clients": items,
    })


@router.post("/api/clients/{client_id}/disconnect")
async def api_clients_disconnect(client_id: str):
    """强制断开一个内网通信节点的 WebSocket 连接。

    ``client_id`` 是 ``GET /api/clients`` 中返回的 ``id`` 字段（即 ``id(ws)`` 的字符串形式）。
    断开后 ``ws_tunnel`` 的 finally 分支会自然回收 ``active_clients`` / 冷却状态 /
    孤儿请求队列；该节点对应的 Claw 容器内 bridge.py 通常会在 3s 后自动重连
    （重连时仍需携带有效 session_token 才能通过验证）。
    """
    try:
        target_ws_id = int(client_id)
    except (TypeError, ValueError):
        return JSONResponse({"detail": "client_id 不是合法的整数"}, status_code=400)

    target_ws = next((ws for ws in state.active_clients if id(ws) == target_ws_id), None)
    if target_ws is None:
        return JSONResponse({"detail": "该客户端不在当前在线列表中（可能刚刚断开）"}, status_code=404)

    addr = f"{target_ws.client.host}:{target_ws.client.port}" if target_ws.client else "Unknown"
    # fire-and-forget close：半死 ws 的关闭帧可能要等 TCP 重传定时器才能返回，直接 await 会
    # 把请求 worker 卡住数十秒。这里只把"关闭信号"派发出去就立即返回；ws_tunnel 的
    # finally 段会自然回收 active_clients / cooldown / 孤儿请求队列等所有状态。
    _fire_and_forget_close(target_ws, code=1000)
    return JSONResponse({
        "status": "ok",
        "message": f"已派发关闭信号给节点 {addr}，连接将在数秒内从 active_clients 列表中移除",
    })
