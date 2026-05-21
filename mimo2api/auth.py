import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

AI_AUTH_ENV = "MIMO_RELAY_OPENAI_KEY"
WEBUI_USERNAME_ENV = "MIMO_WEBUI_USERNAME"
WEBUI_PASSWORD_ENV = "MIMO_WEBUI_PASSWORD"
WEBUI_SECRET_ENV = "MIMO_WEBUI_SECRET"
WEBUI_SESSION_TTL_ENV = "MIMO_WEBUI_SESSION_TTL_SECONDS"
WEBUI_COOKIE_NAME_ENV = "MIMO_WEBUI_COOKIE_NAME"
WEBUI_COOKIE_SECURE_ENV = "MIMO_WEBUI_COOKIE_SECURE"

# 工作目录根（与 manager.py / ui_router.py 中 ROOT_DIR 一致）：mimi3/
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_ai_keys_path() -> str:
    """计算 api_keys.json 的实际持久化路径。

    优先级：``MIMO_AI_KEYS_PATH`` 环境变量 > ``<repo>/api_keys.json``。Docker 部署一般会把
    该变量指向 ``/app/data/api_keys.json``，让 WebUI 添加的 Key 也跟着 ``./data`` 卷一起持久化，
    容器重建不丢失。父目录在缺失时自动创建。
    """
    raw = os.getenv("MIMO_AI_KEYS_PATH", "").strip()
    path = raw if raw else os.path.join(_ROOT_DIR, "api_keys.json")
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        try:
            os.makedirs(parent, exist_ok=True)
        except Exception:
            pass
    return path


AI_KEYS_CONFIG_FILE = _resolve_ai_keys_path()


def _read_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


# ----------------- 多 Key 持久化（WebUI 热加载） -----------------

def _load_extra_ai_keys() -> list[dict]:
    """从 api_keys.json 读取通过 WebUI 添加的所有 Key。

    文件格式：``{"keys": [{"id": "k_xxx", "key": "sk-...", "name": "...", "created_at": 169...}, ...]}``
    每次调用都会重新读盘，从而实现 WebUI 添加/删除后立即热生效（无需重启）。
    """
    if not os.path.exists(AI_KEYS_CONFIG_FILE):
        return []
    try:
        with open(AI_KEYS_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception as exc:
        logger.warning(f"读取 api_keys.json 失败，将忽略文件中的 Key：{exc!r}")
        return []

    raw_keys = data.get("keys") or []
    out: list[dict] = []
    for item in raw_keys:
        if not isinstance(item, dict):
            continue
        key_value = (item.get("key") or "").strip()
        if not key_value:
            continue
        out.append({
            "id": str(item.get("id") or ""),
            "key": key_value,
            "name": str(item.get("name") or ""),
            "created_at": int(item.get("created_at") or 0),
        })
    return out


def _save_extra_ai_keys(keys: list[dict]) -> None:
    """覆盖写入 api_keys.json。"""
    payload = {"keys": keys}
    tmp_path = AI_KEYS_CONFIG_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, AI_KEYS_CONFIG_FILE)


def get_all_ai_api_keys() -> list[str]:
    """返回当前所有有效的 AI API Key（环境变量 + WebUI 文件，按顺序去重）。"""
    out: list[str] = []
    env_key = _read_env(AI_AUTH_ENV)
    if env_key:
        out.append(env_key)
    for item in _load_extra_ai_keys():
        v = item["key"]
        if v and v not in out:
            out.append(v)
    return out


def is_ai_auth_enabled() -> bool:
    """只要有任意一条 Key（环境变量或 WebUI 文件）就视为开启鉴权。"""
    return bool(get_all_ai_api_keys())


def is_web_auth_enabled() -> bool:
    return bool(_read_env(WEBUI_PASSWORD_ENV))


def get_ai_api_key() -> str:
    """返回环境变量配置的主 Key（保留给 _get_webui_secret 等老调用做后备签名密钥用）。"""
    return _read_env(AI_AUTH_ENV)


def get_webui_username() -> str:
    return _read_env(WEBUI_USERNAME_ENV, "admin") or "admin"


def get_webui_password() -> str:
    return _read_env(WEBUI_PASSWORD_ENV)


def get_webui_cookie_name() -> str:
    return _read_env(WEBUI_COOKIE_NAME_ENV, "mimo_webui_session") or "mimo_webui_session"


def get_webui_session_ttl() -> int:
    raw_value = _read_env(WEBUI_SESSION_TTL_ENV, "43200")
    try:
        return max(300, int(raw_value))
    except ValueError:
        return 43200


def webui_cookie_secure() -> bool:
    return _read_env(WEBUI_COOKIE_SECURE_ENV).lower() in {"1", "true", "yes", "on"}


def _get_webui_secret() -> str:
    secret_value = _read_env(WEBUI_SECRET_ENV)
    if secret_value:
        return secret_value
    password = get_webui_password()
    if password:
        return password
    return get_ai_api_key() or "mimo2-webui-fallback-secret"


def extract_ai_api_key(request: Request) -> str | None:
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    for header_name in ("x-api-key", "api-key"):
        header_value = request.headers.get(header_name, "").strip()
        if header_value:
            return header_value
    return None


def verify_ai_api_key(candidate: str | None) -> bool:
    expected_keys = get_all_ai_api_keys()
    if not expected_keys:
        # 未配置任何 Key → 不开启鉴权，全部放行
        return True
    if not candidate:
        return False
    # 与所有候选 Key 做常量时间比较；不提前 break，尽量减少 timing 信号
    valid = False
    for key in expected_keys:
        if secrets.compare_digest(candidate, key):
            valid = True
    return valid


def identify_ai_api_key(candidate: str | None) -> str:
    """识别 candidate 对应的 Key 标识，用于按 Key 维度的用量统计。

    返回值：
      - ``"env"``：匹配 ``MIMO_RELAY_OPENAI_KEY`` 环境变量配置的 Key
      - ``"k_xxx"``：匹配 ``api_keys.json`` 文件中该 id 对应的 Key
      - ``"anonymous"``：未启用鉴权直通流量，或 candidate 为空 / 不匹配任何已配置 Key
    """
    if candidate:
        env_key = _read_env(AI_AUTH_ENV)
        if env_key and secrets.compare_digest(candidate, env_key):
            return "env"
        for item in _load_extra_ai_keys():
            if secrets.compare_digest(candidate, item["key"]):
                return item["id"] or "anonymous"
    return "anonymous"


def require_ai_request(request: Request) -> JSONResponse | None:
    if not is_ai_auth_enabled():
        return None

    if verify_ai_api_key(extract_ai_api_key(request)):
        return None

    return JSONResponse(
        {
            "error": {
                "message": "Unauthorized: missing or invalid API key",
                "type": "invalid_request_error",
            }
        },
        status_code=401,
        headers={"WWW-Authenticate": "Bearer"},
    )


def verify_webui_login(username: str, password: str) -> bool:
    expected_username = get_webui_username()
    expected_password = get_webui_password()
    if not expected_password:
        return True
    return secrets.compare_digest(username or "", expected_username) and secrets.compare_digest(password or "", expected_password)


def _urlsafe_b64encode(raw_text: str) -> str:
    return base64.urlsafe_b64encode(raw_text.encode("utf-8")).decode("ascii").rstrip("=")


def _urlsafe_b64decode(encoded_text: str) -> str:
    padding = "=" * (-len(encoded_text) % 4)
    return base64.urlsafe_b64decode((encoded_text + padding).encode("ascii")).decode("utf-8")


def create_webui_session_token(username: str, now: int | None = None) -> str:
    issued_at = int(time.time() if now is None else now)
    payload = {
        "u": username,
        "iat": issued_at,
        "exp": issued_at + get_webui_session_ttl(),
    }
    payload_encoded = _urlsafe_b64encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    signature = hmac.new(
        _get_webui_secret().encode("utf-8"),
        payload_encoded.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_encoded}.{signature}"


def parse_webui_session_token(token: str | None, now: int | None = None) -> dict | None:
    if not token or "." not in token:
        return None
    payload_encoded, provided_signature = token.split(".", 1)
    expected_signature = hmac.new(
        _get_webui_secret().encode("utf-8"),
        payload_encoded.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not secrets.compare_digest(provided_signature, expected_signature):
        return None

    try:
        payload = json.loads(_urlsafe_b64decode(payload_encoded))
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        return None

    current_time = int(time.time() if now is None else now)
    if int(payload.get("exp", 0)) < current_time:
        return None
    return payload


def is_webui_authenticated(request: Request) -> bool:
    if not is_web_auth_enabled():
        return True
    token = request.cookies.get(get_webui_cookie_name())
    payload = parse_webui_session_token(token)
    return bool(payload and payload.get("u") == get_webui_username())


def require_webui_request(request: Request) -> JSONResponse | None:
    if is_webui_authenticated(request):
        return None
    return JSONResponse(
        {"detail": "Unauthorized"},
        status_code=401,
    )
