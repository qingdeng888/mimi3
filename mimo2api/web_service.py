import asyncio
import base64
import binascii
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, TextIO
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import os
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

MODEL_MAPPING_FILE = Path(__file__).parent.parent / "model_mapping.json"

# 引入 Manager 长驻协程任务
from .manager import start_manager_tasks

# Responses API 转换器
from .responses_converter import convert_request as responses_convert_request
from .responses_converter import convert_response as responses_convert_response
from .responses_converter import ResponsesStreamConverter
# Anthropic Messages API 转换器
from .anthropic_converter import (
    convert_anthropic_request,
    AnthropicStreamConverter,
)
from .audio_helpers import (
    AudioSpeechRequest,
    audio_media_type,
    extract_audio_payload,
    map_openai_tts_model,
    map_openai_tts_voice,
)
from .auth import (
    extract_ai_api_key,
    get_webui_username,
    identify_ai_api_key,
    is_ai_auth_enabled,
    is_web_auth_enabled,
    require_ai_request,
    require_webui_request,
)
from .metrics_store import (
    METRICS_BUCKET_SECONDS,
    METRICS_RETENTION_DAYS,
    build_gateway_stats,
    extract_usage_from_sse_chunk,
    init_metrics_db,
    load_status_history,
    metrics_history_worker,
    node_label,
    reclassify_history,
    record_attempt_finished,
    record_attempt_started,
    record_request_finished,
    record_request_started,
)

# 配置基础日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

manager_bg_task = None
metrics_persist_task = None
sweeper_bg_task = None
heartbeat_sweeper_task = None
single_process_lock_file = None
STALE_QUEUE_TTL = 300
# 心跳超时阈值（秒）：bridge 每 10s 发心跳，超过 35s 没收到 → 视为僵尸节点。
# 留出 3.5 倍心跳间隔的容忍度，避免偶尔网络抖动导致误杀。
HEARTBEAT_TIMEOUT = int(os.getenv("MIMO_HEARTBEAT_TIMEOUT", "35"))
# 心跳扫描间隔（秒）：后台协程每隔多久检查一次所有节点的心跳
HEARTBEAT_SCAN_INTERVAL = 15

def sweep_stale_queues_once(now: float | None = None) -> int:
    now = time.time() if now is None else now
    stale_count = 0
    for req_id, last_activity_at in list(state.req_id_timestamps.items()):
        if now - last_activity_at > STALE_QUEUE_TTL:
            logger.error(f"💀 发现长时间无活动的悬挂队列，强制回收: [{req_id[:8]}]")
            cleanup_pending_request(req_id)
            stale_count += 1
    if stale_count > 0:
        logger.info(f"🧹 垃圾回收周期结束，共清理了 {stale_count} 个泄露队列。当前活跃队列数: {len(state.pending_queues)}")
    return stale_count

async def sweep_stale_queues():
    """后台巡检任务，清理长时间无活动的悬挂请求队列。"""
    while True:
        try:
            await asyncio.sleep(60)
            sweep_stale_queues_once()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"清理死锁队列任务发生异常: {e}")


async def sweep_heartbeat_timeout():
    """后台巡检任务：定期扫描所有在线节点的心跳时间戳，超时未心跳的视为僵尸并踢除。

    设计意图：
      - bridge.py 每 10s 发送 {"type": "heartbeat"}，gateway 在 ws_tunnel 中更新 client_last_heartbeat。
      - 本协程每 HEARTBEAT_SCAN_INTERVAL 秒扫描一次，发现超过 HEARTBEAT_TIMEOUT 未心跳的节点后
        主动 close(1001)，触发 ws_tunnel 的 finally 分支自动回收所有关联状态。
      - 这是禁用时主动 close + 容器销毁间接断开之外的**最后一道兜底防线**：
        即使前两者都失败（Claw API 超时 / bridge nohup 残留 / TCP 半关闭），
        心跳超时后仍然会在 HEARTBEAT_TIMEOUT 内把僵尸节点从 active_clients 中清除。
    """
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SCAN_INTERVAL)
            now = time.time()
            # 复制一份避免迭代中修改
            for ws in list(state.active_clients):
                ws_id = id(ws)
                last_hb = state.client_last_heartbeat.get(ws_id)
                if last_hb is None:
                    # 以 connected_at 作为兜底初始值
                    last_hb = state.client_connected_at.get(ws_id, now)
                if now - last_hb > HEARTBEAT_TIMEOUT:
                    uid = state.client_uid_map.get(ws_id, "未知")
                    addr = f"{ws.client.host}:{ws.client.port}" if ws.client else "Unknown"
                    logger.warning(
                        f"💀 节点 {addr} (uid={uid}) 心跳超时 "
                        f"({int(now - last_hb)}s > {HEARTBEAT_TIMEOUT}s)，判定为僵尸，主动踢除。"
                    )
                    try:
                        await ws.close(code=1001)  # Going Away
                    except Exception:
                        pass  # close 失败不致命，ws_tunnel finally 仍会回收
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"心跳超时巡检任务发生异常: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager_bg_task, metrics_persist_task, sweeper_bg_task, heartbeat_sweeper_task
    logger.info("🚀 正在拉起挂后台的 Claw 账号守护线程...")
    acquire_single_process_lock()

    await asyncio.to_thread(init_metrics_db)
    fixed = await asyncio.to_thread(reclassify_history)
    if fixed:
        logger.info(f"🔧 重新分类了 {fixed} 条历史状态记录")
        
    manager_bg_task = asyncio.create_task(start_manager_tasks())
    metrics_persist_task = asyncio.create_task(metrics_history_worker())
    sweeper_bg_task = asyncio.create_task(sweep_stale_queues()) # 启动巡检死神
    heartbeat_sweeper_task = asyncio.create_task(sweep_heartbeat_timeout())  # 心跳 TTL 兜底
    logger.info(f"💓 心跳超时巡检已启动（超时阈值={HEARTBEAT_TIMEOUT}s，扫描间隔={HEARTBEAT_SCAN_INTERVAL}s）")
    
    yield
    
    for task in [manager_bg_task, metrics_persist_task, sweeper_bg_task, heartbeat_sweeper_task]:
        if task:
            task.cancel()
    if metrics_persist_task:
        try:
            await metrics_persist_task
        except asyncio.CancelledError:
            pass
    release_single_process_lock()

app = FastAPI(lifespan=lifespan)

# 全局状态从 gateway_state 引入
from .gateway_state import state

# 注入前面拆分出的 WebUI 独立路由
from .ui_router import router as ui_router
app.include_router(ui_router)

RETRYABLE_STATUS_CODES = {401, 403, 429}
NODE_RESPONSE_TIMEOUT = 30
MAX_RETRIES = 3
MAX_PENDING_QUEUES = 2000
AI_ROUTE_PREFIXES = ("/v1/", "/anthropic/v1/")
WEBUI_PUBLIC_PATHS = {"/", "/webui", "/api/auth/session", "/api/auth/login", "/api/auth/logout"}

if is_ai_auth_enabled():
    logger.info("🔐 AI API 鉴权已启用")
if is_web_auth_enabled():
    logger.info(f"🔐 WebUI 鉴权已启用，登录用户: {get_webui_username()}")


def is_ai_route(path: str) -> bool:
    return path.startswith(AI_ROUTE_PREFIXES)


def is_webui_route(path: str) -> bool:
    return path.startswith("/api/") and path not in WEBUI_PUBLIC_PATHS


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    if is_ai_route(path):
        auth_error = require_ai_request(request)
        if auth_error is not None:
            return auth_error
        # 鉴权放行后，识别本次请求所用 Key 的 id（用于按 Key 维度的用量统计）。
        # 未启用鉴权 / 未携带 / 不匹配任何已配置 Key 时，统一计入 "anonymous"。
        request.state.api_key_id = identify_ai_api_key(extract_ai_api_key(request))

    if is_webui_route(path):
        auth_error = require_webui_request(request)
        if auth_error is not None:
            return auth_error

    return await call_next(request)


def diagnose_request(body_text: str) -> str:
    """从请求体中提取关键诊断信息，用于 400 错误追踪"""
    try:
        req = json.loads(body_text)
    except Exception:
        return "body=非法JSON"
    msgs = req.get("messages", [])
    model = req.get("model", "未指定")
    stream = req.get("stream", False)
    total_chars = sum(len(str(m.get("content", ""))) for m in msgs)
    est_tokens = total_chars // 3
    tools = req.get("tools", [])
    return (
        f"model={model}, stream={stream}, msgs={len(msgs)}, "
        f"est_tokens≈{est_tokens}, chars={total_chars}, tools={len(tools)}"
    )


def record_error(route: str, status_code: int, reason: str, model: str = "", detail: str = "", request_body: str = ""):
    """记录错误到环形缓冲区，可通过 /api/errors 查询"""
    state.recent_errors.append({
        "ts": int(time.time()),
        "route": route,
        "status": status_code,
        "reason": reason[:200],
        "model": model,
        "detail": detail[:500],
        "request": request_body[:2000] if request_body else "",
    })

STREAM_CHUNK_TIMEOUT = 60
STREAM_KEEPALIVE_INTERVAL = 25  # 秒，需小于 Cloudflare 超时 (~100s)
QUEUE_DRAIN_TIMEOUT = 5
DEFAULT_GATEWAY_ERROR = "Gateway Error: 所有节点请求失败"
NODE_401_COOLDOWN_SECONDS = int(os.getenv("MIMO_NODE_401_COOLDOWN_SECONDS", "30"))
# 同一节点累计冷却达到该阈值时，判定为坏号并主动断开该节点（不再触发重建）；
# 设为 0 表示关闭该自动断开特性（仅冷却，不断开）。
NODE_COOLDOWN_REBUILD_THRESHOLD = int(os.getenv("MIMO_NODE_COOLDOWN_REBUILD_THRESHOLD", "3"))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESS_LOCK_PATH = os.getenv("MIMO_PROCESS_LOCK_PATH", os.path.join(ROOT_DIR, "mimo2api.lock"))



# 后台 fire-and-forget 任务集合
_background_tasks: set[asyncio.Task] = set()
PROCESS_LOCK_SIZE = 1

def _track_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

def _lock_file_nonblocking(lock_file: TextIO) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise OSError("当前平台缺少 msvcrt，无法加锁。")
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, PROCESS_LOCK_SIZE)
        return

    if fcntl is None:
        raise OSError("当前平台缺少 fcntl，无法加锁。")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

def _unlock_file(lock_file: TextIO) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise OSError("当前平台缺少 msvcrt，无法解锁。")
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, PROCESS_LOCK_SIZE)
        return

    if fcntl is None:
        raise OSError("当前平台缺少 fcntl，无法解锁。")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

def acquire_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is not None:
        return

    try:
        lock_path = Path(PROCESS_LOCK_PATH)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.touch(exist_ok=True)
        lock_file = lock_path.open("r+", encoding="utf-8")
        if lock_path.stat().st_size < PROCESS_LOCK_SIZE:
            lock_file.write("\n")
            lock_file.flush()
        _lock_file_nonblocking(lock_file)
    except (BlockingIOError, OSError) as exc:
        if 'lock_file' in locals():
            lock_file.close()
        raise RuntimeError("当前进程锁被占用。") from exc

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    single_process_lock_file = lock_file

def release_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is None:
        return
    try:
        _unlock_file(single_process_lock_file)
    finally:
        single_process_lock_file.close()
        single_process_lock_file = None

@dataclass(slots=True)
class RetryState:
    status_code: int = 502
    response_text: str = DEFAULT_GATEWAY_ERROR

@dataclass(slots=True)
class ForwardAttempt:
    req_id: str
    queue: asyncio.Queue
    target_ws: WebSocket
    first_msg: dict[str, Any]
    attempt_number: int

@app.get("/api/stats")
async def api_stats():
    return JSONResponse(content=build_gateway_stats(len(_background_tasks)))

@app.get("/api/status/history")
async def api_status_history(hours: int = 24):
    hours = max(1, min(hours, 24 * METRICS_RETENTION_DAYS))
    return JSONResponse(content=await asyncio.to_thread(load_status_history, hours))

@app.get("/api/errors")
async def api_errors(limit: int = 50):
    limit = max(1, min(limit, 200))
    errors = list(state.recent_errors)[-limit:]
    errors.reverse()  # 最新的在前
    return JSONResponse(content={"count": len(errors), "errors": errors})

def load_model_mapping() -> dict[str, str]:
    if not MODEL_MAPPING_FILE.exists():
        return {}
    try:
        return json.loads(MODEL_MAPPING_FILE.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

def save_model_mapping(mapping: dict[str, str]) -> None:
    tmp = MODEL_MAPPING_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), "utf-8")
    tmp.rename(MODEL_MAPPING_FILE)

def apply_model_mapping(body_text: str) -> str:
    mapping = load_model_mapping()
    if not mapping:
        return body_text
    try:
        data = json.loads(body_text)
    except (json.JSONDecodeError, AttributeError):
        return body_text
    original_model = data.get("model")
    if original_model and original_model in mapping:
        data["model"] = mapping[original_model]
        logger.info(f"🔀 模型映射: {original_model} → {data['model']}")
        return json.dumps(data, ensure_ascii=False)
    return body_text

def _inject_stream_options(body_text: str) -> str:
    """为流式请求注入 stream_options: {include_usage: true}，确保上游返回 usage 数据用于统计。"""
    try:
        data = json.loads(body_text)
    except (json.JSONDecodeError, AttributeError):
        return body_text
    if not isinstance(data, dict):
        return body_text
    if "stream_options" not in data:
        data["stream_options"] = {"include_usage": True}
    elif isinstance(data["stream_options"], dict) and "include_usage" not in data["stream_options"]:
        data["stream_options"]["include_usage"] = True
    return json.dumps(data, ensure_ascii=False)

def _inject_openclaw_system_prompt(body_text: str) -> str:
    """为所有请求注入 OpenClaw 必需的系统提示词，避免 MiClaw 返回 400 错误。

    OpenAI 格式：在 messages 开头注入 system role
    处理逻辑：
      - 如果已有 system 消息，在其内容前追加提示词
      - 如果没有 system 消息，在 messages 开头插入新的 system 消息
    """
    REQUIRED_PROMPT = "You are a personal assistant running inside OpenClaw"

    try:
        data = json.loads(body_text)
    except (json.JSONDecodeError, AttributeError):
        return body_text

    if not isinstance(data, dict):
        return body_text

    messages = data.get("messages")
    if not isinstance(messages, list) or len(messages) == 0:
        return body_text

    # 检查第一条消息是否为 system
    if messages[0].get("role") == "system":
        # 已有 system 消息，在其内容前追加提示词
        existing_content = messages[0].get("content", "")
        if isinstance(existing_content, str):
            # 避免重复注入
            if REQUIRED_PROMPT not in existing_content:
                messages[0]["content"] = f"{REQUIRED_PROMPT}\n\n{existing_content}"
                logger.debug(f"💉 已在现有 system 消息前追加 OpenClaw 提示词")
        elif isinstance(existing_content, list):
            # content 为数组格式（多模态消息），在第一个 text 块前追加
            for block in existing_content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text", "")
                    if REQUIRED_PROMPT not in text:
                        block["text"] = f"{REQUIRED_PROMPT}\n\n{text}"
                        logger.debug(f"💉 已在现有 system 消息（多模态）前追加 OpenClaw 提示词")
                    break
    else:
        # 没有 system 消息，在开头插入
        messages.insert(0, {
            "role": "system",
            "content": REQUIRED_PROMPT
        })
        logger.debug(f"💉 已注入 OpenClaw 系统提示词到 messages 开头")

    data["messages"] = messages
    return json.dumps(data, ensure_ascii=False)

@app.get("/api/model_mapping")
async def api_get_model_mapping():
    return JSONResponse(content=load_model_mapping())

@app.put("/api/model_mapping")
async def api_put_model_mapping(request: Request):
    body = await request.body()
    try:
        new_mapping = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "请求体不是合法 JSON"}, status_code=400)
    if not isinstance(new_mapping, dict):
        return JSONResponse({"error": "映射必须是 JSON 对象"}, status_code=400)
    save_model_mapping(new_mapping)
    return JSONResponse(content=new_mapping)

@app.delete("/api/model_mapping/{model_name:path}")
async def api_delete_model_mapping(model_name: str):
    mapping = load_model_mapping()
    if model_name in mapping:
        del mapping[model_name]
        save_model_mapping(mapping)
        return JSONResponse({"ok": True, "deleted": model_name})
    return JSONResponse({"error": f"模型 {model_name} 不在映射中"}, status_code=404)

@app.websocket("/ws")
async def ws_tunnel(ws: WebSocket):
    client_addr = f"{ws.client.host}:{ws.client.port}" if ws.client else "Unknown"

    await ws.accept()

    # bridge.py 通过 ?uid=<account_userId> 上报本连接归属哪个账号。
    bridge_uid = ws.query_params.get("uid", "").strip()

    # 会话注册码验证：bridge 必须携带 ?session=<token>，该 token 由 manager 在创建/重建时生成并注册。
    # 禁用/删除账号时 session 被撤销 → bridge 无论如何重连都无法通过验证。
    bridge_session = ws.query_params.get("session", "").strip()
    if not bridge_session or bridge_session not in state.valid_sessions:
        logger.warning(f"🚫 拒绝无效/已撤销/缺失 session 的 /ws 连接: {client_addr} (session={bridge_session[:8] + '...' if bridge_session else '空'})")
        try:
            await ws.close(code=4001)  # 4001 = 会话无效，bridge 收到后停止重连
        except Exception:
            pass
        return

    # ⚠️ 关键顺序：必须先把自己注册进 active_clients + client_uid_map，再扫描去重。
    # 否则两条同 uid 新连接 A、B 几乎同时进入本协程时，A 完成 accept 但还没 append，
    # B 也完成 accept 时扫描到的同 uid 集合里只有"老的 X"而看不到 A → 双方都只驱逐 X，
    # 各自 append 自己 → 最终 active_clients 残留 A 和 B 两条同 uid 的节点（这正是上一版 fix 的失效场景）。
    # 先注册自己后扫描"除自己外"的同 uid，并发场景下 A 的去重会驱逐 B 之前已注册的项（反之亦然），
    # 最终只剩最后进来的那条。
    state.active_clients.append(ws)
    state.client_cooldowns.pop(id(ws), None)
    state.client_cooldown_counts.pop(id(ws), None)
    if bridge_uid:
        state.client_uid_map[id(ws)] = bridge_uid
    if bridge_session:
        state.client_session_map[id(ws)] = bridge_session
    state.client_connected_at[id(ws)] = time.time()
    state.client_last_heartbeat[id(ws)] = time.time()  # 初始心跳 = 接入时刻，避免刚连上就被 TTL 误判

    # 同 uid 去重：扫描"除自己外"的同 uid 旧连接，全部驱逐。
    # 业务背景：
    #   - 路径 B 的"反向重启"通常无法清理容器内 nohup 后台 bridge.py，导致老 bridge 与新 bridge
    #     在同一容器并存，从 WebUI 看到同一账号有 2+ 节点在线（且老连接已经是僵尸）。
    #   - 即便上游 kill 干净，旧 bridge 的 TCP RST 也可能比新 bridge 的连接更晚到。
    # 策略：保留最新连接，驱逐其他同 uid（新 bridge 持有最新 ticket/token，更可信）。
    # 老 ws 被 close(1012) 后，其 ws_tunnel 协程的 finally 段会自然回收 active_clients / cooldown /
    # uid_map / 孤儿请求队列等所有状态，无需在此重复清理（避免与 finally 抢占造成状态不一致）。
    if bridge_uid:
        my_id = id(ws)
        # 复制一份同 uid 的旧 ws 列表后再操作（注意排除自己）
        stale_ws_list = [
            old_ws for old_ws in list(state.active_clients)
            if id(old_ws) != my_id and state.client_uid_map.get(id(old_ws)) == bridge_uid
        ]
        for old_ws in stale_ws_list:
            old_addr = f"{old_ws.client.host}:{old_ws.client.port}" if old_ws.client else "Unknown"
            logger.warning(
                f"♻️ 同 uid={bridge_uid} 已有旧节点 {old_addr} 在线，"
                f"驱逐旧连接以让位给新连接 {client_addr}（避免 WebUI 出现重复节点）。"
            )
            try:
                # 1012 = Service Restart，语义上贴合"被新一轮 bridge 替换"的场景。
                await old_ws.close(code=1012)
            except Exception as e:
                # close 失败不是致命错误：旧 ws 的 receive_text 也会因 socket 状态变化而抛错并进入 finally。
                logger.warning(f"驱逐旧 ws (uid={bridge_uid}) 时 close 抛异常: {e}（finally 仍会回收）")
    uid_label = f" (uid={bridge_uid})" if bridge_uid else " (uid=未上报)"
    logger.info(f"✅ 内网节点已接入: {client_addr}{uid_label}。当前在线节点数: {len(state.active_clients)}")
    
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            # 心跳消息：bridge 每 10s 发送 {"type": "heartbeat"}，仅更新时间戳，不转发
            if data.get("type") == "heartbeat":
                state.client_last_heartbeat[id(ws)] = time.time()
                continue
            req_id = data.get("req_id")
            if req_id and req_id in state.pending_queues:
                touch_pending_request(req_id)
                state.pending_queues[req_id].put_nowait(data)
    except WebSocketDisconnect:
        logger.warning(f"❌ 内网节点主动断开: {client_addr}")
    except Exception as e:
        logger.error(f"❌ 内网节点异常断开: {client_addr}, 错误: {e}")
    finally:
        if ws in state.active_clients:
            state.active_clients.remove(ws)
        state.client_cooldowns.pop(id(ws), None)
        state.client_cooldown_counts.pop(id(ws), None)
        state.client_uid_map.pop(id(ws), None)
        state.client_connected_at.pop(id(ws), None)
        state.client_last_heartbeat.pop(id(ws), None)
        state.client_session_map.pop(id(ws), None)
        
        # 清理该节点的所有孤儿队列
        orphan_ids = state.ws_to_req_ids.pop(id(ws), set())
        for orphan_id in orphan_ids:
            q = state.pending_queues.pop(orphan_id, None)
            state.req_id_to_ws_id.pop(orphan_id, None)
            state.req_id_timestamps.pop(orphan_id, None)
            if q is not None:
                try:
                    q.put_nowait({"type": "error", "body": "节点断开连接"})
                except asyncio.QueueFull:
                    pass
        if orphan_ids:
            logger.warning(f"🧹 节点断开，已清理 {len(orphan_ids)} 个孤儿请求队列")
            
        if state.current_client_index >= len(state.active_clients):
            state.current_client_index = 0
        logger.info(f"当前在线节点数: {len(state.active_clients)}")


def get_next_client() -> WebSocket | None:
    if not state.active_clients:
        return None
    now = time.time()
    available_clients: list[WebSocket] = []
    for client in state.active_clients:
        if state.client_cooldowns.get(id(client), 0) <= now:
            available_clients.append(client)
    if not available_clients:
        return None
    if state.current_client_index >= len(available_clients):
        state.current_client_index = 0
    client = available_clients[state.current_client_index]
    state.current_client_index = (state.current_client_index + 1) % len(available_clients)
    return client


def get_available_client_count() -> int:
    now = time.time()
    return sum(1 for c in state.active_clients if state.client_cooldowns.get(id(c), 0) <= now)


def touch_pending_request(req_id: str) -> None:
    if req_id in state.pending_queues:
        state.req_id_timestamps[req_id] = time.time()


def create_pending_request() -> tuple[str, asyncio.Queue]:
    if len(state.pending_queues) >= MAX_PENDING_QUEUES:
        raise RuntimeError("pending queue 已满")
    req_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue()
    state.pending_queues[req_id] = queue
    state.req_id_timestamps[req_id] = time.time()
    return req_id, queue


def cleanup_pending_request(req_id: str) -> None:
    state.pending_queues.pop(req_id, None)
    state.req_id_timestamps.pop(req_id, None)
    ws_id = state.req_id_to_ws_id.pop(req_id, None)
    if ws_id is not None:
        req_ids = state.ws_to_req_ids.get(ws_id)
        if req_ids is not None:
            req_ids.discard(req_id)
            if not req_ids:
                state.ws_to_req_ids.pop(ws_id, None)


def cooldown_client(ws: WebSocket, seconds: int, reason: str) -> None:
    cooldown_until = time.time() + max(seconds, 0)
    state.client_cooldowns[id(ws)] = cooldown_until

    # 累计第几次进入冷却（重连/断开时会被清零）
    count = state.client_cooldown_counts.get(id(ws), 0) + 1
    state.client_cooldown_counts[id(ws)] = count

    threshold = NODE_COOLDOWN_REBUILD_THRESHOLD
    if threshold > 0 and count >= threshold:
        # 累计冷却次数达到阈值，判定为坏号，主动断开该节点（不触发重建）
        logger.error(
            f"🔥 节点 {node_label(ws)} 因 {reason} 累计冷却第 {count} 次（阈值 {threshold}），"
            f"判定为坏号 → 主动断开该节点。"
        )
        try:
            _track_task(asyncio.create_task(ws.close(code=1000)))
        except Exception:
            pass
    else:
        logger.warning(
            f"⛔ 节点 {node_label(ws)} 因 {reason} 进入冷却 {seconds}s "
            f"(累计第 {count}{'/' + str(threshold) if threshold > 0 else ''} 次)，"
            f"冷却结束时间戳: {int(cooldown_until)}"
        )

async def drain_and_close(req_id: str, queue: asyncio.Queue) -> None:
    try:
        while True:
            msg = await asyncio.wait_for(queue.get(), timeout=QUEUE_DRAIN_TIMEOUT)
            if msg.get("type") in ["finish", "error"]:
                break
    except Exception:
        pass
    finally:
        cleanup_pending_request(req_id)

def should_retry_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or status_code >= 500

def build_ws_payload(req_id: str, method: str, path: str, body: str) -> str:
    return json.dumps({"req_id": req_id, "method": method, "path": path, "body": body})

async def dispatch_to_node(*, method: str, path: str, body: str, log_label: str, attempt_number: int) -> ForwardAttempt | None:
    try:
        req_id, queue = create_pending_request()
    except RuntimeError:
        logger.warning("⚠️ pending queue 已满，拒绝新请求")
        return None
        
    target_ws = get_next_client()
    if not target_ws:
        cleanup_pending_request(req_id)
        return None

    # 🌟 修复内存泄漏的双向绑定：既知道 WS 管哪些 req_id，也知道 req_id 归属于哪个 WS
    state.req_id_to_ws_id[req_id] = id(target_ws)
    state.ws_to_req_ids.setdefault(id(target_ws), set()).add(req_id)

    ws_payload = build_ws_payload(req_id, method, path, body)
    attempt_started_at = time.monotonic()
    record_attempt_started(target_ws)

    try:
        await target_ws.send_text(ws_payload)
        logger.debug(f"👉 {log_label} [{req_id[:8]}] ({method} {path}) -> 节点: {node_label(target_ws)} (尝试 {attempt_number})")
    except RuntimeError:
        record_attempt_finished(target_ws=target_ws, status_code=0, first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000, success=False)
        logger.warning(f"⚠️ {log_label} 转发失败，节点状态异常，尝试切换...")
        cleanup_pending_request(req_id) # 内部会自动解绑 target_ws
        if target_ws in state.active_clients:
            state.active_clients.remove(target_ws)
        state.client_cooldowns.pop(id(target_ws), None)
        return None

    try:
        first_msg = await asyncio.wait_for(queue.get(), timeout=NODE_RESPONSE_TIMEOUT)
    except asyncio.TimeoutError:
        record_attempt_finished(target_ws=target_ws, status_code=504, first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000, success=False)
        raise

    # 打印 bridge 返回的首条响应信息
    logger.debug(
        f"📨 [{req_id[:8]}] bridge 首条响应: type={first_msg.get('type')}, "
        f"status={first_msg.get('status', '-')}, "
        f"headers={json.dumps(dict(list(first_msg.get('headers', {}).items())[:5]), ensure_ascii=False)[:200] if first_msg.get('headers') else '-'}"
    )
    if first_msg.get("type") == "error":
        logger.warning(f"❌ [{req_id[:8]}] bridge 返回错误: {first_msg.get('body', '')[:500]}")

    record_attempt_finished(
        target_ws=target_ws,
        status_code=int(first_msg.get("status", 200)),
        first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000,
        success=first_msg.get("type") != "error" and not should_retry_status(int(first_msg.get("status", 200))),
    )
    return ForwardAttempt(req_id=req_id, queue=queue, target_ws=target_ws, first_msg=first_msg, attempt_number=attempt_number)


async def prepare_forward_attempt(*, method: str, path: str, body: str, log_label: str, retry_state: RetryState, attempt_number: int) -> ForwardAttempt | None:
    attempt = await dispatch_to_node(method=method, path=path, body=body, log_label=log_label, attempt_number=attempt_number)
    if attempt is None:
        return None

    first_msg = attempt.first_msg
    if first_msg.get("type") == "error":
        error_text = first_msg.get("body") or "节点返回错误"
        logger.warning(f"⚠️ {log_label} 节点返回内部错误: {error_text}，尝试切换...")
        retry_state.response_text = f"Gateway Error: {error_text}"
        cleanup_pending_request(attempt.req_id)
        return None

    status_code = first_msg.get("status", 200)
    if status_code == 401:
        cooldown_client(attempt.target_ws, NODE_401_COOLDOWN_SECONDS, "401 Unauthorized")
        retry_state.status_code = 401
        retry_state.response_text = "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点"

    if should_retry_status(status_code):
        logger.warning(f"⚠️ {log_label} 节点返回状态码 {status_code}，触发自动重试 (当前 attempt={attempt_number})...")
        retry_state.status_code = status_code
        _track_task(asyncio.create_task(drain_and_close(attempt.req_id, attempt.queue)))
        return None

    return attempt


def normalize_response_headers(headers: dict | None) -> tuple[str, dict]:
    response_headers = dict(headers or {})
    content_type = response_headers.pop("content-type", "application/json")
    for key in ["content-length", "transfer-encoding", "content-encoding", "connection"]:
        response_headers.pop(key, None)
    return content_type, response_headers


async def collect_response_body(current_req_id: str, current_queue: asyncio.Queue, timeout: int = 120) -> str:
    chunks: list[str] = []
    try:
        while True:
            msg = await asyncio.wait_for(current_queue.get(), timeout=timeout)
            if msg.get("type") == "finish":
                break
            if msg.get("type") == "error":
                raise RuntimeError(msg.get("body") or "节点返回错误")
            if msg.get("type") == "chunk":
                chunks.append(msg.get("body", ""))
    finally:
        cleanup_pending_request(current_req_id)
    return "".join(chunks)

# -------------- API 路由定义 --------------

@app.post("/v1/audio/speech")
async def audio_speech_handler(payload: AudioSpeechRequest, request: Request):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    input_text = payload.input.strip()
    if not input_text:
        return JSONResponse({"error": {"message": "`input` 不能为空"}}, status_code=400)

    messages = []
    if isinstance(payload.instructions, str) and payload.instructions.strip():
        messages.append({"role": "user", "content": payload.instructions})
    messages.append({"role": "assistant", "content": input_text})

    mimo_payload = {
        "model": map_openai_tts_model(payload.model),
        "messages": messages,
        "audio": {"format": payload.response_format.lower(), "voice": map_openai_tts_voice(payload.voice)},
    }
    body_text = json.dumps(mimo_payload, ensure_ascii=False)
    # 注入 OpenClaw 必需的系统提示词
    body_text = _inject_openclaw_system_prompt(body_text)
    
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)
        
    retry_state = RetryState()
    route_key = "/v1/audio/speech"
    request_started_at = time.monotonic()
    api_key_id = getattr(request.state, "api_key_id", None)
    record_request_started(route_key, is_streaming=False, api_key_id=api_key_id)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method="POST", path="/v1/chat/completions", body=body_text, log_label="TTS 映射请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            first_byte_at = time.monotonic()

            raw_body = await collect_response_body(req_id, queue)
            status_code = first_msg.get("status", 200)
            
            if status_code >= 400:
                record_error(route_key, status_code, f"上游返回 {status_code}", detail=raw_body[:500])
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            try:
                response_json = json.loads(raw_body)
            except json.JSONDecodeError:
                record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                return JSONResponse({"error": {"message": "上游 TTS 返回了非法 JSON"}}, status_code=502)

            audio_b64, actual_format = extract_audio_payload(response_json)
            if not audio_b64:
                record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                return JSONResponse({"error": {"message": "上游 TTS 响应里没有音频数据"}}, status_code=502)

            try:
                audio_bytes = base64.b64decode(audio_b64, validate=True)
            except binascii.Error:
                try:
                    audio_bytes = base64.b64decode(audio_b64)
                except (binascii.Error, TypeError):
                    record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                    return JSONResponse({"error": {"message": "上游 TTS 音频数据损坏"}}, status_code=502)

            record_request_finished(route_key=route_key, status_code=200, started_at=request_started_at, first_byte_at=first_byte_at, success=True, api_key_id=api_key_id)
            return Response(audio_bytes, media_type=audio_media_type((actual_format or payload.response_format).lower()))

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue
        except RuntimeError as exc:
            retry_state.status_code = 502
            retry_state.response_text = f"Gateway Error: {exc}"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False, api_key_id=api_key_id)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

@app.post("/v1/responses")
async def responses_handler(request: Request):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    try:
        req_body = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
        chat_req = responses_convert_request(req_body)
    except Exception as exc:
        record_error("/v1/responses", 400, f"请求解析/转换失败: {exc}")
        return JSONResponse({"error": {"message": f"请求解析失败: {exc}"}}, status_code=400)

    model = chat_req.get("model", "")
    is_streaming = chat_req.get("stream", False) is True
    if "stream" not in req_body:
        is_streaming = True
        chat_req["stream"] = True

    chat_body_text = apply_model_mapping(json.dumps(chat_req, ensure_ascii=False))
    # 注入 OpenClaw 必需的系统提示词
    chat_body_text = _inject_openclaw_system_prompt(chat_body_text)
    # 注入 stream_options 确保上游返回 usage（token 用量统计所需）
    if is_streaming:
        chat_body_text = _inject_stream_options(chat_body_text)

    # 🔍 调试日志
    logger.warning(f"🔍 [/v1/responses] 原始 Responses 请求 - model: {req_body.get('model')}, has_instructions: {bool(req_body.get('instructions'))}")
    logger.warning(f"🔍 [/v1/responses] 转换后的 OpenAI 格式请求体: {chat_body_text}")

    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)
        
    retry_state = RetryState()
    route_key = "/v1/responses"
    request_started_at = time.monotonic()
    api_key_id = getattr(request.state, "api_key_id", None)
    record_request_started(route_key, is_streaming=is_streaming, api_key_id=api_key_id)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method="POST", path="/v1/chat/completions", body=chat_body_text, log_label="Responses 映射请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()

            if status_code >= 400:
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                raw_body = await collect_response_body(req_id, queue)
                logger.error(f"❌ [/v1/responses] 上游返回错误 - status_code: {status_code}, body: {raw_body[:1000]}")
                record_error("/v1/responses", status_code, f"上游返回 {status_code}", detail=raw_body[:500])
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            if is_streaming:
                converter = ResponsesStreamConverter(model=model)

                async def responses_stream_generator(current_req_id, current_queue):
                    last_data_time = time.monotonic()
                    stream_succeeded = False
                    data_task = asyncio.ensure_future(current_queue.get())

                    async def _do_keepalive():
                        await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                        return b": keep-alive\n\n"
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                    try:
                        while True:
                            done, _ = await asyncio.wait({data_task, keepalive_task}, return_when=asyncio.FIRST_COMPLETED)

                            if keepalive_task in done:
                                elapsed = time.monotonic() - last_data_time
                                if elapsed > STREAM_CHUNK_TIMEOUT:
                                    logger.warning(f"⚠️ Responses 流式 {elapsed:.0f}s 无数据，节点可能已断开 [{current_req_id[:8]}]")
                                    break
                                yield keepalive_task.result()
                                keepalive_task = asyncio.ensure_future(_do_keepalive())
                                continue

                            last_data_time = time.monotonic()
                            data_task = asyncio.ensure_future(current_queue.get())
                            msg = done.pop().result()
                            if msg.get("type") == "finish":
                                stream_succeeded = True
                                for evt in converter.finalize():
                                    yield evt.encode("utf-8")
                                break
                            elif msg.get("type") == "error":
                                err_evt = f"event: error\ndata: {json.dumps({'type': 'error', 'message': msg.get('body')})}\n\n"
                                yield err_evt.encode("utf-8")
                                break
                            elif msg.get("type") == "chunk":
                                for line in msg.get("body", "").split("\n"):
                                    for evt in converter.process_chunk(line):
                                        yield evt.encode("utf-8")
                    finally:
                        data_task.cancel()
                        keepalive_task.cancel()
                        await asyncio.gather(data_task, keepalive_task, return_exceptions=True)
                        cleanup_pending_request(current_req_id)
                        usage_obj = getattr(converter, "_usage", None)
                        record_request_finished(route_key=route_key, status_code=status_code if stream_succeeded else 502, started_at=request_started_at, first_byte_at=first_byte_at, success=stream_succeeded, usage=usage_obj.model_dump() if usage_obj else None, api_key_id=api_key_id)

                return StreamingResponse(
                    responses_stream_generator(req_id, queue),
                    status_code=status_code,
                    media_type="text/event-stream",
                    headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
                )
            else:
                raw_body = await collect_response_body(req_id, queue)
                try:
                    chat_resp = json.loads(raw_body)
                except json.JSONDecodeError:
                    record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                    return JSONResponse({"error": {"message": "上游返回了非法 JSON"}}, status_code=502)

                responses_resp = responses_convert_response(chat_resp)
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=True, usage=chat_resp.get("usage"), api_key_id=api_key_id)
                return JSONResponse(content=responses_resp)

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False, api_key_id=api_key_id)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

_MODELS = [
    ("mimo-v2.5-pro", "MiMo V2.5 Pro", 1048576, 131072),
    ("mimo-v2.5", "MiMo V2.5", 1048576, 131072),
    ("mimo-v2.5-tts", "MiMo V2.5 TTS", 8192, 8192),
    ("mimo-v2-pro", "MiMo V2 Pro", 1048576, 131072),
    ("mimo-v2-flash", "MiMo V2 Flash", 256000, 131072),
    ("mimo-v2-omni", "MiMo V2 Omni", 256000, 131072),
    ("mimo-v2.5-tts-voicedesign", "MiMo V2.5 TTS VoiceDesign", 8192, 8192),
    ("mimo-v2.5-tts-voiceclone", "MiMo V2.5 TTS VoiceClone", 8192, 8192),
    ("mimo-v2-tts", "MiMo V2 TTS", 8192, 8192),
]


@app.get("/v1/models")
async def get_models(request: Request):
    """自动识别客户端类型：Anthropic SDK 带 x-api-key + anthropic-version 头时返回 Anthropic 格式，否则返回 OpenAI 格式。"""
    # 参考 new-api: 通过请求头自动判断客户端类型
    if request.headers.get("x-api-key") and request.headers.get("anthropic-version"):
        return _models_anthropic_format()
    return _models_openai_format()

@app.get("/anthropic/v1/models")
async def get_anthropic_models():
    return _models_anthropic_format()

def _models_openai_format():
    data = [{"id": m[0], "object": "model", "created": 1700000000, "owned_by": "mimo", "context_length": m[2], "max_tokens": m[2]} for m in _MODELS]
    return JSONResponse(content={"object": "list", "data": data})

def _models_anthropic_format():
    data = [
        {
            "id": model_id,
            "display_name": display_name,
            "created_at": "2025-01-01T00:00:00Z",
            "type": "model",
            "max_input_tokens": context_length,
            "max_tokens": max_output_tokens,
        }
        for model_id, display_name, context_length, max_output_tokens in _MODELS
    ]
    return JSONResponse(content={"data": data, "has_more": False, "first_id": data[0]["id"], "last_id": data[-1]["id"]})

@app.post("/v1/chat/completions")
async def chat_completions_handler(request: Request):
    return await _forward_request(request, "/v1/chat/completions")

@app.post("/v1/messages")
async def v1_messages_handler(request: Request):
    """兼容 Anthropic SDK / Claude Code —— 直接走 /v1/messages，无需 /anthropic 前缀。"""
    return await anthropic_messages_handler(request)

@app.post("/anthropic/v1/messages")
async def anthropic_messages_handler(request: Request):
    """Anthropic Messages API → 转为 OpenAI 格式走 /v1/chat/completions → 响应转回 Anthropic 格式。"""
    if not state.active_clients:
        return JSONResponse({"type": "error", "error": {"type": "overloaded_error", "message": "没有可用的内网节点"}}, status_code=529)

    # ── 1. 读取并转换请求 ──
    body = await request.body()
    try:
        req_body = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
        chat_req = convert_anthropic_request(req_body)
    except Exception as exc:
        return JSONResponse({"type": "error", "error": {"type": "invalid_request_error", "message": str(exc)}}, status_code=400)

    model = req_body.get("model", "")
    is_streaming = req_body.get("stream", False) is True
    # 强制 stream=true 给上游（跟 Responses 端点一样，统一用流式拿数据再转）
    chat_req["stream"] = True

    # ── 2. 应用模型映射，序列化为 body_text ──
    body_text = apply_model_mapping(json.dumps(chat_req, ensure_ascii=False))
    # 注入 OpenClaw 必需的系统提示词
    body_text = _inject_openclaw_system_prompt(body_text)
    # 注入 stream_options 确保上游返回 usage（token 用量统计所需）
    body_text = _inject_stream_options(body_text)

    # 🔍 调试日志
    logger.warning(f"🔍 [/v1/messages] 原始 Anthropic 请求 - model: {req_body.get('model')}, has_system: {bool(req_body.get('system'))}, messages_count: {len(req_body.get('messages', []))}")
    logger.warning(f"🔍 [/v1/messages] 转换后的 OpenAI 格式请求体: {body_text}")

    # ── 3. 用跟 _forward_request 完全一样的路径转发到 bridge ──
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return JSONResponse({"type": "error", "error": {"type": "overloaded_error", "message": "没有可用的内网节点"}}, status_code=529)

    retry_state = RetryState()
    route_key = "/v1/messages"
    request_started_at = time.monotonic()
    api_key_id = getattr(request.state, "api_key_id", None)
    record_request_started(route_key, is_streaming=True, api_key_id=api_key_id)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(
                method="POST", path="/v1/chat/completions", body=body_text,
                log_label="Anthropic→Chat", retry_state=retry_state, attempt_number=attempt + 1,
            )
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()

            # 上游返回错误，直接透传
            if status_code >= 400:
                raw_body = await collect_response_body(req_id, queue)
                logger.error(f"❌ [/v1/messages] 上游返回错误 - status_code: {status_code}, body: {raw_body[:1000]}")
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=False, api_key_id=api_key_id)
                return JSONResponse({"type": "error", "error": {"type": "api_error", "message": raw_body[:1000]}}, status_code=status_code)

            # ── 4. 流式：收集 OpenAI SSE → 转为 Anthropic SSE ──
            if is_streaming:
                converter = AnthropicStreamConverter(model=model)

                async def _anthropic_sse(cur_req_id, cur_queue):
                    stream_ok = False
                    usage_data = None
                    data_task = asyncio.ensure_future(cur_queue.get())
                    keepalive_task = None

                    async def _do_keepalive():
                        await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                        return b": keep-alive\n\n"
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                    try:
                        while True:
                            done, _ = await asyncio.wait({data_task, keepalive_task}, return_when=asyncio.FIRST_COMPLETED)

                            if keepalive_task in done:
                                elapsed = time.monotonic() - last_data_time_ref[0]
                                if elapsed > STREAM_CHUNK_TIMEOUT:
                                    break
                                yield keepalive_task.result()
                                keepalive_task = asyncio.ensure_future(_do_keepalive())
                                continue

                            last_data_time_ref[0] = time.monotonic()
                            data_task = asyncio.ensure_future(cur_queue.get())
                            msg = done.pop().result()
                            if msg.get("type") == "finish":
                                stream_ok = True
                                for ev in converter.process_chunk("data: [DONE]"):
                                    yield ev.encode()
                                break
                            elif msg.get("type") == "error":
                                break
                            elif msg.get("type") == "chunk":
                                chunk_body = msg.get("body", "")
                                # 每个 chunk 都尝试提取 usage（取最后出现的有效值）
                                extracted = extract_usage_from_sse_chunk(chunk_body)
                                if extracted is not None:
                                    usage_data = extracted
                                for line in chunk_body.split("\n"):
                                    for ev in converter.process_chunk(line):
                                        yield ev.encode()
                    finally:
                        data_task.cancel()
                        if keepalive_task is not None:
                            keepalive_task.cancel()
                        await asyncio.gather(data_task, keepalive_task, return_exceptions=True)
                        cleanup_pending_request(cur_req_id)
                        record_request_finished(route_key=route_key, status_code=status_code if stream_ok else 502, started_at=request_started_at, first_byte_at=first_byte_at, success=stream_ok, usage=usage_data, api_key_id=api_key_id)

                last_data_time_ref = [time.monotonic()]
                return StreamingResponse(_anthropic_sse(req_id, queue), status_code=200, media_type="text/event-stream", headers={"cache-control": "no-cache", "x-accel-buffering": "no"})

            # ── 5. 非流式：收集完整响应 → 转为 Anthropic JSON ──
            else:
                # 仍然用流式从上游拿数据，只是在此处拼成完整 JSON 再转换
                full_text = ""
                tool_calls_data = []
                finish_reason = "stop"
                usage_data = None
                while True:
                    msg = await queue.get()
                    if msg.get("type") == "finish":
                        break
                    elif msg.get("type") == "error":
                        cleanup_pending_request(req_id)
                        return JSONResponse({"type": "error", "error": {"type": "api_error", "message": msg.get("body", "")}}, status_code=502)
                    elif msg.get("type") == "chunk":
                        chunk_body = msg.get("body", "")
                        # 每个 chunk 都尝试提取 usage
                        extracted = extract_usage_from_sse_chunk(chunk_body)
                        if extracted is not None:
                            usage_data = extracted
                        for line in chunk_body.split("\n"):
                            line = line.strip()
                            if not line.startswith("data:"):
                                continue
                            data_str = line[5:].strip() if line.startswith("data: ") else line[5:].strip()
                            if data_str == "[DONE]":
                                continue
                            try:
                                chunk = json.loads(data_str)
                                choices = chunk.get("choices", [])
                                if choices:
                                    delta = choices[0].get("delta", {})
                                    if delta.get("content"):
                                        full_text += delta["content"]
                                    if delta.get("tool_calls"):
                                        for tc in delta["tool_calls"]:
                                            idx = tc.get("index", 0)
                                            while len(tool_calls_data) <= idx:
                                                tool_calls_data.append({"id": "", "name": "", "arguments": ""})
                                            if tc.get("id"):
                                                tool_calls_data[idx]["id"] = tc["id"]
                                            if tc.get("function", {}).get("name"):
                                                tool_calls_data[idx]["name"] = tc["function"]["name"]
                                            if tc.get("function", {}).get("arguments"):
                                                tool_calls_data[idx]["arguments"] += tc["function"]["arguments"]
                                    fr = choices[0].get("finish_reason")
                                    if fr:
                                        finish_reason = fr
                            except json.JSONDecodeError:
                                pass

                cleanup_pending_request(req_id)

                # 构建 Anthropic 响应
                content = []
                if full_text:
                    content.append({"type": "text", "text": full_text})
                for tc in tool_calls_data:
                    try:
                        inp = json.loads(tc["arguments"]) if tc["arguments"] else {}
                    except json.JSONDecodeError:
                        inp = {}
                    content.append({"type": "tool_use", "id": tc["id"], "name": tc["name"], "input": inp})
                if not content:
                    content.append({"type": "text", "text": ""})

                stop_map = {"stop": "end_turn", "tool_calls": "tool_use", "length": "max_tokens"}
                anthropic_resp = {
                    "id": f"msg_{uuid.uuid4().hex[:24]}",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": content,
                    "stop_reason": stop_map.get(finish_reason, "end_turn"),
                    "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                }
                record_request_finished(route_key=route_key, status_code=200, started_at=request_started_at, first_byte_at=first_byte_at, success=True, usage=usage_data, api_key_id=api_key_id)
                return JSONResponse(content=anthropic_resp)

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "timeout"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False, api_key_id=api_key_id)
    return JSONResponse({"type": "error", "error": {"type": "api_error", "message": "所有节点请求失败"}}, status_code=502)

async def _forward_request(request: Request, path: str):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    method = request.method
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    retry_state = RetryState()
    body_text = body.decode("utf-8", "ignore").lstrip("\ufeff")
    body_text = apply_model_mapping(body_text)
    # \u6ce8\u5165 OpenClaw \u5fc5\u9700\u7684\u7cfb\u7edf\u63d0\u793a\u8bcd
    body_text = _inject_openclaw_system_prompt(body_text)
    route_key = path
    request_started_at = time.monotonic()
    api_key_id = getattr(request.state, "api_key_id", None)

    is_streaming = False
    try:
        is_streaming = json.loads(body_text).get("stream", False) is True
    except (json.JSONDecodeError, AttributeError):
        pass
    # 自动注入 stream_options 确保上游返回 usage（token 用量统计所需）
    if is_streaming:
        body_text = _inject_stream_options(body_text)
    record_request_started(route_key, is_streaming=is_streaming, api_key_id=api_key_id)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method=method, path=path, body=body_text, log_label="转发请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            logger.debug(f"📩 [{req_id[:8]}] 响应 status={status_code}, path={path}")
            first_byte_at = time.monotonic()
            content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))

            async def stream_generator(current_req_id, current_queue, use_keepalive):
                last_data_time = time.monotonic()
                data_task = asyncio.ensure_future(current_queue.get())
                keepalive_task = None
                stream_succeeded = False
                usage_data = None

                async def _do_keepalive():
                    await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                    return b": keep-alive\n\n"
                if use_keepalive:
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                try:
                    while True:
                        pending = {data_task}
                        if keepalive_task is not None:
                            pending.add(keepalive_task)
                        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                        if keepalive_task is not None and keepalive_task in done:
                            elapsed = time.monotonic() - last_data_time
                            if elapsed > STREAM_CHUNK_TIMEOUT:
                                logger.warning(f"⚠️ 流式 {elapsed:.0f}s 无数据，节点可能已断开 [{current_req_id[:8]}]")
                                break
                            yield keepalive_task.result()
                            keepalive_task = asyncio.ensure_future(_do_keepalive())
                            continue

                        last_data_time = time.monotonic()
                        data_task = asyncio.ensure_future(current_queue.get())
                        msg = done.pop().result()
                        if msg.get("type") == "finish":
                            stream_succeeded = True
                            break
                        elif msg.get("type") == "chunk":
                            chunk_body = msg.get("body", "")
                            if usage_data is None:
                                usage_data = extract_usage_from_sse_chunk(chunk_body)
                            yield chunk_body.encode("utf-8")
                finally:
                    data_task.cancel()
                    if keepalive_task is not None:
                        keepalive_task.cancel()
                    await asyncio.gather(*[t for t in (data_task, keepalive_task) if t is not None], return_exceptions=True)
                    cleanup_pending_request(current_req_id)
                    record_request_finished(route_key=route_key, status_code=status_code if stream_succeeded else 502, started_at=request_started_at, first_byte_at=first_byte_at, success=stream_succeeded and status_code < 400, usage=usage_data, api_key_id=api_key_id)

            if status_code >= 400:
                # 非流式错误响应：读取第一个 chunk 打印出来方便调试
                error_body_parts = []
                try:
                    while not queue.empty():
                        msg = queue.get_nowait()
                        if msg.get("type") == "chunk":
                            error_body_parts.append(msg.get("body", ""))
                        elif msg.get("type") in ("finish", "error"):
                            break
                except Exception:
                    pass
                error_body = "".join(error_body_parts)
                logger.warning(f"❌ [{req_id[:8]}] 上游返回 {status_code}: {error_body[:500]}")
                record_error(route_key, status_code, f"上游返回 {status_code}", detail=(error_body or first_msg.get("body", ""))[:300])

            return StreamingResponse(stream_generator(req_id, queue, use_keepalive=is_streaming), status_code=status_code, media_type=content_type, headers=response_headers)

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求所有节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False, api_key_id=api_key_id)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    uvicorn.run(app, host="0.0.0.0", port=23655, ws_max_size=10**8)
