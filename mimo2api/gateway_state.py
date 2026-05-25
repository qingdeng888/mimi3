import asyncio
import json
import time
from collections import deque
from typing import Any, Dict, List
from fastapi import WebSocket

METRICS_SNAPSHOT_PATH = None  # 延迟初始化，在 metrics_store 中设置

class GatewayState:
    def __init__(self):
        self.active_clients: List[WebSocket] = []
        self.pending_queues: Dict[str, asyncio.Queue] = {}
        self.ws_to_req_ids: Dict[int, set] = {}  # id(ws) -> {req_id, ...}
        self.req_id_to_ws_id: Dict[str, int] = {}
        self.req_id_timestamps: Dict[str, float] = {}
        self.current_client_index: int = 0
        self.rebuild_event: asyncio.Event = asyncio.Event()
        self.client_cooldowns: Dict[int, float] = {}
        # 节点累计冷却次数：id(ws) -> 计数。每进入一次冷却 +1，
        # 达到 MIMO_NODE_COOLDOWN_REBUILD_THRESHOLD 后判定为坏号并自动触发全局重建。
        # WS 断开 / 重连时随同 client_cooldowns 一并清零。
        self.client_cooldown_counts: Dict[int, int] = {}
        # 节点归属账号：id(ws) -> uid 字符串。
        # bridge.py 在连接 /ws 时通过 ?uid=... 上报；ws_tunnel 接收后写入此表。
        # 用于 cooldown_client 升级时调用 trigger_rebuild_for_uid(uid) 做单账号定向重建，
        # 而不是一只坏号触发全局重建拖死所有账号。老版本 bridge 不带 uid 时此表为空，
        # 升级路径会自动 fallback 到 trigger_rebuild() 全局重建（向后兼容）。
        self.client_uid_map: Dict[int, str] = {}
        # 节点接入时间戳：id(ws) -> 接入 Unix 时间戳，用于 WebUI 展示在线时长
        self.client_connected_at: Dict[int, float] = {}
        # 节点最近心跳时间戳：id(ws) -> 最近一次收到心跳的 Unix 时间戳。
        # bridge.py 每 10s 发送 {"type": "heartbeat"}，gateway 收到后更新此表。
        # 后台 TTL 扫描协程定期检查：超过阈值未收到心跳的节点视为僵尸，主动踢除。
        # 节点首次接入时以 connected_at 作为初始值（视作隐式首次心跳），
        # 避免刚连上但还没来得及发第一个心跳就被误判超时。
        self.client_last_heartbeat: Dict[int, float] = {}
        self.metrics_started_at: float = time.time()
        self.metrics_history_last_snapshot: Dict[str, Any] | None = None
        self.metrics: Dict[str, Any] = self._default_metrics()
        self.recent_errors: deque = deque(maxlen=500)

    @staticmethod
    def _default_metrics() -> Dict[str, Any]:
        return {
            "requests_total": 0,
            "requests_succeeded": 0,
            "requests_failed": 0,
            "streaming_requests": 0,
            "non_streaming_requests": 0,
            "attempts_total": 0,
            "attempts_succeeded": 0,
            "attempts_failed": 0,
            "request_latency_sum_ms": 0.0,
            "request_first_byte_latency_sum_ms": 0.0,
            "request_latency_samples_ms": deque(maxlen=2048),
            "request_first_byte_samples_ms": deque(maxlen=2048),
            "status_codes": {},
            "routes": {},
            "nodes": {},
            "tokens": {
                "requests_with_usage": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
            # 按 AI API Key 维度统计的用量（key_id -> 计数器）
            # key_id："env" / "k_xxx" / "anonymous"（未启用鉴权时的直通流量）
            "keys": {},
        }

state = GatewayState()
