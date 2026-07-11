#!/usr/bin/env python3
"""
API 健康检查模块

功能：
1. 定时检测 API 响应状态（每 3 分钟）
2. Bridge 上线后 15 秒快速检测
3. 轻量级重启（第1～9次连续失败）：通过 WebSocket 让 miclaw 重启 bridge
4. 重度重启（连续10次失败）：重置 miclaw + 重新注入
5. 使用 .env 配置的 API Key 和固定测试模型
"""

import asyncio
import logging
import os
import time
from typing import Dict, Optional
import httpx

logger = logging.getLogger("HealthChecker")

# 健康检查配置
HEALTH_CHECK_INTERVAL = 180  # 3 分钟
HEALTH_CHECK_QUICK_DELAY = 15  # Bridge 上线后 15 秒快速检测
HEALTH_CHECK_TIMEOUT = 30  # 检测超时时间（秒）
HEALTH_CHECK_MODEL = "mimo-v2.5-pro"  # 测试模型
HEALTH_CHECK_MESSAGE = "你是谁"  # 测试消息

# 失败阈值
LIGHT_RESTART_THRESHOLD = 1  # 每次失败都触发轻量级重启
HEAVY_RESTART_THRESHOLD = 10  # 连续10次失败触发重度重启

# 从环境变量读取配置
SERVER_HOST = os.getenv("SERVER_HOST", "127.0.0.1")
SERVER_PORT = os.getenv("SERVER_PORT", "23655")
API_KEY = os.getenv("MIMO_RELAY_OPENAI_KEY", "")

# 本地 API 基础 URL
LOCAL_API_BASE = f"http://{SERVER_HOST}:{SERVER_PORT}"


class HealthChecker:
    """API 健康检查器"""

    def __init__(self):
        self.last_check_time: Dict[str, float] = {}  # uid -> 上次检查时间
        self.checking: Dict[str, bool] = {}  # uid -> 是否正在检查
        self.failed_counts: Dict[str, int] = {}  # uid -> 连续失败次数
        # 健康检查日志（最近 100 条）
        self.check_logs: list[Dict] = []
        self.max_logs = 100

    def _add_log(self, uid: str, status: str, message: str, elapsed: float = 0):
        """添加健康检查日志"""
        from datetime import datetime
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "uid": uid,
            "status": status,  # "success", "failed", "restart_light", "restart_heavy"
            "message": message,
            "elapsed": round(elapsed, 2) if elapsed else 0
        }
        self.check_logs.insert(0, log_entry)  # 最新的在前面
        if len(self.check_logs) > self.max_logs:
            self.check_logs = self.check_logs[:self.max_logs]  # 保留最近的

    def clear_logs(self) -> int:
        """清空 WebUI 展示的健康检查日志，并返回清理条数。"""
        cleared_count = len(self.check_logs)
        self.check_logs.clear()
        logger.info(f"🧹 已清空 {cleared_count} 条健康检查展示日志")
        return cleared_count

    async def check_api_health(self, uid: Optional[str] = None) -> bool:
        """检查 API 健康状态

        Args:
            uid: 账号 ID（用于日志标识），None 表示全局检查

        Returns:
            True 表示健康，False 表示不健康
        """
        label = f"[账号 {uid}]" if uid else "[全局]"

        if not API_KEY:
            logger.warning(f"{label} 未配置 MIMO_RELAY_OPENAI_KEY，跳过健康检查")
            return True

        try:
            logger.info(f"{label} 开始 API 健康检查...")

            async with httpx.AsyncClient(timeout=HEALTH_CHECK_TIMEOUT) as client:
                headers = {
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json",
                }

                payload = {
                    "model": HEALTH_CHECK_MODEL,
                    "messages": [{"role": "user", "content": HEALTH_CHECK_MESSAGE}],
                    "stream": False,
                    "max_tokens": 100,
                }

                start_time = time.time()
                response = await client.post(
                    f"{LOCAL_API_BASE}/v1/chat/completions",
                    json=payload,
                    headers=headers,
                )
                elapsed = time.time() - start_time

                if response.status_code == 200:
                    data = response.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content") or ""
                    reasoning = data.get("choices", [{}])[0].get("message", {}).get("reasoning_content") or ""
                    total_content = content + reasoning
                    logger.info(
                        f"{label} ✅ API 健康检查通过 "
                        f"(耗时 {elapsed:.2f}s, 响应长度 {len(total_content)} 字符)"
                    )
                    self._add_log(uid or "global", "success",
                                  f"健康检查通过 (响应长度 {len(total_content)} 字符)", elapsed)
                    logger.debug(f"[DEBUG] 健康检查日志已添加，当前日志数量: {len(self.check_logs)}")
                    return True
                else:
                    logger.error(
                        f"{label} ❌ API 健康检查失败: HTTP {response.status_code}, "
                        f"响应: {response.text[:200]}"
                    )
                    self._add_log(uid or "global", "failed",
                                  f"HTTP {response.status_code}: {response.text[:100]}", elapsed)
                    return False

        except httpx.TimeoutException:
            logger.error(f"{label} ❌ API 健康检查超时（>{HEALTH_CHECK_TIMEOUT}s）")
            self._add_log(uid or "global", "failed", f"超时（>{HEALTH_CHECK_TIMEOUT}s）", 0)
            return False
        except Exception as e:
            logger.error(f"{label} ❌ API 健康检查异常: {e}", exc_info=True)
            self._add_log(uid or "global", "failed", f"异常: {str(e)[:100]}", 0)
            return False

    async def restart_bridge_light(self, uid: str) -> bool:
        """轻量级重启：通过 WebSocket 让 miclaw 重启 bridge（不重新注入）

        Args:
            uid: 账号 ID

        Returns:
            True 表示重启成功，False 表示失败
        """
        from .manager import _account_managers, _claw_creation_lock, NativeClawClient

        logger.warning(f"[账号 {uid}] 🔄 健康检查连续失败，执行轻量级重启（仅重启 bridge）...")
        self._add_log(uid, "restart_light", "开始轻量级重启（仅重启 bridge）", 0)

        # 获取 AccountManager 实例
        manager = _account_managers.get(str(uid))
        if not manager:
            logger.error(f"[账号 {uid}] ❌ 找不到对应的 AccountManager，无法重启")
            return False

        from .gateway_state import state as gw_state
        previous_ws_ids = {
            ws_id
            for ws_id, mapped_uid in gw_state.client_uid_map.items()
            if mapped_uid == uid
        }

        # 获取全局创建锁（防止其他账号同时创建）
        logger.info(f"[账号 {uid}] 🔒 等待获取全局创建锁...")
        async with _claw_creation_lock:
            logger.info(f"[账号 {uid}] ✅ 已获取全局创建锁，开始轻量级重启")

            try:
                # 连接 miclaw 容器
                client = NativeClawClient(manager.ph, manager.cookies, manager.logger)

                # 尝试连接（不创建新实例）
                logger.info(f"[账号 {uid}] 正在连接 miclaw 容器...")
                if not await manager.connect_with_retry(client, max_retries=3, create=False):
                    logger.error(f"[账号 {uid}] ❌ 连接 miclaw 容器失败")
                    await client.close()
                    return False

                # 发送重启 bridge 指令
                restart_cmd = "重启之前运行的bridge"
                logger.info(f"[账号 {uid}] 下发重启 bridge 指令...")
                reply = await client.send_message(restart_cmd, timeout=60)
                logger.info(f"[账号 {uid}] 重启反馈: {reply[:200] if reply else '(无响应)'}")

                await client.close()

                # 等待节点重新上线（最多等待 3 分钟）
                logger.info(f"[账号 {uid}] 等待节点重新上线（最多 3 分钟）...")
                from .manager import _wait_for_node_reconnected
                node_online = await _wait_for_node_reconnected(
                    uid,
                    previous_ws_ids=previous_ws_ids,
                    timeout=180,
                )

                if node_online:
                    logger.info(f"[账号 {uid}] ✅ 轻量级重启成功，节点已重新上线")
                    self._add_log(uid, "success", "轻量级重启成功，节点已重新上线", 0)
                    return True
                else:
                    logger.error(f"[账号 {uid}] ❌ 轻量级重启失败，节点未在 3 分钟内上线")
                    self._add_log(uid, "failed", "轻量级重启失败，节点未在 3 分钟内上线", 0)
                    return False

            except Exception as e:
                logger.error(f"[账号 {uid}] ❌ 轻量级重启过程异常: {e}", exc_info=True)
                self._add_log(uid, "failed", f"轻量级重启异常: {str(e)[:100]}", 0)
                return False

        # 创建锁自动释放

    async def restart_bridge_heavy(self, uid: str) -> bool:
        """重度重启：重置 miclaw + 重新注入（适用于轻量级重启失败的情况）

        Args:
            uid: 账号 ID

        Returns:
            True 表示重启成功，False 表示失败
        """
        from .manager import _account_managers

        logger.error(f"[账号 {uid}] 💥 连续 {HEAVY_RESTART_THRESHOLD} 次健康检查失败，执行重度重启（重置 miclaw + 重新注入）...")
        self._add_log(uid, "restart_heavy", "开始重度重启（重置 miclaw + 重新注入）", 0)

        # 获取 AccountManager 实例
        manager = _account_managers.get(str(uid))
        if not manager:
            logger.error(f"[账号 {uid}] ❌ 找不到对应的 AccountManager，无法重启")
            return False

        try:
            # 调用 AccountManager 的 reset_and_reinject 方法（会自动获取创建锁）
            success, message = await manager.reset_and_reinject()

            if success:
                logger.info(f"[账号 {uid}] ✅ 重度重启成功: {message}")
                self._add_log(uid, "success", f"重度重启成功: {message}", 0)
                return True
            else:
                logger.error(f"[账号 {uid}] ❌ 重度重启失败: {message}")
                self._add_log(uid, "failed", f"重度重启失败: {message}", 0)
                return False

        except Exception as e:
            logger.error(f"[账号 {uid}] ❌ 重度重启过程异常: {e}", exc_info=True)
            self._add_log(uid, "failed", f"重度重启异常: {str(e)[:100]}", 0)
            return False

    async def monitor_account(self, uid: str):
        """监控单个账号的健康状态

        Args:
            uid: 账号 ID
        """
        from .gateway_state import state as gw_state

        logger.info(f"[账号 {uid}] 🏥 健康检查守护线程已启动")

        # 等待节点首次上线
        while True:
            # 检查该 uid 是否已上线
            node_found = False
            for ws_id, mapped_uid in gw_state.client_uid_map.items():
                if mapped_uid == uid:
                    node_found = True
                    break

            if node_found:
                logger.info(f"[账号 {uid}] 检测到节点已上线，{HEALTH_CHECK_QUICK_DELAY}秒后进行首次快速检查")
                await asyncio.sleep(HEALTH_CHECK_QUICK_DELAY)
                break

            await asyncio.sleep(5)

        # 进入定时检查循环
        while True:
            try:
                # 检查该账号的节点是否仍在线
                node_online = False
                for ws_id, mapped_uid in gw_state.client_uid_map.items():
                    if mapped_uid == uid:
                        node_online = True
                        break

                if not node_online:
                    logger.debug(f"[账号 {uid}] 节点已离线，暂停健康检查")
                    # 节点离线，重置状态并等待重新上线
                    self.failed_counts[uid] = 0
                    await asyncio.sleep(30)
                    continue

                # 避免重复检查
                if self.checking.get(uid, False):
                    logger.debug(f"[账号 {uid}] 上次检查仍在进行中，跳过本次")
                    await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                    continue

                self.checking[uid] = True

                # 执行健康检查
                is_healthy = await self.check_api_health(uid)

                if is_healthy:
                    # 健康，重置失败计数
                    self.failed_counts[uid] = 0
                    self.last_check_time[uid] = time.time()
                else:
                    # 不健康，累加失败计数
                    self.failed_counts[uid] = self.failed_counts.get(uid, 0) + 1
                    logger.warning(
                        f"[账号 {uid}] 健康检查失败 "
                        f"(连续 {self.failed_counts[uid]} 次)"
                    )

                    # 连续失败达到重度阈值，才重置 miclaw 并重新注入
                    if self.failed_counts[uid] >= HEAVY_RESTART_THRESHOLD:
                        logger.error(
                            f"[账号 {uid}] 连续 {self.failed_counts[uid]} 次健康检查失败，"
                            f"触发重度重启（重置 miclaw + 重新注入）"
                        )
                        restart_success = await self.restart_bridge_heavy(uid)

                        if restart_success:
                            # 已完成重置和重新注入，从新的健康检查周期开始计数
                            logger.info(f"[账号 {uid}] 重度重启成功，重置失败计数")
                            self.failed_counts[uid] = 0
                            self.checking[uid] = False
                            await asyncio.sleep(HEALTH_CHECK_QUICK_DELAY)
                            continue
                        else:
                            # 重度重启失败，等待下一个周期再试
                            logger.error(f"[账号 {uid}] 重度重启失败，将在下个周期重试")

                    # 第 1～9 次连续失败都让 miclaw 自行重启 bridge，不重新注入
                    elif self.failed_counts[uid] >= LIGHT_RESTART_THRESHOLD:
                        logger.warning(
                            f"[账号 {uid}] 连续 {self.failed_counts[uid]} 次健康检查失败，"
                            f"触发轻量级重启（通过 WS 让 miclaw 重启 bridge）"
                        )
                        restart_success = await self.restart_bridge_light(uid)

                        if restart_success:
                            # Bridge 上线不代表 API 已恢复；保留失败次数，15 秒后重新验证
                            logger.info(
                                f"[账号 {uid}] 轻量级重启成功，保留连续失败计数 "
                                f"{self.failed_counts[uid]}，15 秒后重新检查"
                            )
                            self.checking[uid] = False
                            await asyncio.sleep(HEALTH_CHECK_QUICK_DELAY)
                            continue
                        else:
                            # 轻量级重启失败，等待下一个周期再试
                            logger.error(f"[账号 {uid}] 轻量级重启失败，将在下个周期重试")

                self.checking[uid] = False

                # 等待下一个检查周期
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)

            except asyncio.CancelledError:
                # 任务可能在健康检查或重启流程的任意 await 点被替换。
                # 必须释放账号级检查标记，否则新任务会永久认为旧检查仍在执行。
                self.checking[uid] = False
                logger.info(f"[账号 {uid}] 健康检查守护线程已停止")
                break
            except Exception as e:
                logger.error(f"[账号 {uid}] 健康检查守护线程异常: {e}", exc_info=True)
                self.checking[uid] = False
                await asyncio.sleep(60)


# 全局健康检查器实例
_health_checker = HealthChecker()
# 账号健康检查任务注册表：uid -> asyncio.Task
_health_check_tasks: Dict[str, asyncio.Task] = {}


def start_health_check_for_account(uid: str):
    """为指定账号启动健康检查守护线程

    Args:
        uid: 账号 ID
    """
    # 如果已存在该账号的健康检查任务，先取消
    if uid in _health_check_tasks:
        task = _health_check_tasks[uid]
        if not task.done():
            logger.info(f"[账号 {uid}] 已存在健康检查任务，先取消旧任务")
            task.cancel()

    # 新守护任务不应继承已取消任务的运行中状态。
    _health_checker.checking[uid] = False

    # 创建新任务
    task = asyncio.create_task(_health_checker.monitor_account(uid))
    _health_check_tasks[uid] = task
    logger.info(f"[账号 {uid}] 已启动健康检查守护线程")


def stop_health_check_for_account(uid: str):
    """停止指定账号的健康检查守护线程

    Args:
        uid: 账号 ID
    """
    if uid in _health_check_tasks:
        task = _health_check_tasks.pop(uid)
        if not task.done():
            task.cancel()
            logger.info(f"[账号 {uid}] 已停止健康检查守护线程")


async def start_all_health_checks():
    """启动所有账号的健康检查（在 Manager 启动后调用）"""
    from .manager import _account_managers

    logger.info("🏥 正在为所有活跃账号启动健康检查...")

    for uid in _account_managers.keys():
        start_health_check_for_account(uid)

    logger.info(f"🏥 已为 {len(_account_managers)} 个账号启动健康检查")
