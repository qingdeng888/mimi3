# 健康检查功能实现总结

## 📋 需求回顾

**目标**：为 mimi3 项目增加 API 响应检测机制

**核心需求**：
1. ✅ 定时（每 5 分钟）内网 curl 测试模型响应
2. ✅ Bridge 上线后 15 秒快速检测
3. ✅ 使用 `.env` 的 API Key
4. ✅ 测试模型：`mimo-v2.5-pro`
5. ✅ 测试消息：`"你是谁"`
6. ✅ 无响应时重启 bridge（获取创建锁防止误创建）
7. ✅ 重新上线后释放创建锁

## 📦 实现清单

### 1. 新增文件

#### `/mimo2api/health_checker.py` - 核心健康检查模块
- **`HealthChecker` 类**：健康检查器主类
  - `check_api_health()`: 执行 API 健康检查
  - `restart_bridge_for_account()`: 重启指定账号的 bridge
  - `monitor_account()`: 单个账号的健康监控守护线程

- **全局函数**：
  - `start_health_check_for_account(uid)`: 启动账号健康检查
  - `stop_health_check_for_account(uid)`: 停止账号健康检查

- **配置常量**：
  ```python
  HEALTH_CHECK_INTERVAL = 300        # 5 分钟定时检测
  HEALTH_CHECK_QUICK_DELAY = 15      # Bridge 上线后快速检测
  HEALTH_CHECK_TIMEOUT = 30          # 检测超时
  HEALTH_CHECK_MODEL = "mimo-v2.5-pro"
  HEALTH_CHECK_MESSAGE = "你是谁"
  ```

#### `test_health_check.py` - 测试脚本
- 验证健康检查功能
- 检查环境变量配置
- 执行单次检测测试

#### `HEALTH_CHECK.md` - 详细文档
- 功能说明
- 配置方法
- 工作流程图
- 常见问题 FAQ

### 2. 修改文件

#### `/mimo2api/manager.py`
**修改点 1**: `AccountManager.__init__`
```python
# 新增重建事件（用于健康检查触发重启）
self._rebuild_event = asyncio.Event()
```

**修改点 2**: 节点上线后启动健康检查
```python
if node_online:
    self.logger.info(f"✅ 账号 {self.uid} 节点已上线，释放全局创建锁。")
    # 启动健康检查守护线程
    from .health_checker import start_health_check_for_account
    start_health_check_for_account(self.uid)
```

**修改点 3**: 等待容器过期时监听重建事件
```python
# 等待容器自然过期或重建事件触发
try:
    await asyncio.wait_for(self._rebuild_event.wait(), timeout=wait_time)
    # 重建事件触发，清除事件并重新进入创建流程
    self._rebuild_event.clear()
    self.logger.warning("🔄 收到重建事件信号（健康检查失败），立即重新进入创建流程...")
    continue
except asyncio.TimeoutError:
    # 正常超时，容器自然过期
    self.logger.info("⏰ 等待时间结束，容器应已自然过期...")
```

**修改点 4**: 账号删除/禁用时停止健康检查
```python
# 停止健康检查
from .health_checker import stop_health_check_for_account
stop_health_check_for_account(uid)
```

**修改点 5**: 启动时为已有节点启动健康检查
```python
# 延迟 30 秒后启动健康检查（给账号足够的启动时间）
logger.info("⏳ 将在 30 秒后为已有节点启动健康检查...")
await asyncio.sleep(30)
# ... 为在线节点启动健康检查
```

#### `/env.example`
- 更新 `MIMO_RELAY_OPENAI_KEY` 注释，说明健康检查也使用此密钥

#### `/README.md`
- 在"整体架构"中添加健康检查说明
- 在"日志与排错"中添加健康检查详细说明

## 🔄 工作流程

### 正常流程
```
Bridge 上线
  ↓
启动健康检查守护线程
  ↓
等待 15 秒（快速检测延迟）
  ↓
执行首次健康检查 ←───────┐
  ↓                      │
检测成功                  │
  ↓                      │
等待 5 分钟 ──────────────┘
```

### 失败重启流程
```
健康检查失败
  ↓
累计失败次数 +1
  ↓
失败次数 ≥ 2？
  ├─ 否 → 等待 5 分钟 → 继续检测
  └─ 是 ↓
      断开旧 WebSocket 连接
        ↓
      获取全局创建锁 🔒
        ↓
      触发重建事件（_rebuild_event.set()）
        ↓
      AccountManager 收到事件信号
        ↓
      清除事件标志，中断等待
        ↓
      重新进入创建流程
        ↓
      等待节点重新上线（最多 10 分钟）
        ↓
      释放全局创建锁 🔓
        ↓
      重置失败计数
        ↓
      等待 15 秒后再次快速检测
```

## 🛡️ 安全机制

### 1. 全局创建锁
- 重启 bridge 时获取 `_claw_creation_lock`
- 防止多个账号同时创建容器
- 确保系统资源不被过度占用

### 2. 超时保护
- 健康检查超时：30 秒
- 节点上线等待：10 分钟
- 避免无限期等待导致资源泄漏

### 3. 失败阈值
- 连续 2 次失败才触发重启
- 避免偶尔的网络抖动导致误重启
- 减少不必要的资源消耗

### 4. 任务隔离
- 每个账号独立的健康检查任务
- 单个账号的问题不影响其他账号
- 账号删除/禁用时自动停止对应任务

## 📊 资源消耗估算

### Token 消耗
- 单次检测：约 100 tokens（请求 + 响应）
- 检测频率：5 分钟/次
- 日消耗：100 × 12 × 24 = 28,800 tokens/账号/天
- 10 个账号：约 288,000 tokens/天

### 网络消耗
- 单次请求：约 1 KB
- 单次响应：约 0.5 KB
- 日消耗：1.5 KB × 288 = 432 KB/账号/天

### CPU/内存
- 每个健康检查任务：独立协程，几乎不占用额外资源
- 检测期间：短暂的 HTTP 请求，开销极小

## 🧪 测试方法

### 1. 单元测试
```bash
python test_health_check.py
```

### 2. 集成测试
```bash
# 启动服务
python main.py

# 观察日志中的健康检查信息
# 应该看到类似：
# [账号 xxx] 🏥 健康检查守护线程已启动
# [账号 xxx] ✅ API 健康检查通过 (耗时 1.23s, ...)
```

### 3. 故障模拟
```bash
# 方法 1: 手动停止所有节点，观察是否自动重启
# 方法 2: 修改测试模型为不存在的模型，观察失败处理
# 方法 3: 暂停 gateway，观察超时处理
```

## 📝 使用说明

### 1. 配置
在 `.env` 文件中设置：
```bash
MIMO_RELAY_OPENAI_KEY=sk-your-random-secret-here
SERVER_HOST=127.0.0.1
SERVER_PORT=23655
```

### 2. 启动
```bash
python main.py
```

健康检查会自动启动，无需额外操作。

### 3. 日志监控
观察日志中的健康检查信息：
```
[账号 xxx] 🏥 健康检查守护线程已启动
[账号 xxx] ✅ API 健康检查通过
[账号 xxx] ❌ API 健康检查失败
[账号 xxx] 🔄 API 无响应，准备重启 bridge...
```

## ✅ 完成状态

所有需求已实现并测试通过：

- ✅ 定时检测（每 5 分钟）
- ✅ 快速检测（上线后 15 秒）
- ✅ 使用 `.env` 的 API Key
- ✅ 测试指定模型和消息
- ✅ 无响应时自动重启 bridge
- ✅ 获取/释放创建锁机制
- ✅ 完整的日志记录
- ✅ 详细的文档说明
- ✅ 测试脚本

## 🚀 后续优化建议

1. **可配置的检测参数**
   - 通过环境变量配置检测间隔
   - 可调整失败阈值
   - 可配置超时时间

2. **WebUI 集成**
   - 在控制面板显示健康检查状态
   - 手动触发健康检查按钮
   - 查看历史检测记录

3. **告警机制**
   - 健康检查失败时发送邮件/Webhook
   - 连续多次失败自动禁用账号
   - 导出健康检查报告

4. **更丰富的检测指标**
   - 响应时间趋势
   - 成功率统计
   - 节点健康评分

---

**开发完成时间**：2026-07-10
**版本**：v1.0
**状态**：✅ 已完成并可投入使用
