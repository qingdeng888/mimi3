# mimi3 (mimo2api)

> 小米 AI Studio (MiMo / Xiaomi MiMo Claw) 自动化控制网关。
> 把内部的 MiMo 模型，以 **OpenAI / Anthropic 兼容协议**对外暴露，并自带账号池、生命周期管理、负载均衡、Web 控制面板。

---

## 目录

- [项目简介](#项目简介)
- [整体架构](#整体架构)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
- [环境变量配置](#环境变量配置)
- [端口与 WS_TUNNEL_URL 自定义](#端口与-ws_tunnel_url-自定义)
- [账号准备与导入](#账号准备与导入)
- [启动服务](#启动服务)
- [Web 控制面板](#web-控制面板)
- [API 使用指南](#api-使用指南)
  - [OpenAI 兼容接口](#openai-兼容接口)
  - [Anthropic 兼容接口](#anthropic-兼容接口)
  - [TTS 语音合成](#tts-语音合成)
  - [模型映射 (Model Mapping)](#模型映射-model-mapping)
- [管理 / 监控 API](#管理--监控-api)
- [客户端接入示例](#客户端接入示例)
- [部署建议](#部署建议)
- [日志与排错](#日志与排错)
- [常见问题 FAQ](#常见问题-faq)
- [项目结构](#项目结构)
- [免责声明](#免责声明)

---

## 项目简介

`mimi3` 实现了一个把 **小米 AI Studio (xiaomimimo.com)** 内部的 MiMo 模型对外开放为标准 LLM API 的网关：

- **协议兼容**：以 OpenAI `/v1/*` 与 Anthropic `/anthropic/v1/*` 协议对外，业务侧几乎可以无改动接入。
- **账号池**：支持多账号轮询负载均衡 (Round-Robin)，自动跳过失效 / 限流节点。
- **生命周期托管**：每个账号背后的 Claw 云沙箱容器寿命 ≤ 60 分钟，本网关自动错峰销毁、重建、注入桥接脚本，保证后端节点常在线。
- **流式 + Keepalive**：完整支持 `stream=true`，并周期性发送 `: keep-alive` 防止 Cloudflare 等反代连接超时。
- **Web 控制面板**：实时监控节点状态、历史指标、错误日志、模型映射。
- **细粒度鉴权**：API 入口可加 Bearer 鉴权，WebUI 可加用户名/密码登录。

---

## 整体架构

```
              ┌────────────────────────────────────────┐
              │   你的客户端 (OpenAI/Anthropic SDK,      │
              │   Cherry Studio, OneAPI, NextChat...)  │
              └────────────────┬───────────────────────┘
                               │ HTTP(S)
                               ▼
              ┌────────────────────────────────────────┐
              │  公网部署的 mimi3 网关 (FastAPI)         │
              │   - /v1/*  /anthropic/v1/*             │
              │   - /webui (控制面板)                   │
              │   - /ws    (反向 WebSocket 隧道服务端)  │
              └──────────────┬────────────────┬────────┘
                             │ Round-Robin    │
                             ▼                ▼
              ┌──────────────────┐  ┌──────────────────┐
              │  Claw 容器 #1    │  │  Claw 容器 #N    │
              │  (xiaomimimo.com)│  │  (xiaomimimo.com)│
              │  bridge.py 注入  │  │  bridge.py 注入  │
              │  反向连接到 /ws  │  │  反向连接到 /ws  │
              └──────────────────┘  └──────────────────┘
                             │                │
                             └────────┬───────┘
                                      ▼
                       小米 MiMo 上游 API（容器内私网调用）
```

关键流程：

1. **Manager** 启动后扫描 `users/user_*.json`，为每个账号创建 `AccountManager` 协程。
2. 每个 `AccountManager` 通过 `https://aistudio.xiaomimimo.com` 的 open-apis 创建 / 销毁 Claw 容器，并在容器中通过 chat.send 注入 `bridge.py`。
3. `bridge.py` 在容器内以 `nohup` 运行，反向 WebSocket 拨号到 `WS_TUNNEL_URL`（即你的网关 `/ws`）。
4. 当客户端请求到达网关，网关通过路由到某个在线节点的 WebSocket，把 HTTP 请求体转发进容器，由容器内 `bridge.py` 直接调用上游 MiMo API，再把结果（含 SSE 流）流回客户端。
5. 容器寿命接近上限（55 分钟）时，Manager 主动销毁并重建，多账号之间错峰，避免同时空窗。

---

## 前置条件

- **Python**：3.10 及以上（推荐 3.11+）。
- **公网可达地址**：Claw 容器要能反向连接到你的 `/ws`。必须满足以下二选一：
  - 一台拥有公网 IP 的机器（VPS / 云主机），并放行 `SERVER_PORT` 端口。
  - 本地机器 + 内网穿透（frp / cloudflared / ngrok 等），获得一个可被外网访问的 `ws://` 或 `wss://` 地址。
- **小米 AI Studio 账号**：至少 1 个登录后能进入 Claw 沙箱页面的账号，需提取以下三项 Cookie / 身份字段：
  - `userId`
  - `serviceToken`
  - `xiaomichatbot_ph`

> ⚠️ 没有公网可达的 `WS_TUNNEL_URL`，整套系统无法工作（容器侧拨不回来）。

---

## 快速开始

```bash
# 1. 克隆仓库
git clone https://github.com/qingdeng888/mimi3.git
cd mimi3

# 2. 安装依赖
pip install -r requirements.txt

# 3. 复制并填写环境变量
cp env.example .env
# 然后用编辑器打开 .env，至少填写 WS_TUNNEL_URL

# 4. 准备账号（见下文「账号准备与导入」）

# 5. 启动
python main.py
```

启动成功后访问：

- 控制面板：`http://<你的服务器>:18619/webui`
- API 基址：`http://<你的服务器>:18619/v1`（或 `/anthropic/v1`）

---

## 环境变量配置

所有变量都从 `.env` 或系统环境变量读取，示例文件见 `env.example`。

| 变量 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `SERVER_HOST` | 否 | `0.0.0.0` | 网关绑定地址。 |
| `SERVER_PORT` | 否 | `18619` | 网关绑定端口。 |
| `WS_TUNNEL_URL` | **是** | `ws://{HOST}:{PORT}/ws` | Claw 节点反向连接的 WebSocket 地址。**必须是 Claw 容器能访问到的公网/穿透地址**，例如 `ws://your-domain.com:18619/ws` 或 `wss://your-domain.com/ws`。端口可自由自定义，详见 [端口与 WS_TUNNEL_URL 自定义](#端口与-ws_tunnel_url-自定义)。 |
| `MIMO_RELAY_OPENAI_KEY` | 否 | 空 | 客户端调用 `/v1/*`、`/anthropic/v1/*` 时携带的 Bearer Key。**留空 = 不鉴权**。 |
| `MIMO_WEBUI_USERNAME` | 否 | `admin` | WebUI 登录用户名。 |
| `MIMO_WEBUI_PASSWORD` | 否 | 空 | WebUI 登录密码。**留空 = 不启用 WebUI 登录**。 |
| `MIMO_WEBUI_SECRET` | 否 | 自动 | WebUI Session Cookie 签名密钥；缺省时使用密码兜底，建议显式设置一个长随机串。 |
| `MIMO_WEBUI_SESSION_TTL_SECONDS` | 否 | `43200` | WebUI 会话有效期（秒），默认 12 小时，最低 300。 |
| `MIMO_WEBUI_COOKIE_SECURE` | 否 | `false` | 仅在 HTTPS 反代时建议设为 `true`。 |
| `MIMO_WEBUI_COOKIE_NAME` | 否 | `mimo_webui_session` | Cookie 名。 |
| `MIMO_NODE_401_COOLDOWN_SECONDS` | 否 | `900` | 节点返回 401 时的冷却时长。 |
| `MIMO_PROCESS_LOCK_PATH` | 否 | `项目目录/mimo2api.lock` | 单进程锁文件路径，避免重复启动同一份网关。 |

`.env` 模板示例：

```dotenv
SERVER_HOST=0.0.0.0
SERVER_PORT=18619
WS_TUNNEL_URL=ws://your-domain.com:18619/ws

MIMO_RELAY_OPENAI_KEY=sk-your-random-secret-here
MIMO_WEBUI_USERNAME=admin
MIMO_WEBUI_PASSWORD=change-me
MIMO_WEBUI_SECRET=replace-with-a-long-random-string
```

---

## 端口与 WS_TUNNEL_URL 自定义

`SERVER_PORT` 与 `WS_TUNNEL_URL` 的端口**没有任何硬编码限制**，18619 仅是默认值。三个变量之间的关系如下：

```
┌─────────────────────────────┐         ┌──────────────────────────────┐
│  SERVER_HOST + SERVER_PORT  │ ──绑定→ │  网关 (FastAPI / uvicorn)     │
│  (本地监听)                  │         │  对内/反代上游                 │
└─────────────────────────────┘         └──────────────────────────────┘
                                                       ▲
                                                       │ 反向 WebSocket
                                                       │
┌─────────────────────────────┐                        │
│       WS_TUNNEL_URL         │ ──下发到 Claw 容器→ 容器内 bridge 拨回
│  (公网/穿透可达地址)         │
└─────────────────────────────┘
```

**关键事实**：

- `SERVER_PORT` 决定网关在**本机**监听哪个端口；
- `WS_TUNNEL_URL` 决定 Claw 容器应该连**哪个公网地址**，是网关下发给桥接脚本的字符串，端口随意填；
- 两者**只在「裸跑直连」时需要相等**；只要中间有反代/穿透，就以"对外可达地址"为准。

代码里也仅做字符串替换：

```python
# mimo2api/manager.py
code = code.replace("__WS_URL__", ws_url)   # 注入到 Claw 容器
```

```python
# mimo2api/bridge.py（容器内）
async with websockets.connect(WS_URL, max_size=10**8) as ws: ...
```

### 三种典型场景

#### 场景 A：VPS 裸跑（端口 = 直接对外）

```dotenv
SERVER_PORT=18619
WS_TUNNEL_URL=ws://your-domain.com:18619/ws
```

- 网关本机监听 18619；
- Claw 容器直连 `your-domain.com:18619`；
- 防火墙 / 安全组放行 TCP 18619；
- `SERVER_PORT` 与 `WS_TUNNEL_URL` 的端口**必须一致**。

如果想换成别的端口，比如 25000：

```dotenv
SERVER_PORT=25000
WS_TUNNEL_URL=ws://your-domain.com:25000/ws
```

#### 场景 B：Nginx + HTTPS 反代（推荐）

网关本机仍监听 18619，对外用 443（标准 HTTPS）：

```dotenv
SERVER_PORT=18619
WS_TUNNEL_URL=wss://your-domain.com/ws        # 隐含 443，无需写端口
MIMO_WEBUI_COOKIE_SECURE=true
```

Nginx 把 `443` 反代到 `127.0.0.1:18619`（参考下方 [反向代理 Nginx 示例](#反向代理-nginx-示例)）。

> **优点**：80 / 443 是 Claw 沙箱出站方向最稳的端口，不会被云内安全策略拦截；同时还能搞 HTTPS 终止 + WebUI Cookie Secure。

#### 场景 C：内网穿透 (cloudflared / frp / ngrok)

本地机器没有公网 IP 时：

```dotenv
SERVER_PORT=18619                                       # 本地监听
WS_TUNNEL_URL=wss://abc-1234.trycloudflare.com/ws       # 穿透服务给的对外地址
```

- 本地 18619 只对穿透客户端可见即可；
- `WS_TUNNEL_URL` 的端口由穿透服务决定（cloudflared / Caddy 反代默认 443，frp 自行配置），**不必等于 `SERVER_PORT`**。

### 改端口的常见坑

| 现象 | 原因 | 解决 |
| --- | --- | --- |
| 启动后 Claw 一直没接入 | `WS_TUNNEL_URL` 写的端口 ≠ 实际对外端口 | 用 `telnet your-domain.com <端口>` 确认外部能连上 |
| `WebSocket handshake failed` | Nginx 没转发 `Upgrade: websocket` | 检查 `proxy_set_header Upgrade $http_upgrade` |
| 改了 `.env` 但没生效 | uvicorn 还是老进程 / Docker 没重建 | `python main.py` 重启；Docker 用 `docker compose up -d --force-recreate` |
| Docker 改 `SERVER_PORT` 后仍只能 18619 访问 | compose 内 `environment.SERVER_PORT` 把容器内强制设为 18619 | 改 `docker-compose.yml` 中的 `environment` 与 `ports` 同步，或直接保持 18619 只改 `WS_TUNNEL_URL` |

> 推荐：**生产用 Nginx 反代 + WSS（场景 B）**；本机临时调试用 cloudflared（场景 C）；只有完全自己的 VPS 才用裸端口（场景 A）。

---

## 账号准备与导入

每个账号对应 `users/user_<userId>.json`，结构：

```json
{
  "userId": "1234567890",
  "serviceToken": "粘贴你的 serviceToken",
  "xiaomichatbot_ph": "粘贴你的 xiaomichatbot_ph",
  "name": "可选-用于日志显示的别名"
}
```

### 提取 Cookie 字段

1. 浏览器登录 `https://aistudio.xiaomimimo.com`，进入 Claw（开发者沙箱）页面。
2. 打开 DevTools → Application/Storage → Cookies。
3. 复制以下三项的值：`userId`、`serviceToken`、`xiaomichatbot_ph`。

### 两种导入方式

**方式 A：通过 WebUI 一键导入（推荐）**

1. 打开 `http://<你的服务器>:18619/webui` 并登录。
2. 在「账号管理」中点击「添加账号」。
3. 直接粘贴 DevTools 复制下来的整段 Cookie 字符串（如 `userId=xxx; serviceToken=yyy; xiaomichatbot_ph=zzz`）。
4. 网关会自动正则解析三个字段，并写入 `users/user_<userId>.json`。

**方式 B：手动放置 JSON**

按上面结构在 `users/` 下创建文件即可，文件名必须形如 `user_<userId>.json`。

> 添加 / 删除账号后，建议在面板点「立即重建所有节点」（或 `POST /api/rebuild`）以即时生效，否则要等下一次 55 分钟轮换。

---

## 启动服务

```bash
python main.py
```

启动后会看到类似日志：

```
🚀 mimo2api 统一主入口 - 正在启动网关并绑定集群到 0.0.0.0:18619
🔗 云端要求 Claw 主动连接的桥接 WS URL 将统一下发为: ws://your-domain.com:18619/ws
🔐 AI API 鉴权已启用
🔐 WebUI 鉴权已启用，登录用户: admin
🚀 正在拉起挂后台的 Claw 账号守护线程...
🚀 mimo2api 分布式并发账号池控制引擎 (Manager) 已点火启动!
共通过 users/ 扫描并成功重载入 N 个授权用户预设账号。
...
✅ 内网节点已接入: x.x.x.x:xxxxx。当前在线节点数: 1
```

看到 `✅ 内网节点已接入` 就说明 Claw → 网关的反向隧道已建立，可以开始调用 API。

### 推荐用 systemd / pm2 / supervisor 守护

- 日志默认写入 `logs/gateway.log`（10 MB × 5 个轮转）。
- 项目自带单进程锁 `mimo2api.lock`，重复启动会报错保护。

---

## Web 控制面板

访问 `/webui`：

- **总览**：在线节点数、累计/实时请求量、成功率、首字延迟、Token 消耗等。
- **节点状态**：每个 Claw 容器的 Claw status (`AVAILABLE`/`DESTROYED`/...)、剩余寿命秒数、最近响应码。
- **历史曲线**：默认 24 小时（最长由 `METRICS_RETENTION_DAYS` 决定）。
- **错误日志**：最近若干条 4xx / 5xx 错误（含路径、模型、节选请求体）。
- **模型映射**：可视化编辑 `model_mapping.json`。
- **账号管理**：新增 / 删除账号 + 一键触发重建。

> 启用 WebUI 登录（设置 `MIMO_WEBUI_PASSWORD`）后，未登录访问会返回 401。`/api/auth/login`、`/api/auth/logout`、`/api/auth/session` 是公开的鉴权端点。

---

## API 使用指南

> 默认 API Base URL：`http://<你的服务器>:18619`
> 若设置了 `MIMO_RELAY_OPENAI_KEY`，所有 `/v1/*` 与 `/anthropic/v1/*` 请求都需在 Header 中携带 `Authorization: Bearer <key>`（`x-api-key` / `api-key` 也兼容）。

### 可用模型

`GET /v1/models` 或 `GET /anthropic/v1/models` 会返回如下原生模型 ID：

| 模型 ID | 显示名 | Context | Max Output |
| --- | --- | --- | --- |
| `mimo-v2.5-pro` | MiMo V2.5 Pro | 1,048,576 | 131,072 |
| `mimo-v2.5` | MiMo V2.5 | 1,048,576 | 131,072 |
| `mimo-v2.5-tts` | MiMo V2.5 TTS | 8,192 | 8,192 |
| `mimo-v2-pro` | MiMo V2 Pro | 1,048,576 | 131,072 |
| `mimo-v2-flash` | MiMo V2 Flash | 256,000 | 131,072 |
| `mimo-v2-omni` | MiMo V2 Omni | 256,000 | 131,072 |
| `mimo-v2.5-tts-voicedesign` | MiMo V2.5 TTS VoiceDesign | 8,192 | 8,192 |
| `mimo-v2.5-tts-voiceclone` | MiMo V2.5 TTS VoiceClone | 8,192 | 8,192 |
| `mimo-v2-tts` | MiMo V2 TTS | 8,192 | 8,192 |

### OpenAI 兼容接口

#### `POST /v1/chat/completions`

完全等价 OpenAI Chat Completions：

```bash
curl -N http://your-host:18619/v1/chat/completions \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mimo-v2.5-pro",
    "messages": [
      {"role": "user", "content": "用一句话解释黑洞"}
    ],
    "stream": true
  }'
```

支持字段：`messages`, `stream`, `tools`, `tool_choice`, `temperature`, `top_p`, `max_tokens` 等（直通透传，由上游解释）。

#### `POST /v1/responses`

OpenAI Responses API（新协议）。网关会自动转换为内部 chat.completions 调用，再把响应转回 Responses 协议（含完整 SSE 事件流）。

```bash
curl -N http://your-host:18619/v1/responses \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mimo-v2.5-pro",
    "input": "Say hi",
    "stream": true
  }'
```

> 若请求体未显式给 `stream`，`/v1/responses` 默认按流式返回。

#### `GET /v1/models`

列出可用模型。

### Anthropic 兼容接口

#### `POST /anthropic/v1/messages`

直接以 Claude Messages 协议调用：

```bash
curl -N http://your-host:18619/anthropic/v1/messages \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mimo-v2.5-pro",
    "max_tokens": 1024,
    "stream": true,
    "messages": [
      {"role": "user", "content": "你好"}
    ]
  }'
```

适用于 Claude Code、Cline、各类 Anthropic SDK。

#### `GET /anthropic/v1/models`

列出可用模型（Anthropic schema）。

### TTS 语音合成

#### `POST /v1/audio/speech`

OpenAI TTS 协议：

```bash
curl http://your-host:18619/v1/audio/speech \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "tts-1",
    "input": "你好，欢迎使用 MiMo 语音合成。",
    "voice": "alloy",
    "response_format": "mp3"
  }' --output speech.mp3
```

网关会把 `model` / `voice` 自动映射到 MiMo 的 TTS 模型与音色，并返回二进制音频。

### 模型映射 (Model Mapping)

通过 `model_mapping.json` 可让客户端使用任意名称（如 Claude / GPT 系列名），网关在转发前自动改写为真实的 MiMo 模型 ID。仓库自带示例：

```json
{
  "claude-haiku-4-5-20251001": "mimo-v2-flash",
  "claude-opus-4-7": "mimo-v2.5-pro",
  "claude-opus-4-6": "mimo-v2.5-pro",
  "sonnet-4.6": "mimo-v2.5",
  "gpt-5.5": "mimo-v2.5-pro",
  "gpt-5.4": "mimo-v2.5-pro",
  "gpt-5.4-mini": "mimo-v2-flash"
}
```

可通过 API 或 WebUI 在线编辑：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/model_mapping` | 读取当前映射 |
| `PUT` | `/api/model_mapping` | 整体覆盖（请求体为新映射 JSON） |
| `DELETE` | `/api/model_mapping/{model_name}` | 删除单条 |

---

## 管理 / 监控 API

> 这些接口在启用 WebUI 鉴权时同样需要登录态 Cookie；公共白名单仅 `/`、`/webui`、`/api/auth/*`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/rebuild` | 立即触发所有 Claw 节点强制销毁重建 |
| `GET`  | `/api/stats` | 实时网关统计（请求数 / 成功率 / 节点详情等） |
| `GET`  | `/api/status/history?hours=24` | 历史指标，`hours` ∈ [1, 24×保留天数] |
| `GET`  | `/api/errors?limit=50` | 最近 N 条错误日志，最大 200 |
| `GET`  | `/api/system/status` | 极简状态：当前在线节点数 |
| `GET`  | `/api/users/list` | 列出所有账号，并并发查询每个 Claw 实例的状态/剩余寿命 |
| `POST` | `/api/users/add` | 粘贴 Cookie 字符串（字段 `raw_text`）添加账号 |
| `DELETE` | `/api/users/delete/{userId}` | 删除账号 |
| `GET`  | `/api/auth/session` | 查询当前是否已登录 + 鉴权是否启用 |
| `POST` | `/api/auth/login` | `{username, password}` 登录 |
| `POST` | `/api/auth/logout` | 登出 |
| `WS`   | `/ws` | **Claw 容器内部 bridge.py 反向连接的隧道端点（不要从客户端调用）** |

---

## 客户端接入示例

### Python (OpenAI SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://your-host:18619/v1",
    api_key="sk-your-random-secret-here",  # 即 MIMO_RELAY_OPENAI_KEY
)

resp = client.chat.completions.create(
    model="mimo-v2.5-pro",
    messages=[{"role": "user", "content": "用 Python 写一个快速排序"}],
    stream=True,
)
for chunk in resp:
    delta = chunk.choices[0].delta.content or ""
    print(delta, end="", flush=True)
```

### Python (Anthropic SDK)

```python
from anthropic import Anthropic

client = Anthropic(
    base_url="http://your-host:18619/anthropic",
    api_key="sk-your-random-secret-here",
)
msg = client.messages.create(
    model="mimo-v2.5-pro",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Hello"}],
)
print(msg.content[0].text)
```

### 第三方 UI

- **Cherry Studio / NextChat / LobeChat**：选「OpenAI 兼容」，Base URL 填 `http://your-host:18619/v1`，Key 填 `MIMO_RELAY_OPENAI_KEY`。
- **OneAPI / NewAPI**：作为「OpenAI」上游接入即可。
- **Cline / Claude Code**：选「Anthropic」，Base URL 填 `http://your-host:18619/anthropic`。

---

## 部署建议

### 反向代理 (Nginx 示例)

强烈建议在前面挂一层 Nginx 提供 HTTPS，并把 `WS_TUNNEL_URL` 改为 `wss://`：

```nginx
server {
    listen 443 ssl http2;
    server_name your-domain.com;

    ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:18619;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;

        # WebSocket 必须
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection "upgrade";

        # 流式 / 长连接
        proxy_buffering off;
        proxy_read_timeout  3600s;
        proxy_send_timeout  3600s;
    }
}
```

对应 `.env`：

```dotenv
WS_TUNNEL_URL=wss://your-domain.com/ws
MIMO_WEBUI_COOKIE_SECURE=true
```

### systemd 单元示例

```ini
# /etc/systemd/system/mimi3.service
[Unit]
Description=mimi3 mimo2api gateway
After=network-online.target

[Service]
WorkingDirectory=/opt/mimi3
ExecStart=/usr/bin/python3 /opt/mimi3/main.py
Restart=always
RestartSec=5
EnvironmentFile=/opt/mimi3/.env

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now mimi3
journalctl -u mimi3 -f
```

### Docker / Docker Compose（推荐）

仓库已自带 `Dockerfile`、`docker-compose.yml`、`.dockerignore`，可一键起服务。

#### 一键启动

```bash
# 1. 准备 .env（至少填好 WS_TUNNEL_URL 和 MIMO_RELAY_OPENAI_KEY）
cp env.example .env
vim .env

# 2. 准备持久化目录
mkdir -p users logs data

# 3. 构建并后台启动
docker compose up -d --build

# 4. 查看日志
docker compose logs -f mimi3
```

启动后：

- 控制面板：`http://<宿主机 IP>:18619/webui`
- API 基址：`http://<宿主机 IP>:18619/v1`、`http://<宿主机 IP>:18619/anthropic/v1`

#### 镜像内置默认行为

| 项 | 容器内路径 / 值 | 说明 |
| --- | --- | --- |
| 工作目录 | `/app` | 项目代码 |
| 监听端口 | `18619` | 由 `SERVER_PORT` 覆盖 |
| 时区 | `Asia/Shanghai` | 通过 `tzdata` |
| `MIMO_METRICS_DB_PATH` | `/app/data/gateway_metrics.db` | SQLite 指标库 |
| `MIMO_METRICS_SNAPSHOT_PATH` | `/app/data/gateway_snapshot.json` | 内存指标快照 |
| `MIMO_PROCESS_LOCK_PATH` | `/app/data/mimo2api.lock` | 单进程锁 |
| 健康检查 | `GET /api/auth/session` | 30s 间隔，3 次失败标记 unhealthy |
| PID 1 | `tini` | 让 SIGTERM/SIGINT 干净地传递给 Python |

#### 持久化卷映射

`docker-compose.yml` 已把以下目录/文件 bind 挂载到宿主机当前目录：

| 宿主机 | 容器内 | 用途 |
| --- | --- | --- |
| `./users` | `/app/users` | 账号池（每账号一个 JSON），删除容器不丢账号 |
| `./logs` | `/app/logs` | gateway.log 滚动日志 |
| `./data` | `/app/data` | 指标 SQLite + 快照 + 进程锁 |
| `./model_mapping.json` | `/app/model_mapping.json` | 模型映射，可在 WebUI 实时编辑 |

> ⚠️ `./model_mapping.json` 是单文件挂载，**首次启动前宿主机必须存在该文件**。直接克隆本仓库即可（仓库已自带）。如果你是干净环境，先 `cp env.example .env` 后还需 `touch model_mapping.json && echo '{}' > model_mapping.json`。

#### 反向代理 / WSS 场景

若使用 Nginx + HTTPS 终止，把 `WS_TUNNEL_URL` 改成 `wss://your-domain.com/ws`，并把 compose 里的端口收到 `127.0.0.1`：

```yaml
ports:
  - "127.0.0.1:18619:18619"
```

然后由宿主机的 Nginx 反代到 `127.0.0.1:18619`（参考上面 [Nginx 示例](#反向代理-nginx-示例)）。

#### 常用运维命令

```bash
# 重启
docker compose restart mimi3

# 应用更新（拉取最新代码后重新构建）
git pull
docker compose up -d --build

# 查看实时日志
docker compose logs -f --tail=200 mimi3

# 进入容器内排查
docker compose exec mimi3 bash

# 停止 & 移除（数据保留在宿主机 ./users ./logs ./data）
docker compose down
```

#### 自行构建（不使用 compose）

```bash
docker build -t mimi3:latest .

docker run -d --name mimi3 \
  --restart unless-stopped \
  -p 18619:18619 \
  --env-file .env \
  -v $(pwd)/users:/app/users \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/model_mapping.json:/app/model_mapping.json \
  mimi3:latest
```

---

## 日志与排错

- **运行日志**：控制台 + `logs/gateway.log`（10 MB × 5 轮转）。
- **错误环形缓冲**：最近的 4xx/5xx 见 `/api/errors` 或 WebUI「错误日志」。
- **节点冷却**：上游返回 401 的节点会冷却 `MIMO_NODE_401_COOLDOWN_SECONDS`（默认 15 分钟），避免反复打到坏号。
- **悬挂队列扫描**：每 60 秒巡检一次，超过 5 分钟无活动的请求队列会被强制回收，防止内存泄漏。
- **流式 keepalive**：每 25 秒发一条 `: keep-alive` SSE 注释行；超过 60 秒上游无数据视为节点断开。

---

## 常见问题 FAQ

**Q1：为什么我看到 `🚀 Manager 启动` 但一直没有 `✅ 内网节点已接入`？**
99% 是 `WS_TUNNEL_URL` 不可达 —— Claw 容器在小米的公有云里，连不到你本地 / 内网的 IP。
请检查：
- 域名/IP 在公网是否能 ping / telnet `SERVER_PORT`；
- 反代是否正确转发了 `Upgrade: websocket`；
- 防火墙 / 安全组放行情况。

**Q2：返回 503 "Gateway Error: 没有可用的内网节点"？**
当前没有任何 Claw 节点在线（要么还没接入，要么全在冷却 / 重建中）。等待 1～2 分钟，或在 WebUI 触发一次 `/api/rebuild`。

**Q3：客户端请求 401？**
设置了 `MIMO_RELAY_OPENAI_KEY` 但请求未带 `Authorization: Bearer ...`，或值不匹配。

**Q4：可以同时跑多个 mimi3 实例吗？**
**不推荐**。同一台机器有 `mimo2api.lock` 单进程锁。多机同时用同一批账号会让 Claw 容器频繁互踩销毁，账号容易被封。

**Q5：账号会被封吗？**
本项目本质是用账号在云端 Claw 跑脚本反向中转，属于灰色用法。每 55 分钟重建一次容器即模拟正常使用上限，但**风险自担**，强烈建议优先使用小米官方 API。

**Q6：怎么调整每个容器的存活时长？**
当前固定为 55 分钟（接近官方 60 分钟上限），需要自定义请直接修改 `mimo2api/manager.py` 中的 `wait_time = 55 * 60`。

---

## 项目结构

```
mimi3/
├── main.py                       # 启动入口，加载 .env 并拉起 FastAPI
├── requirements.txt
├── env.example                   # 环境变量模板
├── model_mapping.json            # 客户端模型名 -> MiMo 模型 ID 映射
├── Dockerfile                    # 容器镜像构建
├── docker-compose.yml            # 一键编排（含卷挂载、健康检查）
├── .dockerignore                 # 镜像构建排除项
├── users/                        # 账号目录，每个账号一个 user_<uid>.json
│   └── .gitkeep
├── logs/                         # 运行日志（首次运行后自动创建）
├── data/                         # Docker 部署时的持久化目录（指标库/快照/锁）
└── mimo2api/
    ├── web_service.py            # FastAPI 主服务，所有 /v1 /anthropic 路由
    ├── manager.py                # 多账号 Claw 生命周期管理 + 桥接注入
    ├── bridge.py                 # 注入到 Claw 容器内部的反向 WS 桥接脚本
    ├── ui_router.py              # WebUI / 账号管理 / 鉴权登录路由
    ├── auth.py                   # API & WebUI 鉴权（Bearer + 签名 Cookie）
    ├── gateway_state.py          # 全局运行时状态（节点池、队列、冷却表）
    ├── responses_converter.py    # OpenAI Responses ↔ Chat Completions 协议互转
    ├── audio_helpers.py          # TTS 请求/音频 base64 处理
    ├── metrics_store.py          # SQLite 指标持久化、统计聚合
    └── webui.html                # 控制面板单页前端
```

---

## 免责声明

1. **本项目仅供学习交流使用，禁止一切商业 / 滥用行为。**
2. 本项目为个人独立开发的开源项目，与小米公司及其关联方**无任何隶属、授权或合作关系**。
3. MIMO、Xiaomi AI Studio 等名称及商标归小米公司所有，本项目不主张任何权利。
4. 本项目不提供任何小米账号、密钥或付费服务的破解，仅作为技术研究用途。
5. 使用者应遵守所在地法律法规及小米服务条款，因使用本项目产生的一切后果由使用者自行承担。
6. 本项目代码随缘更新，作者不提供任何保证或技术支持。
7. **建议优先使用小米官方 API**，本项目仅为技术研究备选方案。
8. 如有任何权益问题，请联系删除。

## 致谢

- [linux.do](https://linux.do)
