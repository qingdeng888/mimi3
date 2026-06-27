"""
Anthropic Messages API <-> OpenAI Chat Completions API 格式转换器

支持:
- 请求转换: Anthropic Messages → OpenAI Chat Completions
- 非流式响应转换: OpenAI Chat Completions → Anthropic Messages
- 流式响应转换: OpenAI SSE → Anthropic SSE
"""

import json
import secrets
import time
import logging
from typing import Any, Optional

logger = logging.getLogger("AnthropicConverter")


def _generate_msg_id() -> str:
    return f"msg_{secrets.token_hex(12)}"


# ─── 请求转换 (Anthropic → OpenAI) ───────────────────────────────────────────


def convert_anthropic_request(req: dict[str, Any]) -> dict[str, Any]:
    """
    将 Anthropic Messages API 请求转换为 OpenAI Chat Completions 请求。

    Anthropic 格式特点:
    - system 是独立的顶层字段（字符串或数组）
    - messages 中 content 是 content block 数组
    - tool_use 块在 assistant 消息中
    - tool_result 块在 user 消息中
    - tools 用 input_schema 而非 parameters
    """
    openai_messages: list[dict[str, Any]] = []

    # 1. 处理 system 消息
    system = req.get("system")
    if system:
        if isinstance(system, str):
            openai_messages.append({"role": "system", "content": system})
        elif isinstance(system, list):
            # system 可以是 content block 数组
            system_text = _extract_system_text(system)
            if system_text:
                openai_messages.append({"role": "system", "content": system_text})

    # 2. 转换 messages
    for msg in req.get("messages", []):
        role = msg.get("role", "user")
        content = msg.get("content")

        if role == "assistant":
            openai_messages.extend(_convert_assistant_message(content))
        elif role == "user":
            openai_messages.extend(_convert_user_message(content))
        else:
            # 其他 role 直接透传
            openai_messages.append({"role": role, "content": _content_to_string(content)})

    # 3. 构建 OpenAI 请求
    openai_req: dict[str, Any] = {
        "messages": openai_messages,
        "stream": req.get("stream", False),
    }

    # 模型映射
    if "model" in req:
        openai_req["model"] = req["model"]

    # max_tokens
    if "max_tokens" in req:
        openai_req["max_tokens"] = req["max_tokens"]

    # temperature
    if "temperature" in req:
        openai_req["temperature"] = req["temperature"]

    # top_p
    if "top_p" in req:
        openai_req["top_p"] = req["top_p"]

    # top_k (OpenAI 不直接支持，但某些兼容 API 支持)
    if "top_k" in req:
        openai_req["top_k"] = req["top_k"]

    # stop_sequences → stop
    if "stop_sequences" in req:
        openai_req["stop"] = req["stop_sequences"]

    # tools 转换
    if "tools" in req:
        openai_tools = _convert_tools(req["tools"])
        if openai_tools:
            openai_req["tools"] = openai_tools

    # tool_choice
    if "tool_choice" in req:
        openai_req["tool_choice"] = _convert_tool_choice(req["tool_choice"])

    # 如果流式，添加 stream_options 以获取 usage
    if openai_req.get("stream"):
        openai_req["stream_options"] = {"include_usage": True}

    return openai_req


def _extract_system_text(system_blocks: list) -> str:
    """从 system content block 数组中提取文本。"""
    parts = []
    for block in system_blocks:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            if block.get("type") == "text":
                parts.append(block.get("text", ""))
            elif block.get("type") == "cache_control":
                # cache_control 是 Anthropic 的缓存标记，忽略
                continue
            else:
                parts.append(block.get("text", ""))
    return "\n\n".join(parts)


def _convert_assistant_message(content: Any) -> list[dict[str, Any]]:
    """转换 assistant 消息，处理 text 和 tool_use 块。"""
    if content is None:
        return [{"role": "assistant", "content": ""}]

    if isinstance(content, str):
        return [{"role": "assistant", "content": content}]

    if not isinstance(content, list):
        return [{"role": "assistant", "content": str(content)}]

    # 分离 text 块和 tool_use 块
    text_parts = []
    tool_calls = []

    for block in content:
        if not isinstance(block, dict):
            text_parts.append(str(block))
            continue

        block_type = block.get("type", "")

        if block_type == "text":
            text_parts.append(block.get("text", ""))
        elif block_type == "thinking":
            # Claude 的 thinking block，映射为 reasoning_content
            pass  # 会在下面处理
        elif block_type == "tool_use":
            tool_calls.append({
                "id": block.get("id", f"call_{secrets.token_hex(12)}"),
                "type": "function",
                "function": {
                    "name": block.get("name", ""),
                    "arguments": json.dumps(block.get("input", {}), ensure_ascii=False),
                },
            })

    # 构建 assistant 消息
    assistant_msg: dict[str, Any] = {"role": "assistant"}

    combined_text = "\n".join(text_parts) if text_parts else None
    assistant_msg["content"] = combined_text

    if tool_calls:
        assistant_msg["tool_calls"] = tool_calls

    # 处理 thinking
    thinking_parts = [
        block.get("thinking", "") for block in content
        if isinstance(block, dict) and block.get("type") == "thinking"
    ]
    if thinking_parts:
        assistant_msg["reasoning_content"] = "\n".join(thinking_parts)

    return [assistant_msg]


def _convert_user_message(content: Any) -> list[dict[str, Any]]:
    """转换 user 消息，处理 text、image、tool_result 块。"""
    if content is None:
        return [{"role": "user", "content": ""}]

    if isinstance(content, str):
        return [{"role": "user", "content": content}]

    if not isinstance(content, list):
        return [{"role": "user", "content": str(content)}]

    # 分离各类块
    content_parts: list[dict[str, Any]] = []
    tool_results: list[dict[str, Any]] = []
    messages: list[dict[str, Any]] = []

    for block in content:
        if not isinstance(block, dict):
            content_parts.append({"type": "text", "text": str(block)})
            continue

        block_type = block.get("type", "")

        if block_type == "text":
            content_parts.append({"type": "text", "text": block.get("text", "")})
        elif block_type == "image":
            # Anthropic 图片格式
            source = block.get("source", {})
            if source.get("type") == "base64":
                media_type = source.get("media_type", "image/png")
                data = source.get("data", "")
                content_parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{media_type};base64,{data}"},
                })
            elif source.get("type") == "url":
                content_parts.append({
                    "type": "image_url",
                    "image_url": {"url": source.get("url", "")},
                })
        elif block_type == "tool_result":
            tool_results.append(block)
        elif block_type == "document":
            # PDF 或其他文档，作为文本附件处理
            source = block.get("source", {})
            content_parts.append({
                "type": "text",
                "text": f"[Document: {block.get('title', 'untitled')}]",
            })

    # 先输出 tool_result 作为独立的 tool 角色消息
    for result in tool_results:
        tool_content = _extract_tool_result_content(result)
        messages.append({
            "role": "tool",
            "tool_call_id": result.get("tool_use_id", ""),
            "content": tool_content,
        })

    # 然后输出 user 内容（如果有）
    if content_parts:
        if len(content_parts) == 1 and content_parts[0].get("type") == "text":
            messages.append({"role": "user", "content": content_parts[0]["text"]})
        else:
            messages.append({"role": "user", "content": content_parts})

    return messages if messages else [{"role": "user", "content": ""}]


def _extract_tool_result_content(result: dict) -> str:
    """从 tool_result 块中提取内容字符串。"""
    content = result.get("content")
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                if part.get("type") == "text":
                    parts.append(part.get("text", ""))
                elif part.get("type") == "image":
                    parts.append("[image]")
                else:
                    parts.append(json.dumps(part, ensure_ascii=False))
        return "\n".join(parts)
    return str(content)


def _convert_tools(anthropic_tools: list) -> list[dict[str, Any]]:
    """将 Anthropic 工具定义转换为 OpenAI 格式。"""
    openai_tools = []
    for tool in anthropic_tools:
        if not isinstance(tool, dict):
            continue
        tool_type = tool.get("type", "custom")

        # 跳过 Anthropic 内置工具类型（如 web_search）
        if tool_type in ("web_search_20250305", "computer_20250124", "text_editor_20250124", "bash_20250124"):
            continue

        openai_tools.append({
            "type": "function",
            "function": {
                "name": tool.get("name", ""),
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
            },
        })
    return openai_tools


def _convert_tool_choice(anthropic_tool_choice: Any) -> Any:
    """转换 tool_choice。"""
    if isinstance(anthropic_tool_choice, str):
        return anthropic_tool_choice

    if isinstance(anthropic_tool_choice, dict):
        tc_type = anthropic_tool_choice.get("type", "")
        if tc_type == "auto":
            return "auto"
        elif tc_type == "any":
            return "required"
        elif tc_type == "tool":
            return {
                "type": "function",
                "function": {"name": anthropic_tool_choice.get("name", "")},
            }
    return "auto"


def _content_to_string(content: Any) -> str:
    """将任意 content 转为字符串。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "\n".join(parts)
    return str(content)


# ─── 非流式响应转换 (OpenAI → Anthropic) ─────────────────────────────────────


def convert_openai_response(openai_resp: dict[str, Any], model: str) -> dict[str, Any]:
    """
    将 OpenAI Chat Completions 响应转换为 Anthropic Messages 格式。
    """
    choice = (openai_resp.get("choices") or [{}])[0]
    message = choice.get("message", {})
    content: list[dict[str, Any]] = []

    # 处理 reasoning_content (thinking)
    if reasoning := message.get("reasoning_content"):
        content.append({
            "type": "thinking",
            "thinking": reasoning,
        })

    # 处理文本内容
    if text := message.get("content"):
        content.append({"type": "text", "text": text})

    # 处理 tool_calls
    if tool_calls := message.get("tool_calls"):
        for tc in tool_calls:
            func = tc.get("function", {})
            try:
                input_data = json.loads(func.get("arguments", "{}"))
            except (json.JSONDecodeError, TypeError):
                input_data = {}
            content.append({
                "type": "tool_use",
                "id": tc.get("id", f"toolu_{secrets.token_hex(12)}"),
                "name": func.get("name", ""),
                "input": input_data,
            })

    # 确保 content 至少有一个元素
    if not content:
        content.append({"type": "text", "text": ""})

    # 映射 stop_reason
    stop_reason = _map_finish_reason(choice.get("finish_reason"))

    # 构建 usage
    usage_data = openai_resp.get("usage", {})
    usage = {
        "input_tokens": usage_data.get("prompt_tokens", 0),
        "output_tokens": usage_data.get("completion_tokens", 0),
    }

    return {
        "id": _generate_msg_id(),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": usage,
    }


def _map_finish_reason(finish_reason: Optional[str]) -> str:
    """映射 OpenAI finish_reason → Anthropic stop_reason。"""
    mapping = {
        "stop": "end_turn",
        "tool_calls": "tool_use",
        "length": "max_tokens",
        "content_filter": "end_turn",
        None: "end_turn",
    }
    return mapping.get(finish_reason, "end_turn")


# ─── 流式响应转换 (OpenAI SSE → Anthropic SSE) ────────────────────────────────


class AnthropicStreamConverter:
    """
    将 OpenAI 流式 SSE 块转换为 Anthropic 流式 SSE 事件。

    Anthropic 流式格式:
    - event: message_start       (消息开始)
    - event: content_block_start (内容块开始)
    - event: content_block_delta (内容块增量)
    - event: content_block_stop  (内容块结束)
    - event: message_delta       (消息结束信息)
    - event: message_stop        (流结束)
    """

    def __init__(self, model: str):
        self.model = model
        self.message_id = _generate_msg_id()
        self.started = False
        self.current_text_block_index: Optional[int] = None
        self.content_block_index = 0
        self.tool_call_buffers: dict[int, dict] = {}  # OpenAI tool_call index → buffer
        self.tool_block_indices: dict[int, int] = {}  # OpenAI tool_call index → anthropic block index
        self.input_tokens = 0
        self.output_tokens = 0
        self.finish_reason: Optional[str] = None
        self._pending_events: list[str] = []

    def process_chunk(self, raw_line: str) -> list[str]:
        """
        处理一行 SSE 数据（去掉 "data: " 前缀后的内容）。
        返回要发送的 Anthropic SSE 事件列表。
        """
        events: list[str] = []

        line = raw_line.strip()
        if not line:
            return events

        # 处理 SSE 格式
        if line.startswith("data: "):
            data_str = line[6:]
        elif line.startswith("data:"):
            data_str = line[5:]
        else:
            return events

        if data_str.strip() == "[DONE]":
            # 流结束，发送结束事件
            events.extend(self._finish_stream())
            return events

        try:
            chunk = json.loads(data_str)
        except json.JSONDecodeError:
            return events

        # 如果还没发送 message_start，先发送
        if not self.started:
            events.append(self._make_message_start(chunk))
            self.started = True

        # 提取 usage（如果有）
        if usage := chunk.get("usage"):
            self.input_tokens = usage.get("prompt_tokens", self.input_tokens)
            self.output_tokens = usage.get("completion_tokens", self.output_tokens)

        # 处理 choices
        choices = chunk.get("choices", [])
        if not choices:
            return events

        choice = choices[0]
        delta = choice.get("delta", {})
        finish_reason = choice.get("finish_reason")

        if finish_reason:
            self.finish_reason = finish_reason

        # 处理 reasoning_content (thinking)
        if reasoning := delta.get("reasoning_content"):
            # thinking 块 - 目前大多数场景我们跳过，或者简化处理
            # 如果需要完整支持，可以添加 thinking block
            pass

        # 处理文本内容
        if content := delta.get("content"):
            if self.current_text_block_index is None:
                # 开始一个新的 text block
                self.current_text_block_index = self.content_block_index
                self.content_block_index += 1
                events.append(self._make_event("content_block_start", {
                    "type": "content_block_start",
                    "index": self.current_text_block_index,
                    "content_block": {"type": "text", "text": ""},
                }))

            events.append(self._make_event("content_block_delta", {
                "type": "content_block_delta",
                "index": self.current_text_block_index,
                "delta": {"type": "text_delta", "text": content},
            }))

        # 处理 tool_calls
        if tool_calls := delta.get("tool_calls"):
            # 在 tool_calls 开始前，如果有 text block 还在活跃，先关闭它
            if self.current_text_block_index is not None and any(tc.get("id") for tc in tool_calls):
                events.append(self._make_event("content_block_stop", {
                    "type": "content_block_stop",
                    "index": self.current_text_block_index,
                }))
                self.current_text_block_index = None

            for tc in tool_calls:
                tc_index = tc.get("index", 0)

                if tc.get("id"):
                    # 新的 tool_call 开始
                    # 如果之前有 text block 还在活跃，关闭它
                    if self.current_text_block_index is not None:
                        events.append(self._make_event("content_block_stop", {
                            "type": "content_block_stop",
                            "index": self.current_text_block_index,
                        }))
                        self.current_text_block_index = None

                    block_idx = self.content_block_index
                    self.content_block_index += 1
                    self.tool_block_indices[tc_index] = block_idx
                    self.tool_call_buffers[tc_index] = {
                        "id": tc["id"],
                        "name": tc.get("function", {}).get("name", ""),
                        "arguments": tc.get("function", {}).get("arguments", ""),
                    }

                    events.append(self._make_event("content_block_start", {
                        "type": "content_block_start",
                        "index": block_idx,
                        "content_block": {
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": tc.get("function", {}).get("name", ""),
                            "input": {},
                        },
                    }))

                    # 如果第一次就有 arguments
                    if args := tc.get("function", {}).get("arguments", ""):
                        events.append(self._make_event("content_block_delta", {
                            "type": "content_block_delta",
                            "index": block_idx,
                            "delta": {"type": "input_json_delta", "partial_json": args},
                        }))
                elif tc.get("function", {}).get("arguments"):
                    # 继续接收 arguments
                    args = tc["function"]["arguments"]
                    if tc_index in self.tool_call_buffers:
                        self.tool_call_buffers[tc_index]["arguments"] += args
                    block_idx = self.tool_block_indices.get(tc_index, self.content_block_index - 1)
                    events.append(self._make_event("content_block_delta", {
                        "type": "content_block_delta",
                        "index": block_idx,
                        "delta": {"type": "input_json_delta", "partial_json": args},
                    }))

        # 如果有 finish_reason，流即将结束
        if finish_reason:
            events.extend(self._close_active_blocks())

        return events

    def _finish_stream(self) -> list[str]:
        """生成流结束事件。"""
        events = []

        # 确保所有活跃的 block 被关闭
        events.extend(self._close_active_blocks())

        # message_delta
        stop_reason = _map_finish_reason(self.finish_reason)
        events.append(self._make_event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": None},
            "usage": {"output_tokens": self.output_tokens},
        }))

        # message_stop
        events.append(self._make_event("message_stop", {"type": "message_stop"}))

        return events

    def _close_active_blocks(self) -> list[str]:
        """关闭所有活跃的内容块。"""
        events = []

        # 关闭 text block
        if self.current_text_block_index is not None:
            events.append(self._make_event("content_block_stop", {
                "type": "content_block_stop",
                "index": self.current_text_block_index,
            }))
            self.current_text_block_index = None

        # 关闭所有 tool_use blocks
        for tc_index, block_idx in list(self.tool_block_indices.items()):
            events.append(self._make_event("content_block_stop", {
                "type": "content_block_stop",
                "index": block_idx,
            }))
        self.tool_block_indices.clear()

        return events

    def _make_message_start(self, first_chunk: dict) -> str:
        """构建 message_start 事件。"""
        # 从第一个 chunk 中尝试提取 usage
        usage = first_chunk.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        if input_tokens:
            self.input_tokens = input_tokens

        return self._make_event("message_start", {
            "type": "message_start",
            "message": {
                "id": self.message_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": self.model,
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": self.input_tokens, "output_tokens": 0},
            },
        })

    @staticmethod
    def _make_event(event_type: str, data: dict) -> str:
        """构建 SSE 事件字符串。"""
        return f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
