#!/usr/bin/env python3
"""
健康检查功能测试脚本

用法：
    python test_health_check.py
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 添加 mimo2api 到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mimo2api.health_checker import HealthChecker

async def test_health_check():
    """测试健康检查功能"""
    checker = HealthChecker()

    print("=" * 60)
    print("健康检查功能测试")
    print("=" * 60)

    # 检查配置
    api_key = os.getenv("MIMO_RELAY_OPENAI_KEY", "")
    server_host = os.getenv("SERVER_HOST", "127.0.0.1")
    server_port = os.getenv("SERVER_PORT", "23655")

    print(f"\n配置信息：")
    print(f"  API Base: http://{server_host}:{server_port}")
    print(f"  API Key: {'已配置' if api_key else '未配置（健康检查将跳过）'}")
    print(f"  测试模型: mimo-v2.5-pro")
    print(f"  测试消息: 你是谁")

    if not api_key:
        print("\n⚠️ 未配置 MIMO_RELAY_OPENAI_KEY，无法测试")
        return

    print("\n开始健康检查测试...")
    print("-" * 60)

    # 执行健康检查
    result = await checker.check_api_health(uid="test-account")

    print("-" * 60)
    if result:
        print("✅ 健康检查通过！")
    else:
        print("❌ 健康检查失败！")

    print("\n测试完成。")

if __name__ == "__main__":
    asyncio.run(test_health_check())
