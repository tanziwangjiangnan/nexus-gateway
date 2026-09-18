#!/bin/bash
# 启动实验网关（127.0.0.1:8646）。
# 密钥来自 /root/.hermes/.env —— gateway.yaml 里写的是 ${DEEPSEEK_API_KEY} 等引用，
# 直接 python3 gateway.py 会因缺变量报 "key not resolved"，务必经此脚本启动。
# 用法: ./start-gateway.sh
cd "$(dirname "$0")" || exit 1

set -a
# shellcheck disable=SC1091
. /root/.hermes/.env
set +a

PY=/root/.cache/uv/archive-v0/WdYmsw0dGK6sV6qi/bin/python3
# setsid 让网关脱离当前终端/tmux 会话，否则会话结束时网关会被一起回收。
setsid nohup "$PY" gateway.py >> gateway.log 2>&1 < /dev/null &
sleep 5
echo "已启动，PID $(cat gateway.pid 2>/dev/null || echo '?')（父进程应为 1）"
