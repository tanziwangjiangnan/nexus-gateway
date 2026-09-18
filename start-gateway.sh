#!/bin/bash
# 启动实验网关（127.0.0.1:8646）。
# 密钥来自 /root/.hermes/.env —— gateway.yaml 里写的是 ${DEEPSEEK_API_KEY} 等引用，
# 直接 python3 gateway.py 会因缺变量报 "key not resolved"，务必经此脚本启动。
# 用法: ./start-gateway.sh
# [2026-09-18] 冲突保护：生产网关由 systemd(gateway.service) 托管。
# 手工再起一个会抢 8646 端口，让 systemd 侧反复 "address already in use" 重启
# （2026-09-18 15:42 实际发生过：32 条端口占用 + 34 次重启）。
if systemctl is-active --quiet gateway.service; then
  _pid=$(systemctl show gateway.service -p MainPID --value)
  echo "gateway.service 正在运行（systemd 托管，PID ${_pid}）——无需手工启动。"
  echo "要重启请用：systemctl restart gateway.service"
  echo "要临时做实验：请另给 GATEWAY_PORT / GATEWAY_DB_PATH，别抢 8646。"
  exit 2
fi

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
