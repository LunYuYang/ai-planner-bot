#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="$HOME/ai-planner-bot"
SERVICE_SRC="$APP_DIR/tg-bot.service"
SERVICE_DST="/etc/systemd/system/tg-bot.service"

cd "$APP_DIR"

if [ ! -d venv ]; then
  python3 -m venv venv
fi

source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

sudo cp "$SERVICE_SRC" "$SERVICE_DST"
sudo systemctl daemon-reload
sudo systemctl enable tg-bot
sudo systemctl restart tg-bot
sudo systemctl status tg-bot --no-pager
