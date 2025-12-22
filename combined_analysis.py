#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
from datetime import datetime


def install_and_import(package, import_name=None):
    if import_name is None:
        import_name = package
    try:
        return __import__(import_name)
    except Exception:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return __import__(import_name)


MARKER_START = "__TG_MESSAGE_START__"
MARKER_END = "__TG_MESSAGE_END__"
COMBINED_ENV_FLAG = "TG_COMBINED"


def load_env():
    try:
        dotenv = install_and_import("python-dotenv", "dotenv")
        dotenv.load_dotenv()
    except Exception:
        pass


def run_script_capture(script_name):
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    if not os.path.exists(script_path):
        print(f"Missing script: {script_path}")
        return 1, ""

    env = os.environ.copy()
    env[COMBINED_ENV_FLAG] = "1"

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        output = (result.stdout or "") + (result.stderr or "")
        return result.returncode, output
    except Exception as exc:
        print(f"Error running {script_name}: {exc}")
        return 1, ""


def extract_message(output):
    start = output.find(MARKER_START)
    if start == -1:
        return ""
    start += len(MARKER_START)
    end = output.find(MARKER_END, start)
    if end == -1:
        return ""
    return output[start:end].strip()


def strip_timestamp(message):
    lines = [line for line in message.splitlines() if "⏰" not in line]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines)


def send_telegram_message(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram credentials not configured.")
        return False

    try:
        requests = install_and_import("requests")
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.json().get("ok", False)
    except Exception as exc:
        print(f"Telegram error: {exc}")
        return False


def main():
    load_env()

    code_a, out_a = run_script_capture("ema_aligned.py")
    code_b, out_b = run_script_capture("score_combined.py")

    msg_a = strip_timestamp(extract_message(out_a))
    msg_b = strip_timestamp(extract_message(out_b))

    parts = [msg for msg in [msg_a, msg_b] if msg]
    if not parts:
        print("No Telegram message to send.")
        if code_a != 0 or code_b != 0:
            sys.exit(1)
        return

    combined = "\n\n".join(parts)
    combined += f"\n\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')} Paris"

    if not send_telegram_message(combined):
        sys.exit(1)

    if code_a != 0 or code_b != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
