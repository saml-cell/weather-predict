#!/usr/bin/env python3
"""
Tiny ntfy.sh wrapper for machine-generated alerts.

ntfy is a push-notification service that accepts simple HTTP POSTs and
relays them to subscribed mobile/web clients. No credentials required on
the public instance — just a topic URL. Topic names act as channels;
anyone who knows a topic URL can subscribe to it, so use a long random
topic name to keep a "private" channel on the public server.

Environment variables (read from .env via db.load_config or os.environ):
    NTFY_URL    — base URL (default https://ntfy.sh)
    NTFY_TOPIC  — your secret topic name. REQUIRED for alerts to be sent.

Usage (Python):
    from ntfy_alert import send
    send(
        "MOS drift detected",
        "Bratislava/temp_c regressed 3 days running",
        priority="high",
        tags=["warning", "robot"],
    )

Usage (CLI, for testing):
    .venv/bin/python scripts/ntfy_alert.py --title "test" --message "hello from the weather server"

This is a **best-effort** helper: if the network is down or ntfy is rate
limited, it logs the failure and returns False. It will never raise.
"""

import argparse
import logging
import os
import sys
import urllib.error
import urllib.request

logger = logging.getLogger("ntfy_alert")

DEFAULT_URL = "https://ntfy.sh"
HTTP_TIMEOUT_S = 10
USER_AGENT = "weather-predict-ntfy/1.0"


def _load_env():
    """Load .env file into os.environ if present. Idempotent."""
    env_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".env",
    )
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path) as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception as e:
        logger.debug("could not read .env: %s", e)


def send(
    title: str,
    message: str,
    priority: str = "default",
    tags: list = None,
    topic: str = None,
    url: str = None,
    click: str = None,
) -> bool:
    """Publish a notification to ntfy. Returns True on success, False on any
    failure (network, HTTP, config missing).

    Args:
        title: short headline (shown as notification title on phones)
        message: body text (can be multi-line)
        priority: "min" | "low" | "default" | "high" | "urgent"
        tags: list of emoji/tag strings, e.g. ["warning", "robot"]
        topic: topic name. If None, read from NTFY_TOPIC env var.
        url: ntfy server URL. If None, read from NTFY_URL env var (default
             https://ntfy.sh).
        click: optional URL opened when the notification is tapped.
    """
    _load_env()
    topic = topic or os.environ.get("NTFY_TOPIC")
    if not topic:
        logger.warning(
            "NTFY_TOPIC not set in environment; skipping alert '%s'", title
        )
        return False
    url = url or os.environ.get("NTFY_URL", DEFAULT_URL)
    full = f"{url.rstrip('/')}/{topic}"

    headers = {
        "User-Agent": USER_AGENT,
        "Title": title,
        "Priority": priority,
    }
    if tags:
        headers["Tags"] = ",".join(tags)
    if click:
        headers["Click"] = click

    try:
        req = urllib.request.Request(
            full, data=message.encode("utf-8"), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            if resp.status < 300:
                logger.debug("ntfy → %s (%d bytes)", topic, len(message))
                return True
            logger.warning("ntfy unexpected status %d", resp.status)
            return False
    except urllib.error.HTTPError as e:
        logger.warning("ntfy HTTP %d: %s", e.code, e.reason)
    except urllib.error.URLError as e:
        logger.warning("ntfy network error: %s", e.reason)
    except Exception as e:
        logger.warning("ntfy unexpected error: %s", e)
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--title", default="weather-predict test")
    p.add_argument("--message", default="Hello from scripts/ntfy_alert.py")
    p.add_argument(
        "--priority",
        choices=["min", "low", "default", "high", "urgent"],
        default="default",
    )
    p.add_argument("--tags", default="", help="comma-separated")
    p.add_argument("--topic", default=None, help="override NTFY_TOPIC")
    p.add_argument("--click", default=None, help="optional URL to open on tap")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    ok = send(
        title=args.title,
        message=args.message,
        priority=args.priority,
        tags=[t for t in args.tags.split(",") if t],
        topic=args.topic,
        click=args.click,
    )
    if ok:
        print("sent ✓")
        sys.exit(0)
    print("failed ✗")
    sys.exit(1)


if __name__ == "__main__":
    main()
