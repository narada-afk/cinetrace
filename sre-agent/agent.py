#!/usr/bin/env python3
"""
CineTrace SRE Agent
-------------------
Runs every 5 minutes via cron. Silent on healthy systems.
When something breaks, calls Claude to diagnose and fix it autonomously.

Logs to: /var/log/cinetrace-sre.log
Cooldown: won't call Claude more than once per 15 min (prevents runaway costs)
"""

import os
import sys
import json
import subprocess
import logging
import smtplib
import time
from email.mime.text import MIMEText
from datetime import datetime, timezone
from pathlib import Path

import requests
import anthropic

# ── Config ────────────────────────────────────────────────────────────────────

COMPOSE_FILE = "/opt/cinetrace/docker-compose.prod.yml"
COMPOSE      = ["docker", "compose", "-f", COMPOSE_FILE]
SERVICES     = ["backend", "frontend", "postgres", "redis"]
HEALTH_URL   = "http://localhost:8000/health"
LOG_FILE     = "/var/log/cinetrace-sre.log"
COOLDOWN_FILE= "/tmp/cinetrace-sre-cooldown"
COOLDOWN_SEC = 900   # 15 minutes between Claude calls
MAX_TURNS    = 6     # max agentic turns per incident

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ALERT_EMAIL       = os.getenv("ALERT_EMAIL", "connectnarada@gmail.com")
SMTP_USER         = os.getenv("SMTP_USER", "")
SMTP_PASS         = os.getenv("SMTP_PASS", "")

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("sre")

# ── Claude tools ──────────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "get_logs",
        "description": (
            "Fetch recent log lines from a Docker service. "
            "Always call this first to understand why a service is unhealthy."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "enum": SERVICES,
                    "description": "Service to get logs for",
                },
                "lines": {
                    "type": "integer",
                    "description": "Number of recent log lines (default 60)",
                },
            },
            "required": ["service"],
        },
    },
    {
        "name": "restart_service",
        "description": (
            "Restart a Docker service. Use after reading logs and confirming "
            "a restart will help. Avoid restarting postgres unless absolutely necessary."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "service": {
                    "type": "string",
                    "enum": SERVICES,
                    "description": "Service to restart",
                }
            },
            "required": ["service"],
        },
    },
    {
        "name": "run_diagnostic",
        "description": "Run a safe read-only diagnostic command on the server.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": (
                        "Shell command to run. Allowed prefixes: "
                        "df, free, uptime, ps aux, curl http://localhost, "
                        "cat /proc/meminfo, docker stats --no-stream"
                    ),
                }
            },
            "required": ["command"],
        },
    },
    {
        "name": "check_health",
        "description": "Re-check the current health of all containers. Use after a restart to verify it worked.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "send_alert",
        "description": (
            "Send an alert to the operator. Call this when: "
            "(1) you fixed the issue — severity=info, "
            "(2) the issue persists and needs human help — severity=critical."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "subject":  {"type": "string"},
                "message":  {"type": "string", "description": "What happened, what you did, current status"},
                "severity": {"type": "string", "enum": ["info", "warning", "critical"]},
            },
            "required": ["subject", "message", "severity"],
        },
    },
]

# ── Tool execution ────────────────────────────────────────────────────────────

SAFE_CMD_PREFIXES = [
    "df ", "free", "uptime", "ps aux", "ps -", "curl http://localhost",
    "cat /proc/meminfo", "docker stats --no-stream", "docker ps",
]

def execute_tool(name: str, inputs: dict) -> str:
    if name == "get_logs":
        service = inputs["service"]
        lines   = int(inputs.get("lines", 60))
        r = subprocess.run(
            COMPOSE + ["logs", service, f"--tail={lines}", "--no-log-prefix"],
            capture_output=True, text=True, cwd="/opt/cinetrace",
        )
        out = (r.stdout or r.stderr or "(no output)").strip()
        return out[-4000:]  # trim to last 4k chars

    elif name == "restart_service":
        service = inputs["service"]
        log.info(f"Restarting {service}…")
        r = subprocess.run(
            COMPOSE + ["restart", service],
            capture_output=True, text=True, cwd="/opt/cinetrace",
        )
        time.sleep(10)  # give it time to come up
        check = subprocess.run(
            ["docker", "inspect", "--format={{.State.Status}}", f"cinetrace-{service}-1"],
            capture_output=True, text=True,
        )
        status = check.stdout.strip()
        log.info(f"{service} status after restart: {status}")
        return f"Restart done. {service} is now: {status}"

    elif name == "run_diagnostic":
        cmd = inputs["command"]
        if not any(cmd.startswith(p) for p in SAFE_CMD_PREFIXES):
            return f"Command blocked (not in safe list): {cmd}"
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
        return (r.stdout or r.stderr or "(no output)").strip()[-2000:]

    elif name == "check_health":
        status = collect_status()
        return json.dumps(status, indent=2)

    elif name == "send_alert":
        subject  = inputs["subject"]
        message  = inputs["message"]
        severity = inputs.get("severity", "warning")
        log.info(f"ALERT [{severity.upper()}] {subject}")
        log.info(f"  {message[:300]}")
        _send_email(subject, message, severity)
        return f"Alert sent ({severity}): {subject}"

    return f"Unknown tool: {name}"


def _send_email(subject: str, body: str, severity: str):
    if not SMTP_USER or not SMTP_PASS:
        log.info("SMTP not configured — alert is logged only")
        return
    try:
        msg          = MIMEText(body)
        msg["Subject"] = f"[CineTrace SRE] [{severity.upper()}] {subject}"
        msg["From"]    = SMTP_USER
        msg["To"]      = ALERT_EMAIL
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        log.info(f"Email alert sent to {ALERT_EMAIL}")
    except Exception as e:
        log.error(f"Email failed: {e}")

# ── Health collection ─────────────────────────────────────────────────────────

def collect_status() -> dict:
    status: dict = {
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "containers": {},
        "api_health": None,
        "disk":       None,
        "memory":     None,
    }

    for svc in SERVICES:
        r = subprocess.run(
            ["docker", "inspect", "--format={{.State.Status}}", f"cinetrace-{svc}-1"],
            capture_output=True, text=True,
        )
        status["containers"][svc] = r.stdout.strip() or "not_found"

    try:
        r = requests.get(HEALTH_URL, timeout=5)
        status["api_health"] = r.json()
    except Exception as exc:
        status["api_health"] = f"UNREACHABLE: {exc}"

    for cmd, key in [("df -h /", "disk"), ("free -h", "memory")]:
        r = subprocess.run(cmd.split(), capture_output=True, text=True)
        status[key] = r.stdout.strip()

    return status


def is_healthy(status: dict) -> bool:
    for svc, state in status["containers"].items():
        if state != "running":
            return False
    api = status.get("api_health")
    if isinstance(api, str) and "UNREACHABLE" in api:
        return False
    return True

# ── Cooldown ──────────────────────────────────────────────────────────────────

def in_cooldown() -> bool:
    p = Path(COOLDOWN_FILE)
    if not p.exists():
        return False
    age = time.time() - p.stat().st_mtime
    return age < COOLDOWN_SEC


def set_cooldown():
    Path(COOLDOWN_FILE).touch()

# ── Agentic loop ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an SRE agent for CineTrace — a South Indian cinema analytics site
running on a Hetzner server. The stack:
  • Next.js frontend  (Docker, port 3000)
  • FastAPI backend   (Docker, port 8000)
  • PostgreSQL 15     (Docker, internal)
  • Redis 7           (Docker, internal)

Your job when called:
  1. Read logs to understand the root cause — never restart blindly.
  2. Take the minimal fix action (usually restart the affected service).
  3. Verify with check_health that the service recovered.
  4. Send an alert summarising what happened and what you did.
  5. If you cannot fix it, send a critical alert for human intervention.

Rules:
  - Always read logs before restarting.
  - Do NOT restart postgres unless you see database corruption evidence.
  - Keep actions minimal — one restart attempt per service per incident.
  - Be concise in your reasoning."""


def run_agent(status: dict):
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set — cannot run agent")
        return

    if in_cooldown():
        log.info("Cooldown active — skipping Claude call this cycle")
        return

    set_cooldown()
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    initial = (
        f"Production health check failed at {status['timestamp']}.\n\n"
        f"Status snapshot:\n{json.dumps(status, indent=2)}\n\n"
        "Please diagnose and fix the issue."
    )

    messages = [{"role": "user", "content": initial}]
    log.info("Handing off to Claude…")

    for turn in range(MAX_TURNS):
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            text = " ".join(
                b.text for b in response.content if hasattr(b, "text")
            )
            log.info(f"Claude finished (turn {turn+1}): {text[:300]}")
            break

        if response.stop_reason == "tool_use":
            results = []
            for block in response.content:
                if block.type == "tool_use":
                    log.info(f"Tool call: {block.name}({json.dumps(block.input)[:120]})")
                    out = execute_tool(block.name, block.input)
                    log.info(f"Tool result: {out[:200]}")
                    results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     out,
                    })
            messages.append({"role": "user", "content": results})
    else:
        log.warning("Max turns reached — sending escalation alert")
        _send_email(
            "SRE Agent could not resolve incident",
            f"Max diagnostic turns reached.\nLast status:\n{json.dumps(status, indent=2)}",
            "critical",
        )

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    status = collect_status()

    if is_healthy(status):
        sys.exit(0)   # all good — silent exit

    log.warning(f"Unhealthy: {status['containers']}  api={status['api_health']}")
    run_agent(status)
