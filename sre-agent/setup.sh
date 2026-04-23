#!/bin/bash
# Installs the CineTrace SRE agent on the production server.
# Run once: bash /opt/cinetrace/sre-agent/setup.sh

set -e

AGENT_DIR="/opt/cinetrace/sre-agent"
ENV_FILE="$AGENT_DIR/agent.env"

echo "==> Installing Python dependencies..."
pip3 install -r "$AGENT_DIR/requirements.txt" -q

echo "==> Creating agent.env (edit this with your API key)..."
if [ ! -f "$ENV_FILE" ]; then
  cat > "$ENV_FILE" << 'EOF'
ANTHROPIC_API_KEY=
ALERT_EMAIL=connectnarada@gmail.com
# Optional — Gmail app password for email alerts
# SMTP_USER=your@gmail.com
# SMTP_PASS=your-app-password
EOF
  echo "    Created $ENV_FILE — add your ANTHROPIC_API_KEY"
else
  echo "    $ENV_FILE already exists — skipping"
fi

echo "==> Installing cron job (every 5 minutes)..."
CRON_LINE="*/5 * * * * source $ENV_FILE && python3 $AGENT_DIR/agent.py >> /var/log/cinetrace-sre.log 2>&1"

# Remove old watchdog + any existing SRE cron, then add fresh
( crontab -l 2>/dev/null | grep -v "watchdog.sh" | grep -v "sre-agent"; echo "$CRON_LINE" ) | crontab -

echo "==> Done. Cron job installed:"
crontab -l | grep sre-agent

echo ""
echo "Next step: add your Anthropic API key to $ENV_FILE"
echo "  nano $ENV_FILE"
echo ""
echo "Test manually:"
echo "  source $ENV_FILE && python3 $AGENT_DIR/agent.py"
