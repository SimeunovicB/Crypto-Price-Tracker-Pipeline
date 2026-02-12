"""
Lightweight production script — no Dagster, just Python + cron.
Run via: crontab -e → */1 * * * * cd /path/to/project && /path/to/venv/bin/python main.py
"""

import logging
import os

import duckdb
import requests
from dotenv import load_dotenv
from healthcheck import send_heartbeat

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

# --- Config ---
SYMBOL = "BTCUSD"
API_URL = f"https://api.binance.us/api/v3/ticker/price?symbol={SYMBOL}"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crypto.db")

ALERT_THRESHOLDS = [
    ("1h", 1, 3.0),
    ("4h", 4, 5.0),
    ("24h", 24, 8.0),
]

COOLDOWN_HOURS = 1


# --- Telegram ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message})



# --- DB setup ---
def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            currency VARCHAR,
            price DOUBLE,
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_cooldowns (
            alert_key VARCHAR PRIMARY KEY,
            last_sent TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


# --- Alert logic ---

def is_on_cooldown(conn, alert_key):
    result = conn.execute(
        f"""
        SELECT last_sent FROM alert_cooldowns
        WHERE alert_key = ?
          AND last_sent > (NOW() - INTERVAL {COOLDOWN_HOURS} HOUR)
        """,
        [alert_key],
    ).fetchone()

    if result:
        log.info(f"Alert '{alert_key}' is on cooldown (last sent: {result[0]})")
        return True
    return False


def record_alert(conn, alert_key):
    conn.execute(
        """
        INSERT INTO alert_cooldowns (alert_key, last_sent) VALUES (?, NOW())
        ON CONFLICT (alert_key) DO UPDATE SET last_sent = NOW()
        """,
        [alert_key],
    )


def check_price_alerts(conn, currency, current_price):
    alerts = []

    for label, hours, threshold in ALERT_THRESHOLDS:
        alert_key = f"{currency}_{label}"

        if is_on_cooldown(conn, alert_key):
            continue

        result = conn.execute(
            f"""
            SELECT price FROM prices
            WHERE currency = ?
              AND time <= (NOW() - INTERVAL {hours} HOUR)
            ORDER BY time DESC
            LIMIT 1
            """,
            [currency],
        ).fetchone()

        if result is None:
            log.info(f"No data yet for {currency} @ {label} ago — skipping")
            continue

        old_price = result[0]
        pct_change = ((current_price - old_price) / old_price) * 100

        log.info(f"{currency} {label}: {old_price:.2f} → {current_price:.2f} ({pct_change:+.2f}%)")

        if abs(pct_change) >= threshold:
            arrow = "⬆️" if pct_change > 0 else "⬇️"
            alerts.append((label, f"{label}: {pct_change:+.2f}% {arrow}"))

    return alerts


# --- Main ---

def main():
    log.info("--- Starting application ---")
    log.info(f"Fetching {SYMBOL} from Binance...")

    response = requests.get(API_URL, timeout=10)
    log.info(f"API response status: {response.status_code}")

    if response.status_code != 200:
        log.error(f"API request failed (HTTP {response.status_code}): {response.text}")
        return

    item = response.json()
    log.info(f"{item['symbol']}: {item['price']}")

    if item.get("price") is None:
        log.warning(f"No price returned for {SYMBOL}")
        return

    price = float(item["price"])
    if price <= 0:
        log.warning(f"Skipping {SYMBOL} — price is not positive")
        return

    log.info(f"Connecting to DB at {DB_PATH}")
    conn = duckdb.connect(DB_PATH)
    init_db(conn)

    # Store the price
    conn.execute(
        "INSERT INTO prices (currency, price) VALUES (?, ?)",
        [SYMBOL, price],
    )

    # Check for meaningful changes
    log.info("Checking price alerts...")
    alerts = check_price_alerts(conn, SYMBOL, price)

    if alerts:
        message = (
            f"🚨 {SYMBOL} Price Alert\n"
            f"Current: ${price:,.2f}\n\n"
            + "\n".join(line for _, line in alerts)
        )
        log.info(f"Sending alert:\n{message}")
        send_telegram(message)
        log.info("Telegram alert sent successfully")

        for label, _ in alerts:
            record_alert(conn, f"{SYMBOL}_{label}")
        log.info(f"Cooldowns recorded for {len(alerts)} alert(s)")
    else:
        log.info(f"No meaningful change for {SYMBOL} — no alert sent")

    conn.close()
    log.info("--- Done ---")


if __name__ == "__main__":
    main()
    
    send_heartbeat()
