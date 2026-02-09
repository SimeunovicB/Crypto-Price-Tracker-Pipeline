import requests
import duckdb
from dagster import AssetExecutionContext, ScheduleDefinition, Definitions, define_asset_job, asset
from telegram import send_telegram

# runs once when file loads
conn = duckdb.connect("crypto.db")
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
conn.close()

symbol = "BTCUSD"
api_url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"

# Alert thresholds: (lookback label, hours, % threshold)
ALERT_THRESHOLDS = [
    ("1h", 1, 3.0),
    ("4h", 4, 5.0),
    ("24h", 24, 8.0),
]

COOLDOWN_HOURS = 1


def is_on_cooldown(conn, alert_key, context):
    """Check if an alert was already sent within the cooldown period."""
    result = conn.execute(
        f"""
        SELECT last_sent FROM alert_cooldowns
        WHERE alert_key = ?
          AND last_sent > (NOW() - INTERVAL {COOLDOWN_HOURS} HOUR)
        """,
        [alert_key],
    ).fetchone()

    if result:
        context.log.info(f"Alert '{alert_key}' is on cooldown (last sent: {result[0]})")
        return True
    return False


def record_alert(conn, alert_key):
    """Record that an alert was sent (upsert)."""
    conn.execute(
        """
        INSERT INTO alert_cooldowns (alert_key, last_sent) VALUES (?, NOW())
        ON CONFLICT (alert_key) DO UPDATE SET last_sent = NOW()
        """,
        [alert_key],
    )


def check_price_alerts(conn, currency, current_price, context):
    """Compare current price against historical prices and return alert lines."""
    alerts = []

    for label, hours, threshold in ALERT_THRESHOLDS:
        alert_key = f"{currency}_{label}"

        if is_on_cooldown(conn, alert_key, context):
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
            context.log.info(f"No data yet for {currency} @ {label} ago — skipping")
            continue

        old_price = result[0]
        pct_change = ((current_price - old_price) / old_price) * 100

        context.log.info(
            f"{currency} {label}: {old_price:.2f} → {current_price:.2f} ({pct_change:+.2f}%)"
        )

        if abs(pct_change) >= threshold:
            arrow = "⬆️" if pct_change > 0 else "⬇️"
            alerts.append((label, f"{label}: {pct_change:+.2f}% {arrow}"))

    return alerts


@asset
def crypto_prices(context: AssetExecutionContext):
    """Fetch BTC price, store it, and alert on meaningful moves."""
    response = requests.get(api_url)
    item = response.json()  # single object: {"symbol": "BTCUSD", "price": "..."}

    context.log.info(f"{item['symbol']}: {item['price']}")

    if item.get("price") is None:
        context.log.warning(f"No price returned for {symbol}")
        return item

    price = float(item["price"])
    if price <= 0:
        context.log.warning(f"Skipping {symbol} — price is not positive")
        return item

    conn = duckdb.connect("crypto.db")

    # Store the price
    conn.execute(
        "INSERT INTO prices (currency, price) VALUES (?, ?)",
        [symbol, price],
    )

    # Check for meaningful changes
    alerts = check_price_alerts(conn, symbol, price, context)

    if alerts:
        message = (
            f"🚨 {symbol} Price Alert\n"
            f"Current: ${price:,.2f}\n\n"
            + "\n".join(line for _, line in alerts)
        )
        context.log.info(f"Sending alert:\n{message}")
        send_telegram(message)

        # Record cooldown for each triggered alert
        for label, _ in alerts:
            record_alert(conn, f"{symbol}_{label}")
    else:
        context.log.info(f"No meaningful change for {symbol} — no alert sent")

    conn.close()
    return item


crypto_job = define_asset_job("crypto_job", selection="*")

crypto_schedule = ScheduleDefinition(
    job=crypto_job,
    cron_schedule="*/1 * * * *",  # every minute
)


defs = Definitions(
    assets=[crypto_prices],
    schedules=[crypto_schedule],
)
