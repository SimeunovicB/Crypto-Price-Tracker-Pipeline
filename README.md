# Crypto Price Tracker Pipeline

Tracks BTC price every minute and sends Telegram alerts when meaningful price changes occur.

## Alert Thresholds

| Window | Threshold | Typical Frequency |
|--------|-----------|-------------------|
| 1h     | ≥ 3%     | A few times/month in calm markets, multiple/day in volatile ones |
| 4h     | ≥ 5%     | ~2–4 times/month |
| 24h    | ≥ 8%     | ~1–3 times/month |

Alerts have a **1-hour cooldown** per window to prevent spam.

---

## Setup

### 1. Clone and install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your Telegram credentials:

```
TELEGRAM_BOT_TOKEN=your-bot-token-here
TELEGRAM_CHAT_ID=your-chat-id-here
```

---

## Running

### Local development (with Dagster UI)

```bash
source venv/bin/activate
dagster dev -f pipeline.py
```

Opens a dashboard at `http://127.0.0.1:3000` with scheduling, run history, and logs.

### Production / VM (lightweight, no Dagster)

#### Test it once

```bash
source venv/bin/activate
python main.py
```

#### Test with fake data (triggers all 3 alerts)

```bash
python3 -c "
import duckdb
conn = duckdb.connect('crypto.db')
conn.execute(\"INSERT INTO prices (currency, price, time) VALUES ('BTCUSD', 50000.0, NOW() - INTERVAL 2 HOUR)\")
conn.execute(\"INSERT INTO prices (currency, price, time) VALUES ('BTCUSD', 50000.0, NOW() - INTERVAL 5 HOUR)\")
conn.execute(\"INSERT INTO prices (currency, price, time) VALUES ('BTCUSD', 50000.0, NOW() - INTERVAL 25 HOUR)\")
conn.execute('DELETE FROM alert_cooldowns')
conn.close()
"

python main.py

# Clean up after test
python3 -c "
import duckdb
conn = duckdb.connect('crypto.db')
conn.execute('DELETE FROM prices WHERE price = 50000.0')
conn.execute('DELETE FROM alert_cooldowns')
conn.close()
"
```

#### Set up cron (runs every minute)

```bash
crontab -e
```

Add this line (adjust paths):

```
* * * * * cd /path/to/Crypto-Price-Tracker-Pipeline && /path/to/venv/bin/python main.py >> /home/${USERNAME}/crypto_bot.log 2>&1
```

---

## Useful Commands

```bash
# Check logs
tail -f ~/crypto_bot.log

# Verify cron is active
crontab -l

# Stop the cron job
crontab -e   # delete the line

# Query the database
python3 -c "
import duckdb
conn = duckdb.connect('crypto.db')
print(conn.execute('SELECT * FROM prices ORDER BY time DESC LIMIT 10').fetchall())
conn.close()
"
```

---

## Notes

- **API**: Uses `api.binance.us` which works from both the US and Europe.
- **`main.py` vs `pipeline.py`**: Same logic, different wrappers. `main.py` is lightweight for production (cron), `pipeline.py` is for local dev with Dagster UI.
- **Database**: `crypto.db` is auto-created on first run. Prices accumulate over time — alerts only work once enough historical data exists for each window (1h, 4h, 24h).
- **Cooldown**: After an alert fires, that same window won't alert again for 1 hour (configurable via `COOLDOWN_HOURS` in the code).
- Paths can depend on the environment

---

