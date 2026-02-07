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
conn.close()

symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
api_url = "https://api.binance.com/api/v3/ticker/price"


@asset
def crypto_prices(context: AssetExecutionContext):
    # your existing fetch + save logic
    response = requests.get(api_url)
    # print("Response ", response.json())
    response = response.json()
    data = [item for item in response if item["symbol"] in symbols]
    
    telegram_message = ""
    
    conn = duckdb.connect("crypto.db")
    for item in data:
        print(item["symbol"], item["price"])
        context.log.info(f"{item['symbol']}: {item['price']}")
        if item["symbol"] in symbols and item.get("price") is not None:
            price = float(item["price"])
            if price > 0:
                conn.execute(
                    "INSERT INTO prices (currency, price) VALUES (?, ?)",
                    [item["symbol"], float(item["price"])]
                )
                telegram_message += f"{item['symbol']}: {price}\n"
            else:
                context.log.warning(f"Skipping {item['symbol']} because price is not positive")
                
    send_telegram(f"{telegram_message}")
        
    conn.close()
    return data

# TODO After this is done, for a certain change to send a message to a telegram channel (whatsapp?)


crypto_job = define_asset_job("crypto_job", selection="*")

crypto_schedule = ScheduleDefinition(
    job=crypto_job,
    # cron_schedule="0 * * * *",  # every hour
    cron_schedule="*/1 * * * *",  # every minute
)


defs = Definitions(
    assets=[crypto_prices],
    schedules=[crypto_schedule],
)