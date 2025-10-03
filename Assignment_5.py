from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta, datetime
import snowflake.connector
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn


@task
def return_last_90d_price(symbol):
    vantage_api_key = Variable.get("vantage_api_key")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url)
    data = r.json()
    results = []

    cutoff_date = datetime.now() - timedelta(days=90)
    for d, stock_info in data["Time Series (Daily)"].items():
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        if date_obj >= cutoff_date:
            stock_info["date"] = d
            results.append(stock_info)

    return results


@task
def load_data(conn, records):
    conn = return_snowflake_conn()
    target_table = f"{database}.raw.stock_price"
    cur = conn.cursor()
    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(10,4) NOT NULL,
            close DECIMAL(10,4) NOT NULL,
            high DECIMAL(10,4) NOT NULL,
            low DECIMAL(10,4) NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (symbol, date)
        );""")

        cur.execute(f"DELETE FROM {target_table}")

        for r in records:
            date = r["date"]
            open = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]

            insert_sql = f"""
                INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
                VALUES ('TSCO.LON', '{date}', {open}, {high}, {low}, {close}, {volume})
            """
            cur.execute(insert_sql)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Rolled back:", e)
        raise
    finally:
        cur.close()
        conn.close()


@task
def check_records(conn):
    conn = return_snowflake_conn()
    target_table = f"{database}.raw.stock_price"
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        total = cur.fetchone()[1]
        print("Total number of rows:", total)
        return total
    finally:
        cur.close()
        conn.close()



with DAG(
    dag_id="Stock_price_pipeline",
    schedule=" 0 1 * * *",
    start_date=datetime(2025, 10, 4),
    catchup=False,
    tags=["stocks", "snowflake"]
) as dag:

    symbol = "TSCO.LON"
    prices = return_last_90d_price(symbol)
    load = load_data(prices)
    check = check_records()

    prices >> load >> check
