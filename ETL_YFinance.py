from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta, datetime
import snowflake.connector
import yfinance as yf


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn


@task
def fetch_all_history(symbol):
    
    t = yf.Ticker(symbol)
    df = t.history(period="1mo", interval="1d", auto_adjust=False)

    if df.empty:
        return []

    df = df.reset_index()
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )[["date", "open", "high", "low", "close", "volume"]].dropna()

    records = []
    for row in df.itertuples(index=False):
        records.append(
            {
                "date": row.date.strftime("%Y-%m-%d"),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": int(row.volume),
            }
        )
    return records


@task
def load_data(records, symbol, database="PRATHMESH_1"):
    if not records:
        print(f"[{symbol}] No records to load")
        return

    conn = return_snowflake_conn()
    cur = conn.cursor()
    fq_table = f'{database}.RAW.YFINANCE_DAILY_PRICES'
    try:
        cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        acct, role, wh, db, sch = cur.fetchone()
        print(f"SF Context -> acct={acct}, role={role}, wh={wh}, db={db}, schema={sch}")
        print(f"Target table -> {fq_table}")

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.RAW;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {fq_table} (
                symbol VARCHAR(18) NOT NULL,
                date   DATE         NOT NULL,
                open   NUMBER(14,6) NOT NULL,
                close  NUMBER(14,6) NOT NULL,
                high   NUMBER(14,6) NOT NULL,
                low    NUMBER(14,6) NOT NULL,
                volume NUMBER(20,0) NOT NULL,
                PRIMARY KEY (symbol, date)
            );
        """)

        cur.execute(f"DELETE FROM {fq_table} WHERE symbol = %(symbol)s", {"symbol": symbol})

        insert_sql = f"""
            INSERT INTO {fq_table} (symbol, date, open, high, low, close, volume)
            VALUES (%(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        """
        rows = [{
            "symbol": symbol,
            "date": r["date"],
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
        } for r in records]

        cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"[{symbol}] Inserted {len(rows)} rows into {fq_table}")
    except Exception as e:
        conn.rollback()
        print(f"[{symbol}] Rolled back: {e}")
        raise
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="ETL_yfinance_pipeline",
    schedule_interval="0 3 * * *",
    start_date=datetime(2025, 10, 3),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["yfinance"],
    
) as dag:

    symbols = ["TSCO.L", "AAPL"]

    for sym in symbols:
        prices = fetch_all_history(sym)
        load = load_data(prices, sym)
        prices >> load
