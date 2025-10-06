from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def train(cur, train_input_table, train_view, forecast_function_name):
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS
        SELECT
            CAST(date AS DATE) AS DATE,
            CAST(close AS NUMBER(14,6)) AS CLOSE,
            CAST(symbol AS STRING) AS SYMBOL
        FROM {train_input_table}
        WHERE date IS NOT NULL AND close IS NOT NULL AND symbol IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY date) = 1
        ORDER BY SYMBOL, DATE;
    """

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA         => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME     => 'SYMBOL',
        TIMESTAMP_COLNAME  => 'DATE',
        TARGET_COLNAME     => 'CLOSE',
        CONFIG_OBJECT      => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    cur.execute(create_view_sql)
    cur.execute(create_model_sql)
    cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT       => {{ 'prediction_interval': 0.95 }}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table}
        AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT
            SYMBOL,
            DATE,
            CLOSE AS ACTUAL,
            CAST(NULL AS NUMBER(14,6)) AS FORECAST,
            CAST(NULL AS NUMBER(14,6)) AS LOWER_BOUND,
            CAST(NULL AS NUMBER(14,6)) AS UPPER_BOUND
        FROM {train_input_table}
        UNION ALL
        SELECT
            REPLACE(SERIES, '\"', '') AS SYMBOL,
            TS AS DATE,
            CAST(NULL AS NUMBER(14,6)) AS ACTUAL,
            FORECAST,
            LOWER_BOUND,
            UPPER_BOUND
        FROM {forecast_table};"""

    cur.execute(make_prediction_sql)
    cur.execute(create_final_table_sql)


with DAG(
    dag_id="YFinance_Forecast",
    start_date=datetime(2024, 10, 3),
    schedule_interval='30 3 * * *',
    catchup=False,
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=['Forecast', 'yfinance'],
) as dag:

    train_input_table     = "PRATHMESH_1.RAW.YFINANCE_DAILY_PRICES"
    train_view            = "PRATHMESH_1.RAW.YFINANCE_VIEW"
    forecast_function_name= "PRATHMESH_1.RAW.PREDICT_STOCK_PRICE"
    forecast_table        = "PRATHMESH_1.ANALYTICS.YFINANCE_FORECAST"
    final_table           = "PRATHMESH_1.ANALYTICS.YFINANCE_DAILY_PRICES_ALL"

    cur = return_snowflake_conn()

    train_task   = train(cur, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)

    train_task >> predict_task
