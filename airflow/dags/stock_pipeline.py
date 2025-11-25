# airflow/dags/stock_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extract import fetch_daily
from src.validate import validate_df
from src.transform import feature_engineer
from src.load import write_to_db
import pandas as pd

default_args = {"owner":"you","depends_on_past":False,"retries":1,"retry_delay": timedelta(minutes=5)}
with DAG(dag_id="stock_etl_daily", start_date=datetime(2025,1,1), schedule_interval="0 2 * * *", default_args=default_args, catchup=False) as dag:

    def extract(**kwargs):
        symbols = ["AAPL","MSFT"]
        out = {}
        for s in symbols:
            df = fetch_daily(s)
            out[s] = df
        kwargs['ti'].xcom_push(key='raw', value={k:v.to_dict(orient='records') for k,v in out.items()})

    def validate(**kwargs):
        raw = kwargs['ti'].xcom_pull(key='raw')
        for s, rows in raw.items():
            df = pd.DataFrame(rows)
            res = validate_df(df)
            if not res["success"]:
                raise ValueError(f"Validation failed for {s}")

    def transform_load(**kwargs):
        raw = kwargs['ti'].xcom_pull(key='raw')
        for s, rows in raw.items():
            df = pd.DataFrame(rows)
            df = feature_engineer(df)
            write_to_db(df)

    t1 = PythonOperator(task_id='extract', python_callable=extract, provide_context=True)
    t2 = PythonOperator(task_id='validate', python_callable=validate, provide_context=True)
    t3 = PythonOperator(task_id='transform_load', python_callable=transform_load, provide_context=True)

    t1 >> t2 >> t3