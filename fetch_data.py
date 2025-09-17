from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def fetch_data():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    sql = "SELECT * FROM employees;"
    df = hook.get_pandas_df(sql)
    print("Fetched data successfully!")
    print(df.head())
    return df.to_dict(orient="records")

# For manual testing
if __name__ == "__main__":
    fetch_data()
