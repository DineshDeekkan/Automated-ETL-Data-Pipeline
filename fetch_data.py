import psycopg2
import pandas as pd
import argparse

def fetch_data(job_title=None, min_salary=None):
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="cdac",
            user="dinesh",
            password="july",
            port=5432
        )

        sql = "SELECT * FROM employees WHERE 1=1"
        params = []

        if job_title:
            sql += " AND job_title = %s"
            params.append(job_title)
        if min_salary:
            sql += " AND salary_in_usd > %s"
            params.append(min_salary)

        df = pd.read_sql(sql, conn, params=tuple(params))
        print("Fetched data successfully!")
        print(df.head())

    except Exception as e:
        print("Error:", e)
        return []
    finally:
        conn.close()

    return df.to_dict(orient="records")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_title", type=str)
    parser.add_argument("--min_salary", type=float)
    args = parser.parse_args()

    records = fetch_data(job_title=args.job_title, min_salary=args.min_salary)
    print(records)
