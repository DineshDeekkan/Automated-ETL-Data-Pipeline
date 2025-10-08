import psycopg2
import pandas as pd
from process_data import process_data
import argparse

def save_results(job_title=None, min_salary=None):
    # Step 1: Get processed data with filters
    records = process_data(job_title=job_title, min_salary=min_salary)
    df = pd.DataFrame(records)

    if df.empty:
        print("No records found for the given filters.")
        return

    # Step 2: Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="cdac",
        user="dinesh",
        password="july",
        port=5432
    )
    cursor = conn.cursor()

    # Step 3: Clear previous run’s data
    cursor.execute("TRUNCATE TABLE employees_bonus;")
    conn.commit()

    # Step 4: Insert processed data
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO employees_bonus (
                job_title, level, type, mode, year, residence, salary, currency, salary_in_usd, location,
                bonus, total_compensation, tax, net_pay
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            row['job_title'], row['level'], row['type'], row['mode'], row['year'], row['residence'],
            row['salary'], row['currency'], row['salary_in_usd'], row['location'],
            row['bonus'], row['total_compensation'], row['tax'], row['net_pay']
        ))
    conn.commit()
    print("Processed data saved successfully in employees_bonus table.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_title", type=str)
    parser.add_argument("--min_salary", type=float)
    args = parser.parse_args()

    save_results(job_title=args.job_title, min_salary=args.min_salary)
            
