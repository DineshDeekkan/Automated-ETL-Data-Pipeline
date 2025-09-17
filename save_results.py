from airflow.providers.postgres.hooks.postgres import PostgresHook

def save_results(processed_rows):
    if not processed_rows:
        return
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
    insert_sql = """
        INSERT INTO employee_summary
          (job_title, level, type, mode, year, residence, salary, currency, salary_in_usd, location, salary_grade, bonus, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING;
    """
    for row in processed_rows:
        cur.execute(insert_sql, (
            row["job_title"], row["level"], row["type"], row["mode"], row["year"], row["residence"],
            row["salary"], row["currency"], row["salary_in_usd"], row["location"],
            row["salary_grade"], row["bonus"]
        ))
    conn.commit()
    cur.close()
    conn.close()
    print("Saved processed data successfully!")

# For manual testing
if __name__ == "__main__":
    import fetch_data
    import process_data
    rows = fetch_data.fetch_data()
    processed = process_data.process_data(rows)
    save_results(processed)
