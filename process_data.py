import pandas as pd
from fetch_data import fetch_data
import argparse

def process_data(job_title=None, min_salary=None):
    # Fetch filtered data
    records = fetch_data(job_title=job_title, min_salary=min_salary)
    df = pd.DataFrame(records)

    if df.empty:
        print("No records found for the given filters.")
        return []

    latest_year = df['year'].max()
    df_latest = df[df['year'] == latest_year].copy()

    # Bonus calculation
    def calculate_bonus(salary):
        if salary < 50000:
            return salary * 0.15
        elif salary <= 100000:
            return salary * 0.10
        else:
            return salary * 0.05

    df_latest['bonus'] = df_latest['salary_in_usd'].apply(calculate_bonus)
    df_latest['total_compensation'] = df_latest['salary_in_usd'] + df_latest['bonus']
    df_latest['tax'] = df_latest['total_compensation'] * 0.10
    df_latest['net_pay'] = df_latest['total_compensation'] - df_latest['tax']

    top_10_total_comp = df_latest.nlargest(10, 'total_compensation')
    avg_bonus_per_job = df_latest.groupby('job_title')['bonus'].mean()

    print(f"Latest year: {latest_year}")
    print("Top 10 employees by total compensation:\n",
          top_10_total_comp[['job_title', 'level', 'total_compensation']])
    print("Average bonus per job title:\n", avg_bonus_per_job)

    return df_latest.to_dict(orient='records')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_title", type=str)
    parser.add_argument("--min_salary", type=float)
    args = parser.parse_args()
    process_data(job_title=args.job_title, min_salary=args.min_salary)
