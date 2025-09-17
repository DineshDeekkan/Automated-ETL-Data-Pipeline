import pandas as pd

def process_data(rows):
    if not rows:
        return []
    df = pd.DataFrame(rows)
    df["salary_grade"] = df["salary_in_usd"].apply(
        lambda s: "High" if s >= 100000 else "Mid" if s >= 50000 else "Low"
    )
    df["bonus"] = df["salary_in_usd"] * 0.10
    print("Processed data successfully!")
    print(df.head())
    return df.to_dict(orient="records")

# For manual testing
if __name__ == "__main__":
    import fetch_data
    rows = fetch_data.fetch_data()
    process_data(rows)
