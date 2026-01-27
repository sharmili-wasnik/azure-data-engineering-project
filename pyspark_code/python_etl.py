import pandas as pd

df = pd.read_csv("../data/sales_data.csv")

df["total_amount"] = df["price"] * df["quantity"]

df = df.drop_duplicates()

df.to_csv("../data/cleaned_sales_data.csv", index=False)

print("ETL Process Completed Successfully")
