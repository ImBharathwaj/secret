import pandas as pd

# Specify the path to your Parquet file
file_path = 'cluster_emails_20250618.parquet'

# Read the Parquet file into a DataFrame
df = pd.read_parquet(file_path)

# Now you can work with the DataFrame
print(df)