import pandas as pd

# Read the parquet file
df = pd.read_parquet('cluster_emails_20250618.parquet')

# Replace 0 with 4 in the 'Cluster' column
df['Cluster'] = df['Cluster'].replace('0', '4')

# Save the updated DataFrame back to parquet
df.to_parquet('cluster_emails_20250618.parquet', index=False)
print(df)