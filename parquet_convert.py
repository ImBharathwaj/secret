import pandas as pd
from functools import reduce
import glob

# Path pattern to your parquet files
file_pattern = 'cluster_emails_*.parquet'

# Get list of all parquet files matching the pattern
files = glob.glob(file_pattern)[::-1]
print(files)
col_name = []
for i in files:
    quarter = (int(i[19:21]) - 1) // 3 + 1
    col_name.append(f"Q{quarter}_{i[15:19]}")

# Read all parquet files into a list of DataFrames
dataframes = []
for i, file in enumerate(files, start=0):
    df = pd.read_parquet(file)
    # Rename 'cluster_id' to 'cluster_{i}'
    df = df.rename(columns={'Cluster': f'{col_name[i]}'})
    dataframes.append(df)

# Merge all DataFrames on 'email'
merged_df = reduce(lambda left, right: pd.merge(left, right, on='Email', how='inner'), dataframes)

cols = ['Email'] + [col for col in merged_df.columns if col != 'Email']
merged_df = merged_df[cols]

# Save or display
merged_df.to_parquet('combined_with_email.parquet')
print(merged_df)