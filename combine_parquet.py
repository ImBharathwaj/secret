import pandas as pd
import glob

# Path to your parquet files
parquet_files = glob.glob('*.parquet')

# Read all files into a list of DataFrames
dfs = [pd.read_parquet(f) for f in parquet_files]

# Concatenate all DataFrames
combined_df = pd.concat(dfs, ignore_index=True)

# Create the 'email' column
# For example, if 'email1' and 'email2' exist:
combined_df['email'] = combined_df['Cluster'].combine_first(combined_df['Cluster'])

print(combined_df)
# Select only the required columns with new names
# result_df = combined_df[['email', 'Cluster']].copy()

# # Save to a new Parquet file
combined_df.to_parquet('combined_with_email.parquet')