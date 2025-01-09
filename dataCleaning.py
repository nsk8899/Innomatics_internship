import pandas as pd
import glob

# List all CSV files
files = glob.glob('property_data_location_*.csv')
# Read and concatenate files
df_list = [pd.read_csv(file) for file in files]
merged_df = pd.concat(df_list, ignore_index=True)
# Save the merged data
merged_df.to_csv('merged_property_data.csv', index=False)