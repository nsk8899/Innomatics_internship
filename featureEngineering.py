interactions_df = pd.read_csv('property_interactions_data.csv')
interactions_count = interactions_df.groupby('property_id').size().reset_index(name='total_interactions')
merged_df = merged_df.merge(interactions_count, on='property_id', how='left')
merged_df['total_interactions'].fillna(0, inplace=True)