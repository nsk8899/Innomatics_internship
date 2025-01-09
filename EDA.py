merged_df['rent'].hist(bins=50)
corr_matrix = merged_df.corr()
sns.heatmap(corr_matrix, annot=True)