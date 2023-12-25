class DataCleaning:
    def __init__(self, file_path):
        self.credentials = self.read_db_creds(file_path)
        
    @staticmethod
    def clean_user_data(df):
        # Your cleaning logic goes here
        # Example: Drop rows with NULL values
        df_cleaned = df.dropna()
        return df_cleaned