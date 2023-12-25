class DataCleaning:
    def __init__(self, data):


class DataCleaning:
    @staticmethod
    def clean_user_data(df):
        # Your cleaning logic goes here
        # Example: Drop rows with NULL values
        df_cleaned = df.dropna()
        return df_cleaned