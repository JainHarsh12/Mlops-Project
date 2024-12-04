import os
import pandas as pd
from sklearn.model_selection import train_test_split
from src.nik.utils import MySQLUtils  # Use the MySQLUtils class
from datetime import datetime

class DataIngestion:
    def __init__(self, output_dir="artifacts/raw_data"):
        """Initialize data ingestion with the output directory."""
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.mysql_utils = MySQLUtils()  # MySQL utility instance

    def fetch_data(self):
        """Fetch data from MySQL."""
        print("Fetching data from MySQL...")
        data = self.mysql_utils.fetch_artifacts()  # Fetch data from MySQL
        if not data:
            raise Exception("No data found in the database!")
        print(f"Fetched {len(data)} records from MySQL.")
        return pd.DataFrame(data)  # Convert to DataFrame

    def preprocess_data(self, df):
        """Preprocess raw data for machine learning."""
        print("Preprocessing data...")
        # Convert JSON-like fields into usable data
        df['metrics'] = df['metrics'].apply(eval)  # Convert metrics to dict
        df['hyperparameters'] = df['hyperparameters'].apply(eval)  # Convert hyperparameters to dict

        # Example: Extract specific columns for training
        df['accuracy'] = df['metrics'].apply(lambda x: x.get('accuracy', 0))
        df['learning_rate'] = df['hyperparameters'].apply(lambda x: x.get('learning_rate', 0))
        df['epochs'] = df['hyperparameters'].apply(lambda x: x.get('epochs', 0))
        
        # Drop unnecessary columns if needed
        df = df[['model_name', 'version', 'accuracy', 'learning_rate', 'epochs']]
        print("Preprocessing completed.")
        return df

    def split_data(self, df):
        """Split data into training and testing sets."""
        print("Splitting data into train and test sets...")
        X = df[['accuracy', 'learning_rate', 'epochs']]  # Features
        y = df['model_name']  # Labels
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        print("Train-test split completed.")
        return X_train, X_test, y_train, y_test

    def save_data(self, X_train, X_test, y_train, y_test):
        """Save the train and test datasets."""
        train_file_path = os.path.join(self.output_dir, f"train_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        test_file_path = os.path.join(self.output_dir, f"test_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

        # Combine features and labels for saving
        train_data = pd.concat([X_train, y_train], axis=1)
        test_data = pd.concat([X_test, y_test], axis=1)

        train_data.to_csv(train_file_path, index=False)
        test_data.to_csv(test_file_path, index=False)

        print(f"Training data saved to {train_file_path}")
        print(f"Testing data saved to {test_file_path}")
        return train_file_path, test_file_path

    def run(self):
        """Orchestrate data ingestion."""
        # Fetch and preprocess data
        df = self.fetch_data()
        df = self.preprocess_data(df)

        # Perform train-test split and save the datasets
        X_train, X_test, y_train, y_test = self.split_data(df)
        train_file, test_file = self.save_data(X_train, X_test, y_train, y_test)
        return train_file, test_file
