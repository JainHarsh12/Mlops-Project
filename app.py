from src.nik.logger import logging
from src.nik.exception import CustomException
from src.nik.components.data_ingestion import DataIngestion
import sys

if __name__ == "__main__":
    logging.info("The execution has started.")
    
    try:
        # Initialize DataIngestion object
        ingestion = DataIngestion()
        
        # Trigger data ingestion and save train-test datasets
        train_file_path, test_file_path = ingestion.run()
        
        logging.info(f"Data ingestion completed.")
        logging.info(f"Training data saved at: {train_file_path}")
        logging.info(f"Testing data saved at: {test_file_path}")

    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")
        raise CustomException(e, sys)
