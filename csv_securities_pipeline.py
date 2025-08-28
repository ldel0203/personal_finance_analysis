from extract.csv_securities_extractor import CsvSecuritiesExtractor
from transform.csv_securities_transformer import CsvSecuritiesTransformer
from load.mysql_loader import MySQLLoader
import logging
from pathlib import Path

class CsvSecuritiesPipeline:
    def __init__(self, file_path:str, db_config:dict):
        self.file_path = file_path
        self.file_name = Path(file_path).name
        self.db_config = db_config
        
    def run(self):
        #1. Extract data
        logging.info(f"Extracting data from {self.file_name}")
        extractor = CsvSecuritiesExtractor(self.file_path)
        raw_data = extractor.extract_securities()
        
        #2. Transform data
        logging.info(f"Transforming data from {self.file_name}")
        transformer = CsvSecuritiesTransformer(raw_data)
        clean_data = transformer.transform_all()
        
        #3. Load data
        logging.info(f"Loading data into MySQL from {self.file_name}")
        loader = MySQLLoader(
            self.db_config,
            clean_data
        )

        loader.load_all()
        
        logging.info(f"Data loaded from {self.file_name}")