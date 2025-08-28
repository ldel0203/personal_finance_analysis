from extract.ofx_extractor import OfxExtractor
from transform.ofx_transformer import OfxTransformer
from load.mysql_loader import MySQLLoader
import logging
from pathlib import Path

class OfxPipeline:
    def __init__(self, file_path:str, db_config:dict):
        self.file_path = file_path
        self.file_name = Path(file_path).name
        self.db_config = db_config
        
    def run(self):
        #1. Extract data
        logging.info(f"Extracting data from {self.file_name}")
        exctractor = OfxExtractor(self.file_path)
        raw_data = exctractor.extract_all()
        
        #2. Transform data
        logging.info(f"Transforming data from {self.file_name}")
        transformer = OfxTransformer(raw_data, self.db_config)
        clean_data = transformer.transform_all()
        
        #3. Load data
        logging.info(f"Loading data into MySQL from {self.file_name}")
        loader = MySQLLoader(
            self.db_config,
            clean_data
        )

        loader.load_all()
        
        logging.info(f"Data loaded from {self.file_name}")