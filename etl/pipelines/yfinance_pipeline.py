from extract.yfinance_extractor import YFinanceExtractor
from extract.mysql_extractor import MySQLExtractor
from load.mysql_loader import MySQLLoader
import logging
from pathlib import Path

class YfinancePipeline:
    def __init__(self, db_config:dict):
        self.db_config = db_config

    def run(self):
        #1. Extract data from DB
        db_extractor = MySQLExtractor(self.db_config)
        tickers = db_extractor.get_securities_to_update()
        security_prices_date = db_extractor.get_securities_price_import_dates()

        #2. Extract data from yfinance
        y_extractor = YFinanceExtractor()
        data = {}
        if tickers != []:
            securities_info = y_extractor.extract_securities_info(tickers)
            data["securities_info"] = securities_info
        else:
            logging.info("No data to update in securities table")
        if not security_prices_date.empty:
            security_prices = y_extractor.extract_security_prices(security_prices_date)
            data["security_prices"] = security_prices
        else:
            logging.info("No data to add in security_prices table")


        #3. Load data
        loader = MySQLLoader(self.db_config, data)

        loader.load_all()
