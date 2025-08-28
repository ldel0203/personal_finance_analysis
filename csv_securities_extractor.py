import linecache
import logging
import pandas as pd
import re

class CsvSecuritiesExtractor:
    def __init__(self, file_path: str, sep=";", encoding="utf-8"):
        self.file_path = file_path
        self.sep = sep
        self.encoding = encoding
        
    def extract_account_id(self):
        account_id = None
        tmp_account_id = linecache.getline(self.file_path,3).strip()
        match = re.search(r"\d{11}", tmp_account_id)
        if match:
            account_id = match.group()
        else:
            logging.error("ERROR : No account_id found")
            raise ValueError("No account_id found in file")
        
        return account_id

    def extract_securities(self):
        df = pd.read_csv(self.file_path, sep=self.sep, encoding=self.encoding, skiprows=4, usecols=lambda col: not col.startswith("Unnamed"))
        
        df["account_id"] = self.extract_account_id()
        
        logging.info(f"CSV file loaded: {self.file_path}")
        return df