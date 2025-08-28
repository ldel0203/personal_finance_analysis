import logging
from ofxparse import OfxParser
import pandas as pd
from pathlib import Path

class OfxExtractor:
    def __init__(self, file_path : str):
        
        self.file_path = Path(file_path)
        
        if not Path.exists(self.file_path):
            logging.error(f"File not found : {file_path}")
            raise FileNotFoundError("File not found")
        if self.file_path.suffix.lower() != ".ofx":
            logging.error(f"Invalid file type : {file_path}")
            raise ValueError("File must be an 'ofx' file")
        
        
        self.file_directory = self.file_path.parent
        self.file_name = self.file_path.name 
        
    def extract_transactions(self) -> pd.DataFrame:
        with open(self.file_path, 'r', encoding='utf-8') as file:
            ofx = OfxParser.parse(file)
            
        data = []
        for account in ofx.accounts:
            for transaction in account.statement.transactions:
                data.append({
                    "routing_number"    : account.routing_number,
                    "account_id"        : account.account_id,
                    "account_type"      : account.account_type,
                    "currency"          : account.curdef,
                    "balance"           : account.statement.balance,
                    "date"              : transaction.date,
                    "payee"             : transaction.payee,
                    "memo"              : transaction.memo,
                    "amount"            : transaction.amount,
                    "transaction_id"    : transaction.id
                })
                
        return pd.DataFrame(data)
    
    def extract_accounts(self) -> pd.DataFrame:
        with open(self.file_path, 'r', encoding='utf-8') as file:
            ofx = OfxParser.parse(file)
            
        data = []
        for account in ofx.accounts:
            data.append({
                "routing_number"    : account.routing_number,
                "account_id"        : account.account_id,
                "account_type"      : account.account_type,
                "currency"          : account.curdef,
                "balance"           : account.statement.balance,
                "date"              : ofx.signon.dtserver
            })
                
        return pd.DataFrame(data)
    
    def extract_all(self) -> dict:
        data = {
            "accounts"      : self.extract_accounts(),
            "transactions"  : self.extract_transactions()
        }
        
        return data
