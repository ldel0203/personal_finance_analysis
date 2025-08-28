import logging
import pandas as pd
import re
from extract.mysql_extractor import MySQLExtractor

class OfxTransformer:
    def __init__(self, raw_data: dict, db_config: dict):
        self.raw_data_accounts = raw_data["accounts"]
        self.raw_data_transactions = raw_data["transactions"]
        self.db_config = db_config
        self.db_payees = []
        
    def clean_payee(self, payee: str) -> str:
        if not isinstance(payee, str):
            return payee
        
        if self.db_payees == []:
            db_extractor = MySQLExtractor(self.db_config)
            self.db_payees = db_extractor.get_clean_payees()
        
        payee = payee.upper()
        
        # reduce multiple spaces
        payee = re.sub(r"\s{2,}", " ", payee)
        
        # check if ther already is a clean payee in database
        for db_payee in self.db_payees:
            if db_payee in payee:
                return db_payee
        
        
        # if there is no clean payee in database, clean it
        # clean X0000
        payee = re.sub(r"^[A-Z]\d{4}\s+", "", payee)
        
        # clean date
        payee = re.sub(r"(\s+\d{2}/\d{2}|\s+\d{2}H\d{2})+$", "", payee)
        
        # clean unnecessary prefixes at the begenning
        payee = re.sub(r"^(VIR INST\s+|WEB\s+|DE\s+|VERS\s+|MLLE.\s+|MLLE\s+|MR.\s+|MR\s+|M.\s+|M\s+|M.OU\s+|OU\s+|MME\s+|ET\s+)+", "", payee)
        
        # clean start and end spaces
        payee = payee.strip()
        
        return payee
        
    
    def transform_account_types(self) -> pd.DataFrame:
        account_types = self.raw_data_accounts[["account_type"]].drop_duplicates().copy()
        
        account_types["is_checking_account"] = account_types["account_type"].apply(lambda x: 1 if "checking" in str(x).lower() else 0)
        
        return account_types
        
    def transform_accounts(self) -> pd.DataFrame:
        accounts = self.raw_data_accounts[["routing_number", "account_id", "account_type", "currency"]].drop_duplicates().copy()
        accounts["name"] = "Undefined"
        return accounts

    def transform_balances(self) -> pd.DataFrame:
        balances = self.raw_data_accounts[["account_id", "balance", "date"]].drop_duplicates().copy()
        balances["date"] = pd.to_datetime(balances["date"])
        
        balances = balances.sort_values("date").groupby("account_id").tail(1)
        return balances
    
    def transform_banks(self) -> pd.DataFrame:
        banks = self.raw_data_accounts[["routing_number"]].drop_duplicates().copy()
        banks["name"] = "Undefined"
        return banks
    
    def transform_currency(self) -> pd.DataFrame:
        currency = self.raw_data_accounts[["currency"]].drop_duplicates().copy()
        currency["name"] = "Undefined"
        currency["symbol"] = "-"
        return currency
    
    def transform_transactions(self) -> pd.DataFrame:
        transactions = self.raw_data_transactions[["account_id", "date", "payee", "memo", "amount", "transaction_id"]].copy()
        
        #clean payee
        transactions["clean_payee"] = transactions["payee"].apply(self.clean_payee)
        
        # is_expense column
        transactions["is_expense"] = transactions["amount"].apply(lambda x: 0 if x>0 else 1)
        
        # absolue amount
        transactions["amount"] = transactions["amount"].apply(abs)
        
        return transactions
    
    def transform_all(self) -> dict:
        account_types = self.transform_account_types().rename(columns={
            "account_type"      : "name"
        })
        
        accounts = self.transform_accounts().rename(columns={
            "routing_number"    : "bank_id",
            "account_id"        : "id",
            "account_type"      : "account_type_name",
            "currency"          : "currency_abbreviation"
        })
        
        balances = self.transform_balances().rename(columns={
            "balance"   : "value"
        })
        
        banks = self.transform_banks().rename(columns={
            "routing_number"    : "id"
        })
        
        currency = self.transform_currency().rename(columns={
            "currency"  : "abbreviation"
        })
        
        transactions = self.transform_transactions().rename(columns={
            "transaction_id"    : "id"
        })
        
        return {
            "account_types" : account_types,
            "accounts"      : accounts,
            "balances"      : balances,
            "banks"         : banks,
            "currency"      : currency,
            "transactions"  : transactions
        }
        