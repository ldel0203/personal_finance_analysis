import logging
import pandas as pd
import re

class CsvSecuritiesTransformer:
    def __init__(self, raw_data: pd.DataFrame):
        self.raw_data = raw_data
        
    def clean_amounts(self, amount:str):
        
        match = re.search(r"[\d,]+", amount)
        if match:
            amount = match.group().replace(",", ".")
            amount = float(amount)
            
        return amount
        
    def transform_accounts(self) -> pd.DataFrame:
        accounts = self.raw_data[["account_id"]].drop_duplicates().copy()
        
        accounts["bank_id"] = 0
        accounts["account_type_name"] = "INVESTMENT"
        accounts["currency_abbreviation"] = "EUR"
        accounts["name"] = "Undefined"
        
        return accounts
    
    def transform_securities(self) -> pd.DataFrame:
        securities = self.raw_data[["Valeur", "ISIN"]].drop_duplicates().copy()
        securities = securities[securities["ISIN"].notna() & (securities["ISIN"] != "")]
        
        securities["currency_abbr"] = "EUR"
        securities["ticker"] = "Undefined"
        securities["type"] = "Undefined"
        
        return securities
    
    def transform_security_operations(self) -> pd.DataFrame:
        operations = self.raw_data[["Opération", "ISIN", "Quantité", "Montant Net", "Frais", "Date", "account_id"]].copy()
        operations = operations[operations["ISIN"].notna() & (operations["ISIN"] != "")]
        
        operations["operation_type"] = operations["Opération"].apply(
            lambda x: "purchase" if "ACHAT" in x.upper()
            else "sale" if "VENTE" in x.upper()
            else "tax"
        )
        
        operations["Montant Net"] = operations["Montant Net"].apply(self.clean_amounts)
        operations["Frais"] = operations["Frais"].apply(self.clean_amounts)
        
        operations["gross_amount"] = operations.apply(
            lambda row: row["Montant Net"] + row["Frais"]
            if row["operation_type"] == "sale"
            else row["Montant Net"] - row["Frais"],
            axis=1
        )
        
        operations["gross_unit_price"] = round(operations["gross_amount"]/operations["Quantité"], 4)
        operations["net_unit_price"] = round(operations["Montant Net"]/operations["Quantité"], 4)
        
        
        return operations.drop(columns="Opération")
    
    def transform_all(self):
        accounts = self.transform_accounts().rename(columns={
            "account_id" : "id"
        })
        
        securities = self.transform_securities().rename(columns={
            "ISIN"      : "isin",
            "Valeur"    : "name"
        })
        
        security_operations = self.transform_security_operations().rename(columns={
            "ISIN"      : "isin",
            "Quantité"  : "quantity",
            "Montant Net"   : "net_amount",
            "Frais"     : "fees",
            "Date"      : "date"
        })
        
        return {
            "accounts"  : accounts,
            "securities": securities,
            "security_operations": security_operations
        }