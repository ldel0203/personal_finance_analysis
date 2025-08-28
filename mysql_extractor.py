import pandas as pd
from sqlalchemy import create_engine
import logging

class MySQLExtractor:
    def __init__(self, db_config: dict):
        self.user = db_config["user"]
        self.password = db_config["password"]
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.database = db_config["database"]
        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    def extract_query(self, query: str):
        try:
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            logging.error(f"Failed to execute query - {e}")
            raise

    def get_clean_payees(self):
        query = "SELECT DISTINCT payee FROM categories_transaction_link ORDER BY LENGTH(payee) DESC;"
        payees = self.extract_query(query)
        
        return payees["payee"].to_list()

    def get_securities_to_update(self):
        query = "SELECT ticker FROM securities WHERE ticker <> 'Undefined' AND (type = 'Undefined' OR market IS NULL)"
        tickers = self.extract_query(query)
        
        return tickers["ticker"].to_list()
    
    def get_securities_price_import_dates(self) -> pd.DataFrame:
        query = """
        WITH purchase_securities AS (
            SELECT  s.isin, 
                    s.ticker,
                    MIN(so.date) AS first_purchase_date,
                    SUM(so.quantity) AS total_purchased
            FROM securities s
            LEFT JOIN security_operations so ON s.isin = so.isin
            WHERE operation_type = 'purchase' AND ticker <> 'Undefined'
            GROUP BY s.isin, s.ticker
        ),
        sale_securities AS (
            SELECT  s.isin, 
                    s.ticker,
                    MAX(so.date) AS last_sale_date,
                    SUM(so.quantity) AS total_saled
            FROM securities s
            LEFT JOIN security_operations so ON s.isin = so.isin
            WHERE operation_type = 'sale' AND ticker <> 'Undefined'
            GROUP BY s.isin, s.ticker
        ),
        import_span_dates AS (
            SELECT  ps.isin, 
                    ps.ticker, 
                    ps.first_purchase_date AS start_import_date,
                    CASE
                        WHEN ps.total_purchased = COALESCE(ss.total_saled, 0) 
                            THEN ss.last_sale_date
                        ELSE CURDATE()
                    END AS end_import_date
            FROM purchase_securities ps
            LEFT JOIN sale_securities ss ON ps.isin = ss.isin
        ),
        last_import_dates AS (
            SELECT isin, MAX(date) AS last_import_date
            FROM security_prices
            GROUP BY isin
        )
        SELECT  isd.isin,
                isd.ticker,
                GREATEST(isd.start_import_date, COALESCE(lid.last_import_date, DATE('1900-01-01'))) AS start_import_date,
                ADDDATE(isd.end_import_date, INTERVAL 1 DAY) AS end_import_date
        FROM import_span_dates isd
        LEFT JOIN last_import_dates lid ON isd.isin = lid.isin
        WHERE isd.end_import_date > COALESCE(lid.last_import_date, DATE('1900-01-01')) AND isd.ticker IS NOT NULL;
        """
        return self.extract_query(query)