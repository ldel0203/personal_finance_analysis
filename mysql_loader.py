import logging
from sqlalchemy import create_engine, text
import pandas as pd

class MySQLLoader:
    def __init__(self, db_config: dict, df: dict):
        self.user = db_config["user"]
        self.password = db_config["password"]
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.database = db_config["database"]
        self.df = df
        
        # Create SQLAlchemy engine
        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            echo=False,  # set to True for SQL debug
            future=True
        )
    
    def load_account_types(self):
        try:
            df = self.df["account_types"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for account_types table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_account_types (
                    name VARCHAR(45) NOT NULL,
                    is_checking_account BOOLEAN NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_account_types", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO account_type (name, is_checking_account)
                SELECT tmp.name, tmp.is_checking_account
                FROM tmp_account_types tmp
                LEFT JOIN account_type act ON LOWER(tmp.name) = LOWER(act.name)
                WHERE act.name IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to account_type")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to account_type - {e}")
    
    def load_accounts(self):
        try:
            df = self.df["accounts"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for accounts table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_accounts (
                    id BIGINT NOT NULL,
                    name VARCHAR(128) NOT NULL,
                    account_type_name VARCHAR(45) NOT NULL,
                    currency_abbreviation VARCHAR(4) NOT NULL,
                    bank_id INT NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_accounts", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO accounts (id, name, account_type_id, currency_id, bank_id)
                SELECT 
                    tmp.id, 
                    tmp.name, 
                    at.id,
                    c.id,
                    tmp.bank_id
                FROM tmp_accounts tmp
                LEFT JOIN account_type at   ON LOWER(tmp.account_type_name) = LOWER(at.name)
                LEFT JOIN currency c        ON LOWER(tmp.currency_abbreviation) = LOWER(c.abbreviation)
                LEFT JOIN accounts act      ON tmp.id = act.id
                WHERE act.name IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to accounts")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to accounts - {e}")
    
    def load_balances(self):
        try:
            df = self.df["balances"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for balances table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_balances (
                    account_id BIGINT NOT NULL,
                    date DATE NOT NULL,
                    value DECIMAL(10,2) NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_balances", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO balances (account_id, date, value)
                SELECT tmp.account_id, tmp.date, tmp.value
                FROM tmp_balances tmp
                LEFT JOIN balances act ON tmp.account_id = act.account_id AND tmp.date = act.date
                WHERE act.account_id IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to balances")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to balances - {e}")
    
    def load_banks(self):
        try:
            df = self.df["banks"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for banks table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_banks (
                    id INT,
                    name VARCHAR(45) NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_banks", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO banks (id, name)
                SELECT tmp.id, tmp.name
                FROM tmp_banks tmp
                LEFT JOIN banks act ON tmp.id = act.id
                WHERE act.id IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to banks")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to banks - {e}")
    
    def load_currency(self):
        try:
            df = self.df["currency"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for currency table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_currency (
                    name VARCHAR(16) NOT NULL,
                    abbreviation VARCHAR(4) NOT NULL,
                    symbol VARCHAR(4) NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_currency", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO currency (name, abbreviation, symbol)
                SELECT tmp.name, tmp.abbreviation, tmp.symbol
                FROM tmp_currency tmp
                LEFT JOIN currency act ON LOWER(tmp.abbreviation) = LOWER(act.abbreviation)
                WHERE act.id IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to currency")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to currency - {e}")
    
    def load_securities(self):
        try:
            df = self.df["securities"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for securities table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_securities (
                    isin VARCHAR(12) PRIMARY KEY,
                    ticker VARCHAR(32) NOT NULL,
                    name VARCHAR(128) NOT NULL,
                    type VARCHAR(32) NOT NULL,
                    currency_abbr VARCHAR(4) NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_securities", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO securities (isin, ticker, name, type, currency_id)
                SELECT tmp.isin, tmp.ticker, tmp.name, tmp.type, c.id
                FROM tmp_securities tmp
                LEFT JOIN currency c ON LOWER(tmp.currency_abbr) = LOWER(c.abbreviation)
                LEFT JOIN securities act ON LOWER(tmp.name) = LOWER(act.name)
                WHERE act.isin IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to securities")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to securities - {e}")
            
    def load_securities_optional_info(self):
        try:
            df = self.df["securities_info"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for securities table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_securities (
                    ticker VARCHAR(32) NOT NULL,
                    type VARCHAR(32) NOT NULL,
                    market VARCHAR(64)
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_securities", con=conn, if_exists="append", index=False)
                
                #3. update optional data into table
                insert_sql = """
                UPDATE securities s
                INNER JOIN tmp_securities tmp ON s.ticker = tmp.ticker
                SET s.type = tmp.type,
                    s.market = tmp.market;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to securities")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to securities - {e}")
    
    def load_security_operations(self):
        try:
            df = self.df["security_operations"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for security_operations table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_security_operations (
                    date DATE NOT NULL,
                    isin VARCHAR(12) NOT NULL,
                    operation_type ENUM('purchase', 'sale', 'tax') NOT NULL,
                    quantity INT NOT NULL,
                    net_amount DECIMAL(10,4) NOT NULL,
                    gross_amount DECIMAL(10,4) NOT NULL,
                    net_unit_price DECIMAL(10,4) NOT NULL,
                    gross_unit_price DECIMAL(10,4),
                    fees DECIMAL(10,2) DEFAULT 0,
                    account_id BIGINT NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_security_operations", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO security_operations (date, isin, operation_type, quantity, net_amount, gross_amount, net_unit_price, gross_unit_price, fees, account_id)
                SELECT tmp.date, tmp.isin, tmp.operation_type, tmp.quantity, tmp.net_amount, tmp.gross_amount, tmp.net_unit_price, tmp.gross_unit_price, tmp.fees, tmp.account_id
                FROM tmp_security_operations tmp
                LEFT JOIN security_operations act ON tmp.date = act.date AND tmp.isin = act.isin AND tmp.quantity = act.quantity AND tmp.account_id = act.account_id
                WHERE act.id IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to security_operations")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to security_operations - {e}")
    
    def load_security_prices(self):
        try:
            df = self.df["security_prices"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for security_prices table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_security_prices (
                    isin VARCHAR(12) NOT NULL,
                    date DATE NOT NULL,
                    open_price DECIMAL(10,4),
                    close_price DECIMAL(10,4),
                    high DECIMAL(10,4),
                    low DECIMAL(10,4),
                    volume BIGINT
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_security_prices", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO security_prices (date, isin, open_price, close_price, high, low, volume)
                SELECT tmp.date, tmp.isin, tmp.open_price, tmp.close_price, tmp.high, tmp.low, tmp.volume
                FROM tmp_security_prices tmp
                LEFT JOIN security_prices act ON tmp.date = act.date AND tmp.isin = act.isin
                WHERE act.id IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to security_prices")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to security_prices - {e}")
    
    def load_transactions(self):
        try:
            df = self.df["transactions"]
        except KeyError as e:
            logging.error(f"ERROR : No DataFrame found for transactions table")
            return
            
        try :
            with self.engine.begin() as conn:
                #1. create temp table
                create_tmp_table_sql = """
                CREATE TEMPORARY TABLE tmp_transactions (
                    id BIGINT NOT NULL,
                    account_id BIGINT NOT NULL,
                    date DATE NOT NULL,
                    payee VARCHAR(256),
                    clean_payee VARCHAR(128),
                    memo VARCHAR(256),
                    amount DECIMAL(10,2) NOT NULL,
                    is_expense TINYINT(1) NOT NULL
                );
                """
                conn.execute(text(create_tmp_table_sql))
                
                #2. insert data into temp table
                df.to_sql("tmp_transactions", con=conn, if_exists="append", index=False)
                
                #3. insert unique data into table
                insert_sql = """
                INSERT INTO transactions (id, account_id, date, payee, clean_payee, memo, amount, is_expense)
                SELECT tmp.id, tmp.account_id, tmp.date, tmp.payee, tmp.clean_payee, tmp.memo, tmp.amount, tmp.is_expense
                FROM tmp_transactions tmp
                LEFT JOIN transactions act ON tmp.id = act.id
                WHERE act.id IS NULL;
                """
                conn.execute(text(insert_sql))
                logging.info("Data added to transactions")
        except Exception as e:
            logging.error(f"ERROR : Unable to add data to transactions - {e}")
    
    def load_all(self):
        if "account_types" in self.df:
            self.load_account_types()
        if "banks" in self.df:
            self.load_banks()
        if "currency" in self.df:
            self.load_currency()
        
        if "accounts" in self.df:
            self.load_accounts()
        if "securities" in self.df:
            self. load_securities()
        if "securities_info" in self.df:
            self. load_securities_optional_info()
        
        if "balances" in self.df:
            self.load_balances()
        if "security_operations" in self.df:
            self.load_security_operations()
        if "security_prices" in self.df:
            self.load_security_prices()
        if "transactions" in self.df:
            self.load_transactions()
