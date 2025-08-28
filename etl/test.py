from extract.ofx_extractor import OfxExtractor
from extract.mysql_extractor import MySQLExtractor
from transform.ofx_transformer import OfxTransformer
from load.mysql_loader import MySQLLoader

db_config = {
    "user"      : "root",
    "password"  : "root",
    "host"      : "localhost",
    "port"      : 3306,
    "database"  : "personnal_finance_db"
}

ofx_extractor = OfxExtractor(r"D:\Users\lucas\OneDrive\Economie\7 - powerBI\etl\data\to_process\CA20250726_173158.ofx")
raw_data = ofx_extractor.extract_transactions()

db_extractor = MySQLExtractor(db_config)
payees = db_extractor.get_clean_payees()
transformer = OfxTransformer(raw_data, db_config)

clean_data = transformer.transform_transactions()
print(clean_data)