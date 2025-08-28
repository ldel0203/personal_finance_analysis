import logging
import os
from pipelines.main_pipeline import MainPipeline

if __name__ == "__main__":
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
    filename= os.path.join(os.path.dirname(__file__), "logs.log"),
    filemode="a"
    )

    data_directory = os.path.join(os.path.dirname(__file__), "data")
    
    db_config = {
        "user"      : "root",
        "password"  : "root",
        "host"      : "localhost",
        "port"      : 3306,
        "database"  : "personnal_finance_db"
    }
    MainPipeline(data_dir=data_directory, db_config=db_config).run()