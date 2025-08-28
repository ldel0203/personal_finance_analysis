import os
import pathlib
from pipelines.csv_securities_pipeline import CsvSecuritiesPipeline
from pipelines.ofx_pipeline import OfxPipeline
from pipelines.yfinance_pipeline import YfinancePipeline
import shutil
import logging

class MainPipeline:
    def __init__(self, data_dir: str, db_config: dict):
        self.data_dir = data_dir
        self.db_config = db_config
        
    def move_file_to(self, file_path, to_folder):
        if not os.path.exists(file_path):
            logging.error(f"File not found : {file_path}")
            raise FileNotFoundError("File not found")
        if not os.path.exists(to_folder):
            logging.error(f"Folder doesn't exist : {to_folder}")
            raise NotADirectoryError("Folder doesn't exist")
        
        path = pathlib.Path(file_path)
        
        #remove file in destination folder if exist
        next_path = pathlib.Path(to_folder) / path.name
        if pathlib.Path.exists(next_path):
            os.remove(next_path)
            logging.info(f"Removed duplicate file in target: {next_path}")
        
        #move file
        file_path = shutil.move(file_path, to_folder)
        
    def process_all_csv_securities_files(self):
        #1. move error files to process folder
        file_counter = 0
        for file in os.scandir(os.path.join(self.data_dir,"error")):
            _, ext = os.path.splitext(file.name)
            if file.is_file() and ext.lower() == ".csv":
                self.move_file_to(file, os.path.join(self.data_dir,"to_process"))
                file_counter += 1
                logging.info(f"Moving from error directory {file.name}")
        logging.info(f"REPORT : {file_counter} file(s) moved to processing folder")
        
        #2. process all ofx files in to_process folder
        file_counter = 0
        file_counter_error = 0
        for file in os.scandir(os.path.join(self.data_dir,"to_process")):
            _, ext = os.path.splitext(file.name)
            if file.is_file() and ext.lower() == ".csv":
                try:
                    pipeline = CsvSecuritiesPipeline(file.path, self.db_config)
                    pipeline.run()
                    self.move_file_to(file, os.path.join(self.data_dir,"archives"))
                    file_counter += 1
                except Exception as e:
                    logging.error(f"Error processing {file.name} : {e}")
                    self.move_file_to(file, os.path.join(self.data_dir,"error"))
                    file_counter_error += 1
        logging.info(f"REPORT : {file_counter} file(s) successfully processed - {file_counter_error} file(s) encountered an error")
        
    def process_all_ofx_files(self):
        #1. move error files to process folder
        file_counter = 0
        for file in os.scandir(os.path.join(self.data_dir,"error")):
            _, ext = os.path.splitext(file.name)
            if file.is_file() and ext.lower() == ".ofx":
                self.move_file_to(file, os.path.join(self.data_dir,"to_process"))
                file_counter += 1
                logging.info(f"Moving from error directory {file.name}")
        logging.info(f"REPORT : {file_counter} file(s) moved to processing folder")
        
        
        #2. process all ofx files in to_process folder
        file_counter = 0
        file_counter_error = 0
        for file in os.scandir(os.path.join(self.data_dir,"to_process")):
            _, ext = os.path.splitext(file.name)
            if file.is_file() and ext.lower() == ".ofx":
                try:
                    pipeline = OfxPipeline(file.path, self.db_config)
                    pipeline.run()
                    self.move_file_to(file, os.path.join(self.data_dir,"archives"))
                    file_counter += 1
                except Exception as e:
                    logging.error(f"Error processing {file.name} : {e}")
                    self.move_file_to(file, os.path.join(self.data_dir,"error"))
                    file_counter_error += 1
        logging.info(f"REPORT : {file_counter} file(s) successfully processed - {file_counter_error} file(s) encountered an error")

    def process_yfinance(self):
        pipeline = YfinancePipeline(self.db_config)
        pipeline.run()

    def run(self):
        # Source OFX
        self.process_all_ofx_files()
        
        # Source csv securities
        self.process_all_csv_securities_files()
        
        # Source yfinance
        self.process_yfinance()
