import os
import pandas as pd
import logging
import time
from create_connection import create_connection

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level= logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

logger = logging.getLogger('ingestion_db')

engine = create_connection()

def ingest_db(df, table_name, con):
    '''this func injects the dataframe into db table'''
    df.to_sql(table_name, con=con, if_exists='replace', index=False)

def load_raw_data():
    '''this func loads csv as dataframe and loads into db'''
    start = time.time()
    for file in os.listdir("data"):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join("data", file))
            logging.info(f"Ingesting {file} to table {file[:-4]}")
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end-start)/60
    logger.info(f"Ingestion Complete")
    logger.info(f"Total time taken : {total_time}")

