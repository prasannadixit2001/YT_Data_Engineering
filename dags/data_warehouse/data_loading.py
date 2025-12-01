import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    file_path = f"./data/YT_data_MrBeast_{date.today()}.json" #path where the data is stored
    try:
        logger.info(f"Processing file: YT_data_MrBeast_{date.today()}.json")

        with open(file_path,'r',encoding='utf-8') as raw_data: #reading the file
            data = json.load(raw_data) #Our data is small so this is fine as it will store the data in memory otherwise out of memory error can occur
        return data

    except FileNotFoundError:
        logger.error(f"file not found:{file_path}")
        raise   
    except json.JSONDecodeError:
        logger.error(f"Invalid json in file:{file_path}")
        raise
