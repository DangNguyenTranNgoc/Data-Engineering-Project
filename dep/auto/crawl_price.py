#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This fil for automation crawl stock price with id.
"""

import os
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, time
from sqlalchemy import create_engine
from dotenv import load_dotenv

from dep.crawler.stock_price import StockPriceCrawler
from dep.utils.logging_utils import (
    get_logger,
    start_logging,
)
from dep.utils.data_info_handler import (
    DataInfoHandler,
    MetaDataType,
    MetaDataKey,
)
from dep.utils.database import Database


DATA_DIR = f"data"
FILE_NAME = "metadata.yaml"
PRICE_DATA_DIR = f"{DATA_DIR}{os.sep}stock"
PRICE_DF_HEADER = ["stock_code", "date", "ref_price", "diff_price", "diff_price_rat",
                "close_price", "vol", "open_price", "highest_price",
                "lowest_price", "transaction", "foreign_buy", "foreign_sell"]
dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)
DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
DATABASE_TABLE = "stock_price"
MODULE_NAME = "dep.utils.data_info_handler"


class CrawlPriceAutomationException(Exception):
    """
    Exception class
    """

    def __init__(self, value):
        self.value = value


    def __str__(self):
        return repr(self.value)


class CrawlPriceAutomation():
    """
    Auto crawl price with info from metadata
    """

    def __init__(self, stock_id:str) -> None:
        self.logger = get_logger(MODULE_NAME)
        self.stock_id = stock_id
        self.data_info = DataInfoHandler()
        self.stock_crawler = StockPriceCrawler(stock_id)

    
    def get_latest_date(self):
        """
        Get the latest date on DB
        """
        database = Database()
        try:
            query = (
                f"SELECT date "
                f"FROM stock_price "
                f"WHERE code = '{self.stock_id}' "
                f"ORDER BY date DESC LIMIT 1"
            )
            response = database.get_one(query)
            self.logger.verbose(f"Database response: {response}")
            if response is not None:
                return datetime.combine(response[0], time.min)
            return None
        finally:
            database.close_connect()
    

    def __insert_csv_to_database(self):
        """
        Read metadata => Read csv file => Insert to database
        """
        latest_date = self.get_latest_date()
        while self.data_info.metadata[MetaDataType.STOCK_PRICE.value][self.stock_id]:
            record = self.data_info.get_first_record(MetaDataType.STOCK_PRICE, self.stock_id)
            if record is not None:
                # Read csv file
                data = pd.read_csv(record[MetaDataKey.FILE_PATH.value], index_col=False)
                # Convert second column to datetime
                data[PRICE_DF_HEADER[1]] = pd.to_datetime(data[PRICE_DF_HEADER[1]])
                # Check the data read vs in file
                if len(data.index) != record[MetaDataKey.RECORD_NUMER.value]:
                    self.logger.warning(
                        f"The number of record read ({len(data.index)}) is different with in file ({record[MetaDataKey.RECORD_NUMER.value]})"
                    )
                if latest_date is not None:
                    data = self.__get_data_from_date(data, latest_date)
                self.__insert_database(data)
            # Remove the record when done
            self.data_info.pop_record(MetaDataType.STOCK_PRICE, self.stock_id)


    def exec(self):
        """
        Runner
        """
        # Get latest date from DB
        from_date = self.get_latest_date()
        # Crawl data
        if from_date is not None:
            data = self.stock_crawler.crawl_from_date(from_date)
        else:
            # If stock id is not existed (new stock)
            data = self.stock_crawler.crawl_all()
        # Save data to csv
        str_time = time.strftime(r"%Y-%m-%d_%H%M%S", time.gmtime())
        file_name = f"{self.stock_id}_stock_price_{str_time}.csv"
        self.stock_crawler.export_to_csv(file_name=file_name, data=data, dest=PRICE_DATA_DIR)
        # Save metadata
        self.data_info.append_metadata(metadata_type=MetaDataType.STOCK_PRICE,
                                        crawl_date=datetime.now(),
                                        stock_id=self.stock_id,
                                        file_path=f"{PRICE_DATA_DIR}{os.sep}{file_name}",
                                        from_date=data[-1][1].date(),
                                        to_date=data[0][1].date(),
                                        record_number=len(data))
        self.data_info.save_file()
        # Insert crawled data to database
        self.__insert_csv_to_database()
        self.data_info.save_file()
    

    def __get_data_from_date(self, data:pd.DataFrame, from_date:datetime):
        if not isinstance(data, pd.DataFrame) or data.ndim != 2:
            raise CrawlPriceAutomationException(f"Invalid data")
        if not isinstance(from_date, datetime):
            raise CrawlPriceAutomationException(f"Invalid date")
        mask = data[PRICE_DF_HEADER[1]] > from_date
        return data.loc[mask]
        

    def __insert_database(self, data:pd.DataFrame):
        if not isinstance(data, pd.DataFrame) or data.ndim != 2:
            raise CrawlPriceAutomationException(f"Invalid data")
        connect_string = (
            f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@"
            f"{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
        )
        engine = create_engine(connect_string)
        data.to_sql(DATABASE_TABLE, engine, if_exists='append', index=False, index_label='id')
        self.logger.info(f"Inserted {len(data.index)} new row(s)")


start_logging()
crawl_price_auto = CrawlPriceAutomation("fox")
crawl_price_auto.exec()
