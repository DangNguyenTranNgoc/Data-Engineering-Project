#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This fil for automation crawl stock price with id.
"""
import os
import pandas as pd
from datetime import datetime

from dep.crawler.stock_price import StockPriceCrawler
from dep.utils.logging_utils import (
    get_logger,
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
STOCK_INFO_DATA_CSV = f"{DATA_DIR}{os.sep}info{os.sep}info_all_2021-11-09_144249.csv"
DATABASE_TABLE = "stock_price"
MODULE_NAME = "dep.auto.crawl_price_auto"


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

    def __init__(self, test_db=False) -> None:
        self.logger = get_logger(MODULE_NAME)
        self.database = Database(test_db)


    def crawl_data(self, stock_id:str):
        """
        Runner
        """
        self.logger.info(f"Crawl stock of '{stock_id}'")
        # Get latest date from DB
        from_date = self.database.get_latest_date(stock_id)
        crawler = StockPriceCrawler(stock_id)
        data_info = DataInfoHandler()
        # Crawl data
        if from_date is not None:
            data = crawler.crawl_from_date(from_date)
        else:
            # If stock id is not existed (new stock)
            data = crawler.crawl_all()
        # Save data to csv
        str_time = '{0:%Y%m%d_%H%M%S}'.format(datetime.now())
        file_name = f"{stock_id}_stock_price_{str_time}.csv"
        crawler.export_to_csv(file_name=file_name, data=data, dest=PRICE_DATA_DIR)
        # Save metadata
        data_info.append_metadata(metadata_type=MetaDataType.STOCK_PRICE,
                                crawl_date=datetime.now(),
                                stock_id=stock_id,
                                file_path=f"{PRICE_DATA_DIR}{os.sep}{file_name}",
                                from_date=data[-1][1].date(),
                                to_date=data[0][1].date(),
                                record_number=len(data))
        data_info.save_file()
    

    def crawl_all_stock_from_db(self):
        """
        Crawl all stock with stock id get from stock_info
        """
        self.logger.warning(f"Will crawl ALL stock price with company code get from database")
        stock_list = self.database.get_all_code()
        for stock in stock_list:
            self.crawl_data(stock)


    def insert_to_datbase(self):
        """
        Read metadata => Read csv file => Insert to database
        """
        self.logger.info(f"Begin insert CSV file to database. Files info get from metadata.yaml")
        data_info = DataInfoHandler()
        # Get list of key in case of dictionary changed size during iteration
        id_list = list(data_info.metadata[MetaDataType.STOCK_PRICE.value].keys())
        for stock in id_list:
            self.__insert_to_database(data_info, stock)


    def __insert_to_database(self, data_info:DataInfoHandler, stock_id:str):
        if not isinstance(data_info, DataInfoHandler):
            raise CrawlPriceAutomationException(f"Expected 'DataInfoHandler', recieve {type(data_info)}")
        self.logger.info(f"Insert csv(s) of {stock_id.upper()}")
        latest_date = self.database.get_latest_date(stock_id)
        while data_info.metadata[MetaDataType.STOCK_PRICE.value][stock_id]:
            record = data_info.get_first_record(MetaDataType.STOCK_PRICE, stock_id)
            if record is not None:
                # Read csv file
                data = pd.read_csv(record[MetaDataKey.FILE_PATH.value], index_col=False)
                self.logger.info(f"Insert file '{record[MetaDataKey.FILE_PATH.value]}'...")
                # Convert second column to datetime
                data[PRICE_DF_HEADER[1]] = pd.to_datetime(data[PRICE_DF_HEADER[1]])
                # Check the data read vs in file
                if len(data.index) != record[MetaDataKey.RECORD_NUMER.value]:
                    self.logger.warning(
                        f"The number of record read ({len(data.index)}) is different with in file ({record[MetaDataKey.RECORD_NUMER.value]})"
                    )
                if latest_date is not None:
                    data = self.__get_data_from_date(data, latest_date)
                self.database.insert_data(data, 'stock_price')
            # Remove the record when done
            data_info.pop_record(MetaDataType.STOCK_PRICE, stock_id)
        data_info.save_file()


    def insert_info(self):
        """
        Insert stock info into database
        """
        data = pd.read_csv(STOCK_INFO_DATA_CSV, index_col=False)
        data['pub_date'] = pd.to_datetime(data['pub_date'])
        self.database.insert_data(data, 'stock_info')
        self.logger.info(f"Inserted {len(data.index)} new row(s)")


    def __get_data_from_date(self, data:pd.DataFrame, from_date:datetime):
        if not isinstance(data, pd.DataFrame) or data.ndim != 2:
            raise CrawlPriceAutomationException(f"Invalid data")
        if not isinstance(from_date, datetime):
            raise CrawlPriceAutomationException(f"Invalid date")
        mask = data[PRICE_DF_HEADER[1]] > from_date
        return data.loc[mask]