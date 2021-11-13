#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This fil for automation crawl stock price with id.
Step:
1/ Call this script with id of the stock.
2/ Get the latest date in DB
    - Exception: id not existed => Crawl all
3/ Call script crawler.stock_price with date and file name.
4/ Save data to metadata with data info handler
"""
import os
from datetime import datetime
import time

from dep.crawler.stock_price import StockPriceCrawler
from dep.utils.logging_utils import (
    get_logger,
    start_logging,
)
from dep.utils.data_info_handler import (
    DataInfoHandler,
    MetaDataType,
)


DATA_DIR = f"data"
FILE_NAME = "metadata.yaml"
PRICE_DATA_DIR = f"{DATA_DIR}{os.sep}stock"
MODULE_NAME = "dep.utils.data_info_handler"

class CrawlPriceAutomation():
    """
    Auto crawl price with info from metadata
    """

    def __init__(self, stock_id:str) -> None:
        self.logger = get_logger(MODULE_NAME)
        self.stock_id = stock_id
        self.data_info = DataInfoHandler()
        self.stock_crawler = StockPriceCrawler(stock_id)
    

    def exec(self):
        """
        Runner
        """
        # Get latest date from DB
        from_date = datetime(2020, 11, 11)
        data = self.stock_crawler.crawl_from_date(from_date)
        # If stock id is not existed (new stock)
        # data = self.stock_crawler.crawl_all()
        str_time = time.strftime(r"%Y-%m-%d_%H%M%S", time.gmtime())
        file_name = f"{self.stock_id}_stock_price_{str_time}.csv"
        self.stock_crawler.export_to_csv(file_name=file_name, data=data, dest=PRICE_DATA_DIR)
        self.data_info.append_metadata(metadata_type=MetaDataType.STOCK_PRICE,
                                        crawl_date=datetime.now(),
                                        stock_id=self.stock_id,
                                        file_path=f"{PRICE_DATA_DIR}{os.sep}{file_name}",
                                        from_date=from_date.date(),
                                        to_date=datetime.now().date(),
                                        record_number=len(data))
        self.data_info.save_file()


start_logging()
crawl_price_auto = CrawlPriceAutomation("fox")
crawl_price_auto.exec()
