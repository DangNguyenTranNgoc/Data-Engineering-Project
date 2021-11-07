#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re
import argparse
import os

from ..utils import (
    get_logger,
    start_logging,
)

BASE_URL = r'https://www.cophieu68.vn/historyprice.php'
LAST_INDEX = -1
DATA_DIR = f"data{os.sep}stock"
DATETIME_FORMAT = r"%d-%m-%Y"
PAGE_NUMBER_FILTER = r'(currentPage=)([\d]+)'
CHARACTER_IGNORE = ['\n']
PRICE_DF_HEADER = ["date", "ref_price", "diff_price", "diff_price_rat",
                "close_price", "vol", "open_price", "highest_price",
                "lowest_price", "transaction", "foreign_buy", "foreign_sell"]
MODULE_NAME = "stock_crawler.price"

class StockPriceCrawler():

    def __init__(self, logger):
        self.logger = logger

    def datetime_parser(self, date_str:str) -> datetime:
        """
        Parse dd-mm-yyyy string to datetime object
        """
        try:
            datetime_obj = datetime.strptime(date_str, DATETIME_FORMAT)
        except ValueError as error:
            self.logger.debug(error)
            datetime_obj = None
        return datetime_obj


    def str2fl_parser(self, str_number:str, comma=False) -> float:
        """
        Parser (str) 17.50 to (float) 17.5
        """
        try:
            _str = str_number
            if comma:
                _str = _str.replace(",", "")
            number = float(_str)
        except ValueError as error:
            self.logger.debug(error)
            number = None
        return number


    def record_parser(self, row) -> list:
        """
        Parse a row of price table. EX:
        """
        try:
            date = self.datetime_parser(row.contents[3].text.strip())
            ref_price = self.str2fl_parser(row.contents[5].text.strip())
            diff_price = self.str2fl_parser(row.contents[7].text.strip())
            diff_price_rat = self.str2fl_parser(row.contents[9].text.strip()[:-1])
            close_price = self.str2fl_parser(row.contents[11].text.strip())
            vol = self.str2fl_parser(row.contents[13].text.strip(), comma=True)
            open_price = self.str2fl_parser(row.contents[15].text.strip())
            highest_price = self.str2fl_parser(row.contents[17].text.strip())
            lowest_price = self.str2fl_parser(row.contents[19].text.strip())
            transaction = self.str2fl_parser(row.contents[21].text.strip(), comma=True)
            foreign_buy = self.str2fl_parser(row.contents[23].text.strip(), comma=True)
            foreign_sell = self.str2fl_parser(row.contents[25].text.strip(), comma=True)
            record = [date, ref_price, diff_price, diff_price_rat, 
                    close_price, vol, open_price, highest_price, 
                    lowest_price, transaction, foreign_buy, foreign_sell]
            if record.count(None) == len(record):
                record = None
        except AttributeError as error:
            self.logger.debug(error)
            record = None
        except IndexError as error:
            self.logger.debug(error)
            record = None
        return record


    def get_record_date(self, row) -> datetime:
        """
        Return the date of the record
        """
        try:
            date = self.datetime_parser(row.contents[3].text.strip())
        except AttributeError as error:
            self.logger.debug(error)
            date = None
        return date


    def get_page_num_from_url(self, link:str) -> int:
        """
        Return page number from link 
        Ex: https://www.cophieu68.vn/historyprice.php?currentPage=29&id=aaa => 29
        """
        try:
            page_num = re.search(PAGE_NUMBER_FILTER, link)
            if page_num is not None:
                return int(page_num[2])
        except Exception as ex:
            self.logger.debug(ex)
        return 0


    def crawl_page(self, link:str) -> list:
        """
        Crawl price from a page
        """
        try:
            self.logger.info(f"Crawl a page from url: {link}")
            request = requests.get(link, verify=False)
            soup = BeautifulSoup(request.content, from_encoding="utf-8")
            price_content = soup.find('div', {'id':'content'})
            price_list = self.price_table_parser(price_content.table)
            if not price_list:
                price_list = None
        except Exception as error:
            self.logger.debug(error)
            price_list = None
        return price_list


    def crawl_all(self, id:str) -> list:
        """
        Crawl all price from the first to the last page.
        """
        self.logger.info(f"Crawl all from url: {BASE_URL}?id={id}")
        request = requests.get(f"{BASE_URL}?id={id}", verify=False)
        soup = BeautifulSoup(request.content, from_encoding="utf-8")
        price_content = soup.find('div', {'id':'content'})
        last_page = price_content.ul.contents[-1].a.attrs['href']
        # Parse current page
        price_list = []
        price_table = self.price_table_parser(price_content.table)
        [price_list.append(record) for record in price_table]
        page_num = self.get_page_num_from_url(last_page) + 1
        # Parse to the last
        for i in range(2, page_num):
            url = "{}?currentPage={}&id={}".format(BASE_URL, i, id)
            _price_list = self.crawl_page(url)
            if _price_list is not None:
                [price_list.append(record) for record in _price_list]
        self.logger.info(f"Done! Crawl total {len(price_list)}")
        return price_list


    def price_table_parser(self, price_table) -> list:
        """
        Parse price table to list
        """
        price_list = []
        for row in price_table.contents:
            if row in CHARACTER_IGNORE:
                continue
            price_row = self.record_parser(row)
            if price_row is not None:
                price_list.append(price_row)
        return price_list
    

    def get_latest_date_db(self) -> datetime:
        """
        """
        return datetime(2021, 3, 23)#23-03-2021


    def __find_index_date(self, table:list, date:datetime):
        if isinstance(table, list) and isinstance(table[0], list):
            for idx, row in enumerate(table):
                if date >= row[0]:
                    return idx
            return None
    

    def crawl_by_date(self, id:str):
        """
        Crawl data from the date to the latest date in wbesite
        """
        try:
            date = self.get_latest_date_db()
            logger.info(f"Crawl from url: {BASE_URL}?id={id} from date: {date.strftime(r'%Y-%m-%d')}")
            request = requests.get(f"{BASE_URL}?id={id}", verify=False)
            soup = BeautifulSoup(request.content, from_encoding="utf-8")
            price_content = soup.find('div', {'id':'content'})
            # Parse current page
            price_list = []
            price_table = self.price_table_parser(price_content.table)
            [price_list.append(record) for record in price_table]
            # Check if date is in list
            idx = self.__find_index_date(price_list, date)
            if idx is not None:
                price_list = price_list[:idx]
                return price_list
            # Continue to the last page
            last_page = price_content.ul.contents[-1].a.attrs['href']
            page_num = self.get_page_num_from_url(last_page) + 1
            # Parse to the last
            for i in range(2, page_num):
                url = "{}?currentPage={}&id={}".format(BASE_URL, i, id)
                _price_list = self.crawl_page(url)
                if _price_list is not None:
                    idx = self.__find_index_date(_price_list, date)
                    if idx is not None:
                        _price_list = _price_list[:idx]
                        [price_list.append(record) for record in _price_list]
                        break
                    # Date not existed in _price_list, add all to price_list
                    [price_list.append(record) for record in _price_list]
            self.logger.info(f"Done! Crawl total {len(price_list)}")
            return price_list
        except Exception as ex:
            self.logger.debug(ex)
        return None


    def export_to_csv(self, *, file_name:str, data:list, dest):
        """
        Convert list to DataFrame then export to csv file at dest location
        """
        if not os.path.isdir(dest):
            self.logger.debug(f"{dest} is not a directory!")
            raise NotADirectoryError(f"{dest} is not a directory!")
        self.logger.info(f"Write data to file {file_name} at {dest}")
        try:
            price_df = pd.DataFrame(data, columns=PRICE_DF_HEADER)
            price_df = price_df.sort_values(by='date')
            price_df.to_csv(f"{dest}{os.sep}{file_name}", index=False)
        except Exception as ex:
            self.logger.debug(ex)


def argument_parser():
    """
    Add CLI argument parser
    """
    parser = argparse.ArgumentParser(description='Tool crawl stock price from cophieu68')
    parser.add_argument("-i", "--id", action='store', type=str,
                        dest='stock_id', required=True, help='The id of the stock')
    parser.add_argument("-d", "--data-dir", action='store', type=str, default=DATA_DIR,
                        dest='data_dir', required=False, help='The location store data')
    parser.add_argument("-l", "--latest", action='store_true',
                        dest='is_latest', required=False, help='Is crawl from the latest date in DB?')
    parser.add_argument("-f", "--file-name", action='store', type=str,
                        dest='file_name', required=False, help='The name of the exported data file')
    return parser.parse_args()


def main(args, log):
    """
    """
    stock_price_crawler = StockPriceCrawler(log)
    stock_id = args.stock_id.strip()

    if args.is_latest:
        data = stock_price_crawler.crawl_by_date(id=stock_id)
    else:
        data = stock_price_crawler.crawl_all(id=stock_id)
    
    if args.data_dir:
        dest = args.data_dir
    else:
        dest = DATA_DIR
    
    if args.file_name:
        file_name = args.file_name
    else:
        file_name = f"{stock_id}_stock_price.csv"
    
    stock_price_crawler.export_to_csv(file_name=file_name, data=data, dest=dest)


if __name__ == "__main__":
    logger = get_logger(MODULE_NAME)
    start_logging()
    args = argument_parser()
    main(args, logger)
