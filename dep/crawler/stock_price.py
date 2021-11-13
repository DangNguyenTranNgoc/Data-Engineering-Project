#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re
import argparse
import os

from dep.utils import (
    get_logger,
    start_logging,
)

BASE_URL = r'https://www.cophieu68.vn/historyprice.php'
DATA_DIR = f"data{os.sep}stock"
DATETIME_FORMAT = r"%d-%m-%Y"
PAGE_NUMBER_FILTER = r'(currentPage=)([\d]+)'
CHARACTER_IGNORE = ['\n']
PRICE_DF_HEADER = ["stock_code", "date", "ref_price", "diff_price", "diff_price_rat",
                "close_price", "vol", "open_price", "highest_price",
                "lowest_price", "transaction", "foreign_buy", "foreign_sell"]
MODULE_NAME = "dep.stock_crawler.price"


class StockPriceCrawlerException(Exception):
    """
    Base class for errors in StockInfoCrawler
    """
    def __init__(self, value):
        self.value = value


    def __str__(self):
        return repr(self.value)


class StockPriceCrawler:
    """
    Class stock price data crawler
    """
    def __init__(self, stock_id:str, base_url=BASE_URL):
        self.logger = logging.getLogger(MODULE_NAME)
        self.base_url = base_url
        self.stock_id = stock_id


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
            else:
                record.insert(0, self.stock_id)
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


    def price_table_parser(self, price_table) -> list:
        """
        Parse price table to list
        """
        price_list = []
        for idx, row in enumerate(price_table.contents):
            # Ignore special character and header
            if (row in CHARACTER_IGNORE) or (idx == 1):
                continue
            price_row = self.record_parser(row)
            if price_row is not None:
                price_list.append(price_row)
        return price_list


    def export_to_csv(self, *, file_name:str, data:list, dest):
        """
        Convert list to DataFrame then export to csv file at dest location
        """
        if not os.path.isdir(dest):
            self.logger.error(f"{dest} is not a directory!")
            raise StockPriceCrawlerException(f"{dest} is not a directory!")
        self.logger.info(f"Write data to file {file_name} at {dest}")
        try:
            price_df = pd.DataFrame(data, columns=PRICE_DF_HEADER)
            price_df = price_df.sort_values(by='date')
            price_df.to_csv(f"{dest}{os.sep}{file_name}", index=False)
        except Exception as ex:
            self.logger.error(ex)
            raise ex


    def crawl_all(self, page:int=1, max_page:int=None, data:list=[]) -> list:
        """
        Crawl all data recursive
        """
        # Current page is not 1 and missing maximum page number
        # It could lead to non-stop cycle
        if max_page is None and page != 1:
            self.logger.error(f"Exception occur: Missing max page at page: {page}")
            raise StockPriceCrawlerException(f"Missing max page at page: {page}")
        # Current page is 1 => find max page
        if max_page is None and page == 1:
            data = []
            first_list, max_page = self.__crawl_first_page()
            [data.append(row) for row in first_list]
            return self.crawl_all(page+1, max_page, data)
        # Parse page
        url = f"{self.base_url}?currentPage={page}&id={self.stock_id}"
        prices = self.crawl_page(url)
        [data.append(row) for row in prices]
        # If at the last page => return
        if page == max_page:
            self.logger.info(f"Done! Crawl total {len(data)} row(s)")
            return data
        # Else (page 1 to n-1) => call to page + 1 with data loaded
        return self.crawl_all(page+1, max_page, data)


    def crawl_from_date(self, from_date:datetime, page:int=1, max_page:int=None, data:list=[]) -> list:
        """
        Crawl all data recursive from latest date to the date
        """
        if max_page is None and page != 1:
            raise StockPriceCrawlerException(f"Missing max page at page: {page}")
        if not isinstance(from_date, datetime):
            raise StockPriceCrawlerException(f"Expected 'datetime', recieve '{type(from_date)}'")
        if from_date > datetime.today():
            raise StockPriceCrawlerException(f"Input date ({from_date}) is greater than today")
        if max_page is None and page == 1:
            self.logger.info(f"Crawling from date {from_date.date()} to current date.")
            data = []
            first_list, max_page = self.__crawl_first_page()
            idx = self.__find_index_date(first_list, from_date)
            if idx is not None:
                [data.append(row) for row in first_list[:idx]]
                self.logger.info(f"Done! Crawl total {len(data)} row(s)")
                return data
            [data.append(row) for row in first_list]
            return self.crawl_from_date(from_date, page+1, max_page, data)
        url = f"{self.base_url}?currentPage={page}&id={self.stock_id}"
        prices = self.crawl_page(url)
        idx = self.__find_index_date(prices, from_date)
        if idx is not None:
            [data.append(row) for row in prices[:idx]]
            self.logger.info(f"Done! Crawl total {len(data)} row(s)")
            return data
        if page == max_page:
            self.logger.info(f"Done! Crawl total {len(data)} row(s)")
            return data
        return self.crawl_from_date(from_date, page+1, max_page, data)


    def __get_price_table_from_url(self, url:str):
        request = requests.get(url, verify=False)
        soup = BeautifulSoup(request.content, from_encoding="utf-8")
        price_content = soup.find('div', {'id':'content'})
        if price_content is not None:
            return price_content
        return None
    

    def __crawl_first_page(self) -> tuple:
        """
        Crawling first page. Return data and last page number.
        """
        url = f"{self.base_url}?id={self.stock_id}"
        self.logger.info(f"Crawl the first page from url {url}")
        price_content = self.__get_price_table_from_url(url)
        # Get the last page number
        last_page = price_content.ul.contents[-1].a.attrs['href']
        last_page_num = self.__get_page_num_from_url(last_page)
        # Parse price table
        price_table = self.price_table_parser(price_content.table)
        return price_table, last_page_num
    

    def __find_index_date(self, table:list, from_date:datetime):
        if isinstance(table, list) and isinstance(table[0], list) and isinstance(from_date, datetime):
            for idx, row in enumerate(table):
                if isinstance(row[1], datetime) and (from_date >= row[1]):
                    return idx
        return None
        
    
    def __get_page_num_from_url(self, link:str) -> int:
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


def dateType(arg:str):
    """
    Custom date type for argeparser
    """
    try:
        from_date = datetime.strptime(arg, DATETIME_FORMAT)
        return from_date
    except ValueError as ex:
        raise StockPriceCrawlerException(f"Invalid date! Expected 'dd-mm-yyyy', recieved '{arg}'")



def argument_parser():
    """
    Add CLI argument parser
    """
    parser = argparse.ArgumentParser(description='Tool crawl stock price from cophieu68')
    parser.add_argument("-i", "--id", action='store', type=str,
                        dest='stock_id', required=True, help='The id of the stock')
    parser.add_argument("-d", "--data-dir", action='store', type=str, default=DATA_DIR,
                        dest='data_dir', required=False, help='The location store data')
    parser.add_argument("--from-date", action='store', type=dateType, dest='from_date',
                        required=False, 
                        help='Crawl from this date to the latest date. Format: "15-09-2021"',)
    parser.add_argument("-f", "--file-name", action='store', type=str,
                        dest='file_name', required=False, help='The name of the exported data file')
    return parser.parse_args()


def main():
    """
    Main function for control
    """
    args = argument_parser()
    logger = get_logger(MODULE_NAME)
    start_logging()
    stock_id = args.stock_id.strip()
    stock_price_crawler = StockPriceCrawler(logger, stock_id)
    try:
        if args.from_date:
            data = stock_price_crawler.crawl_from_date(from_date=args.from_date)
        else:
            data = stock_price_crawler.crawl_all()

        if args.data_dir:
            dest = args.data_dir
        else:
            dest = DATA_DIR

        if args.file_name:
            file_name = args.file_name
        else:
            file_name = f"{stock_id}_stock_price.csv"

        stock_price_crawler.export_to_csv(file_name=file_name, data=data, dest=dest)
    except KeyboardInterrupt:
        logger.info("Runner exit manually (KeyboardInterrupt)")
        return 0
    except Exception as ex:
        logger.debug(ex)
        raise ex


if __name__ == "__main__":
    main()
