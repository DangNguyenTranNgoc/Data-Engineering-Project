#!/usr/bin/env python
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import sys
import time
from enum import Enum
import argparse

from dep.utils import (
    get_logger,
    start_logging,
)

IGNORNE_CHAR = ['\n']
INFO_HEADER = ["code", "name", "pub_date", "first_vol", "pub_price", "curr_vol", "treasury_shares", "pub_vol", "fr_owner", "fr_owner_rat", "fr_vol_remain", "curr_price", "market_capital", "category", "exchanges"]
DATA_DIR = f"data{os.sep}info"
BASE_URL = r'https://www.cophieu68.vn/companylist.php'
MODULE_NAME = 'dep.stock_crawler.info'


class Exchange(Enum):
    HOSE = 1
    HNX = 2
    UPCOM = 3


class Category(Enum):
    bds  = "Bất Động Sản"
    caosu  = "Cao Su"
    ck  = "Chứng Khoán"
    congnghe  = "Công Nghệ Viễn Thông"
    daukhi  = "Nhóm Dầu Khí"
    dichvu  = "Dịch vụ - Du lịch"
    dtpt  = "Đầu tư phát triển"
    dtxd  = "Đầu tư xây dựng"
    duocpham  = "Dược Phẩm / Y Tế / Hóa Chất"
    dvci  = "Dịch vụ công ích"
    giaoduc  = "Giáo Dục"
    hk  = "Hàng không"
    khoangsan  = "Khoáng Sản"
    nangluong  = "Năng lượng Điện/Khí/"
    nganhang  = "Ngân hàng- Bảo hiểm"
    nhua  = "Nhựa - Bao Bì"
    phanbon  = "Phân bón"
    sxkd  = "Sản Xuất - Kinh doanh"
    thep  = "Ngành Thép"
    thucpham  = "Thực Phẩm"
    thuongmai  = "Thương Mại"
    thuysan  = "Thủy Sản"
    vantai  = "Vận Tải/ Cảng / Taxi"
    vlxd  = "Vật Liệu Xây Dựng"
    xaydung  = "Xây Dựng"


class StockInfoCrawlerException(Exception):
    """
    Base class for errors in StockInfoCrawler
    """
    def __init__(self, value):
        self.value = value


    def __str__(self):
        return repr(self.value)


class StockInfoCrawler:
    """
    Class stock info crawler
    """
    def __init__(self, logger, base_url=BASE_URL) -> None:
        self.logger = logger
        self.base_url = base_url


    def convert_str2fl(self, str:str) -> float:
        """
        Convert str to float
        """
        try:
            string = str.replace(',','')
            number = float(string)
        except TypeError as err:
            self.logger.debug(err)
            number = 0.0
        return number


    def parse_info_table(self, *, category:str, exchanges:str) -> list:
        """
        Parse info table to list
        """
        url = self.__id2url_converter(category, exchanges)
        info_table = self.__get_info_table(url)
        self.logger.info(f"Crawl info from category \"{category.upper()}\" in \"{exchanges.upper()}\"")
        info_list = []
        for record in info_table.children:
            if record not in IGNORNE_CHAR:
                info = self.__record_parser(record, category, exchanges)
                if info:
                    info_list.append(info)
        self.logger.info(f"Done! Crawling total {len(info_list)} record(s)")
        return info_list


    def crawl_all(self) -> list:
        """
        Crawl all info to one list
        """
        info_list = []
        self.logger.info("Crawl all info")
        for exchange in Exchange:
            for cat in Category:
                info = self.parse_info_table(category=cat.name, exchanges=exchange.name)
                if info:
                    [info_list.append(row) for row in info]
        self.logger.info(f"Done! Crawling all total {len(info_list)} record(s)")
        return info_list


    def save_to_csv(self, data:list, file:str, dest:str=DATA_DIR):
        """
        Save data to csv file
        """
        if not os.path.isdir(dest):
            self.logger.error(f"{dest} is not a directory!")
            raise StockInfoCrawlerException(f"{dest} is not a directory!")
        try:
            path = f"{dest}{os.sep}{file}"
            self.logger.info(f"Save data to {path}")
            info_df = pd.DataFrame(data, columns=INFO_HEADER)
            info_df.to_csv(path, encoding='utf-8', index=False)
        except Exception as ex:
            self.logger.error(ex)
            raise ex


    def __record_parser(self, tr, category:str='', exchanges:str='') -> list:
        try:
            id = tr.contents[3].text.strip()
            name = tr.contents[3].a['title'].strip()
            pub_date = tr.contents[7].text.strip()
            first_vol = self.convert_str2fl(tr.contents[9].text.strip())
            pub_price = self.convert_str2fl(tr.contents[11].text.strip())
            curr_vol = self.convert_str2fl(tr.contents[13].text.strip())
            treasury_shares = self.convert_str2fl(tr.contents[15].text.strip())
            pub_vol = self.convert_str2fl(tr.contents[17].text.strip())
            fr_owner = self.convert_str2fl(tr.contents[19].text.strip())
            fr_owner_rat = fr_owner/pub_vol*100
            fr_vol_remain = self.convert_str2fl(tr.contents[21].contents[0].strip())
            curr_price = self.convert_str2fl(tr.contents[23].strong.text.strip())
            market_capital = self.convert_str2fl(tr.contents[25].text.strip())
            info = [id, name, pub_date, first_vol, pub_price, curr_vol, treasury_shares, pub_vol, fr_owner, fr_owner_rat, fr_vol_remain, curr_price, market_capital, category, exchanges]
        except Exception as ex:
            self.logger.debug(ex)
            info = None
        return info


    def __id2url_converter(self, category:str, exchanges:str) -> str:
        category_name = self.__get_category_from_str(category).name
        exchange_id = self.__get_exchange_from_str(exchanges).value
        url = f"{self.base_url}?keyword=&category=%5E{category_name}&stcid={exchange_id}&search=T%C3%ACm+Ki%E1%BA%BFm"
        return url


    def __get_info_table(self, url:str):
        request = requests.get(url, verify=False)
        soup = BeautifulSoup(request.content, "lxml", from_encoding="utf-8")
        table = soup.find_all('table')
        return table[3]


    def __get_category_from_str(self, str:str) -> Category:
        try:
            return Category[str.lower()]
        except KeyError:
            self.logger.error(f"Exception occur: category {str} is not existed!")
            raise StockInfoCrawlerException(f"Exception occur: category {str} is not existed!")


    def __get_exchange_from_str(self, str:str) -> Exchange:
        try:
            return Exchange[str.upper()]
        except KeyError:
            self.logger.error(f"Exception occur: category {str} is not existed!")
            raise StockInfoCrawlerException(f"Exception occur: category {str} is not existed!")


def argument_parser():
    """
    Add CLI argument parser
    """
    parser = argparse.ArgumentParser(description='Tool crawl stock price from cophieu68')
    parser.add_argument("-c", "--category", action='store', type=str,
                        dest='category_name', required=False, help='The name of category')
    parser.add_argument("-e", "--exchanges", action='store', type=str,
                        dest='exchanges_name', required=False, help='The exchanges name')
    parser.add_argument("-a", "--all", action='store_true', default=True,
                        dest='crawl_all', required=False, help='Crawl all info')
    parser.add_argument("-o", "--output", action='store', choices=['csv', 'db'], default='csv',
                        nargs='?', dest='output', required=False, help='export to CSV or DB')
    parser.add_argument("-d", "--data-dir", action='store', type=str, default=DATA_DIR,
                        dest='data_dir', required=False, help=f'The location store data, default location: {DATA_DIR}')
    parser.add_argument("-l", "--list", action='store_true',
                        dest='list', required=False, help='Show list of Exchanges and Categories')
    return parser


def print_list_category():
    """
    Print console list of exchanges and categories
    """
    return """=== List of Exchanges ==="
{}
=== List of Categories ===
{}
""".format("\t".join([exchange.name for exchange in Exchange]),
"\n".join([f"{category.name:15} {category.value}" for category in Category]))


def main():
    """
    Main function for control
    """
    parser = argument_parser()
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_usage()
        return 0
    if args.list:
        print(print_list_category())
        return 0
    if not args.crawl_all and (not args.category_name or not args.exchanges_name):
        print("Missing category name or exchanges name!")
        parser.print_help()
        return 0
    logger = get_logger(MODULE_NAME)
    start_logging()
    try:
        stock_info_crawler = StockInfoCrawler(logger)
        if args.crawl_all:
            info_list = stock_info_crawler.crawl_all()
        else:
            info_list = stock_info_crawler.parse_info_table(category=args.category_name,
                                                        exchanges=args.exchanges_name)
        if args.output == 'csv':
            str_time = time.strftime(r"%Y-%m-%d_%H%M%S", time.gmtime())
            if args.category_name and args.exchanges_name:
                file = f"info_{args.category_name}_{args.exchanges_name}_{str_time}.csv"
            else:
                file = f"info_all_{str_time}.csv"
            stock_info_crawler.save_to_csv(info_list, file, args.data_dir)
        elif args.output == 'db':
            logger.info("In development")
    except KeyboardInterrupt:
        logger.info("Runner exit manually (KeyboardInterrupt)")
        return 0
    except Exception as ex:
        logger.debug(ex)
        raise ex


if __name__ == "__main__":
    logger = get_logger(MODULE_NAME)
    start_logging()
    #main()
    stock_info_crawler = StockInfoCrawler(logger)
    info_list = stock_info_crawler.crawl_all()
    print(info_list)