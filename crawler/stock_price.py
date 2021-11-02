#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re


BASE_URL = r'https://www.cophieu68.vn/historyprice.php'
LAST_INDEX = -1
DATETIME_FORMAT = r"%d-%m-%Y"
PAGE_NUMBER_FILTER = r'(currentPage=)([\d]+)'
CHARACTER_IGNORE = ['\n']
PRICE_DF_HEADER = ["date", "ref_price", "diff_price", "diff_price_rat",
                "close_price", "vol", "open_price", "highest_price",
                "lowest_price", "transaction", "foreign_buy", "foreign_sell"]


def datetime_parser(date_str:str) -> datetime:
    """
    Parse dd-mm-yyyy string to datetime object
    """
    try:
        datetime_obj = datetime.strptime(date_str, DATETIME_FORMAT)
    except ValueError as error:
        print(error)
        datetime_obj = None
    return datetime_obj

    
def str2fl_parser(str_number:str, comma=False) -> float:
    """
    Parser (str) 17.50 to (float) 17.5
    """
    try:
        _str = str_number
        if comma:
            _str = _str.replace(",", "")
        number = float(_str)
    except ValueError as error:
        print(error)
        number = None
    return number


def record_parser(row) -> list:
    """
    Parse a row of price table. EX:
    <tr>
        <td class="td_bottom3 td_bg1">#1</td>
        <td class="td_bottom3 td_bg1" nowrap="nowrap">01-11-2021</td>
        <td class="td_bottom3 td_bg1" align="right">17.20</td>
        <td class="td_bottom3 td_bg2" align="right" nowrap="nowrap">
            <span class="priceup">0.35</span>
        </td>
        <td class="td_bottom3 td_bg2" align="right" nowrap="nowrap">
            <span class="priceup">2.03%</span>
        </td>
        <td class="td_bottom3 td_bg2" align="right">
            <span class="priceup"><strong>17.55</strong></span>
        </td>
        <td class="td_bottom3 td_bg1" align="right">13,662,600</td>
        <td class="td_bottom3 td_bg2" align="right"><span class="pricedown">17.20</span></td>
        <td class="td_bottom3 td_bg2" align="right"><span class="priceup">17.85</span></td>
        <td class="td_bottom3 td_bg2" align="right"><span class="pricedown">16.95</span></td>
        <td class="td_bottom3 td_bg1" align="right">0</td>
        <td class="td_bottom3 td_bg1" align="right">204,600</td>
        <td class="td_bottom3 td_bg1" align="right">27,500</td>
    </tr>
    """
    try:
        date = datetime_parser(row.contents[3].text.strip())
        ref_price = str2fl_parser(row.contents[5].text.strip())
        diff_price = str2fl_parser(row.contents[7].text.strip())
        diff_price_rat = str2fl_parser(row.contents[9].text.strip()[:-1])
        close_price = str2fl_parser(row.contents[11].text.strip())
        vol = str2fl_parser(row.contents[13].text.strip(), comma=True)
        open_price = str2fl_parser(row.contents[15].text.strip())
        highest_price = str2fl_parser(row.contents[17].text.strip())
        lowest_price = str2fl_parser(row.contents[19].text.strip())
        transaction = str2fl_parser(row.contents[21].text.strip(), comma=True)
        foreign_buy = str2fl_parser(row.contents[23].text.strip(), comma=True)
        foreign_sell = str2fl_parser(row.contents[25].text.strip(), comma=True)
        record = [date, ref_price, diff_price, diff_price_rat, 
                close_price, vol, open_price, highest_price, 
                lowest_price, transaction, foreign_buy, foreign_sell]
        if record.count(None) == len(record):
            record = None
    except AttributeError as error:
        print(error)
        record = None
    except IndexError as error:
        print(error)
        record = None
    return record


def get_record_date(row) -> datetime:
    """
    Return the date of the record
    """
    try:
        date = datetime_parser(row.contents[3].text.strip())
    except AttributeError as error:
        print(error)
        date = None
    return date


def get_page_num_from_url(link:str) -> int:
    """
    Return page number from link 
    Ex: https://www.cophieu68.vn/historyprice.php?currentPage=29&id=aaa
    => 29
    """
    page_num = re.search(PAGE_NUMBER_FILTER, link)
    if page_num is not None:
        return page_num[2]
    return None


def crawl_page(link) -> list:
    """
    Crawl price from a page
    """
    try:
        request = requests.get(link, verify=False)
        soup = BeautifulSoup(request.content, from_encoding="utf-8")
        price_content = soup.find('div', {'id':'content'})
        price_list = price_table_parser(price_content.table)
        if not price_list:
            price_list = None
    except Exception as error:
        print(error)
        price_list = None
    return price_list


def crawl_all(link) -> list:
    """
    Crawl all price from the last page.
    link: https://www.cophieu68.vn/historyprice.php?id=aaa
    => base: https://www.cophieu68.vn/historyprice.php
    => id: aaa
    """
    id = 'aaa'
    request = requests.get("{}?id=aaa".format(BASE_URL), verify=False)
    soup = BeautifulSoup(request.content, from_encoding="utf-8")
    price_content = soup.find('div', {'id':'content'})
    last_page = price_content.ul.contents[-1]
    # Parse current page
    price_list = []
    price_list.append(price_table_parser(price_content.table))
    # Parse to the last
    for i in range(2, get_page_num_from_url(last_page) + 1):
        url = "{}?currentPage={}?id={}".format(BASE_URL, i, id)
        _price_list = crawl_page(url)
        if _price_list is not None:
            price_list.append(_price_list)
    return price_list


def price_table_parser(price_table) -> list:
    """
    Parse price table to list
    """
    price_list = []
    for row in price_table.contents:
        if row in CHARACTER_IGNORE:
            continue
        price_row = record_parser(row)
        if price_row is not None:
            price_list.append(price_row)
    return price_list


# instead of provide url, just provide id of stock
price_list = crawl_all(r'https://www.cophieu68.vn/historyprice.php?id=aaa')
price_df = pd.DataFrame(price_list, columns=PRICE_DF_HEADER)
price_df = price_df.sort_values(by='date')
print(price_df)
price_df.to_csv(r"D:\test.csv", index=False)