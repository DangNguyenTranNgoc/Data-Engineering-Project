#!/usr/bin/env python
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import pandas as pd
import os

nn_url = r'https://www.cophieu68.vn/companylist.php?keyword=&category=%5Enganhang&stcid=1&search=T%C3%ACm+Ki%E1%BA%BFm'
bds_url = r'https://www.cophieu68.vn/companylist.php?keyword=&category=%5Ebds&stcid=1&search=T%C3%ACm+Ki%E1%BA%BFm'
congnghe_url = r'https://www.cophieu68.vn/companylist.php?keyword=&category=%5Econgnghe&stcid=3&search=T%C3%ACm+Ki%E1%BA%BFm'

URL = congnghe_url
CATEGORY = 'congnghe'
EXCHANGES = 'UPCOM'
IGNORNE_CHAR = ['\n']
INFO_HEADER = ["Mã CK", "Tên Công Ty", "Ngày GDĐT", "KLNY LầnĐầu", "Giá NY", "KL ĐangLưuHành", "Cổ Phiếu Quỹ", "Khối Lượng Niêm yết", "NN được sở hữu", "Tỷ Lệ NN Được sở Hữu", "NN còn được phép mua", "Giá HT", "Vốn Thị Trường", "Nhóm", "Sàn Giao Dịch"]
DATA_FOLDER = "data"
FILE_NAME = "congnghe_upcom"

def tr_parser(tr, category:str='', exchanges:str='') -> list:
    try:
        id = tr.contents[3].text.strip()
        name = tr.contents[3].a['title'].strip()
        pub_date = tr.contents[7].text.strip()
        first_vol = convert_str2fl(tr.contents[9].text.strip())
        pub_price = convert_str2fl(tr.contents[11].text.strip())
        curr_vol = convert_str2fl(tr.contents[13].text.strip())
        treasury_shares = convert_str2fl(tr.contents[15].text.strip())
        pub_vol = convert_str2fl(tr.contents[17].text.strip())
        fr_owner = convert_str2fl(tr.contents[19].text.strip())
        fr_owner_rat = fr_owner/pub_vol*100
        fr_vol_remain = convert_str2fl(tr.contents[21].contents[0].strip())
        curr_price = convert_str2fl(tr.contents[23].strong.text.strip())
        market_capital = convert_str2fl(tr.contents[25].text.strip())

        info = [id, name, pub_date, first_vol, pub_price, curr_vol, treasury_shares, pub_vol, fr_owner, fr_owner_rat, fr_vol_remain, curr_price, market_capital, category, exchanges]
    except Exception as ex:
        print(ex)
        info = None
    
    return info



def convert_str2fl(str:str) -> float:
    try:
        string = str.replace(',','')
        number = float(string)
    except TypeError as err:
        print(err)
        number = 0.0
    return number


r = requests.get(URL, verify=False)
soup = BeautifulSoup(r.content, "lxml", from_encoding="utf-8")

tables = soup.find_all('table')
# Number 3 (or item 4th) based on html structure
info_table = tables[3]
info_list = []

for tr in info_table.children:
    if tr not in IGNORNE_CHAR:
        info = tr_parser(tr, CATEGORY, EXCHANGES)
        if info:
            info_list.append(info)

info_df = pd.DataFrame(info_list, columns=INFO_HEADER)
print(info_df)

info_df.to_csv('..{}{}{}{}.csv'.format(os.sep, DATA_FOLDER, os.sep, FILE_NAME), encoding='utf-8', index=False)
