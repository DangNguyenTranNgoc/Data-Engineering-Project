#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging
import pandas as pd
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine


DATA_DIR = f"data"
MODULE_NAME = "dep.utils.database"
dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)
DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
DATA_PATH = f"data{os.sep}stock.db"


class DatabaseException(Exception):
    """
    Base exception for database utils
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class Database():
    """
    Class utils for working with PostgreSQL server
    """
    
    def __init__(self, test_db=False) -> None:
        self.logger = logging.getLogger(MODULE_NAME)
        self.db_type = test_db
        self.engine = self.__prepare_engine()
    
    
    def query_data(self, query:str) -> pd.DataFrame:
        """
        Query data to pandas
        """
        self.logger.verbose(f"Excute query: {query}")
        return pd.read_sql_query(query, self.engine)
    

    def excute_query(self, query:str):
        """
        Excute the query.
        """
        self.logger.verbose(f"Excute query: {query}")
        with self.engine.connect() as conn:
            conn.execute(query)

    
    def insert_data(self, data:pd.DataFrame, table:str):
        """
        Insert data into table.
        """
        self.logger.verbose(f"Insert data to table '{table}'")
        data.to_sql(table, self.engine, if_exists='append', index=False, index_label="id")
    
    
    def get_latest_date(self, stock_id:str) -> date:
        """
        Get the last date of a stock
        """
        query = (
            f"SELECT date "
            f"FROM stock_price "
            f"WHERE code = '{stock_id}' "
            f"ORDER BY date DESC LIMIT 1"
        )
        self.logger.verbose(f"Excute query: {query}")
        data = pd.read_sql_query(query, self.engine)
        if not data.empty:
            data['date'] = pd.to_datetime(data['date'])
            self.logger.verbose(f"The latest date of '{stock_id}' is {data.iloc[0]['date'].date()}")
            return data.iloc[0]['date'].date()
        return None
    
    
    def get_all_code(self) -> list:
        """
        Get all stock's codes
        """
        query = r"SELECT DISTINCT code FROM stock_info"
        self.logger.verbose(f"Excute query: {query}")
        data = pd.read_sql_query(query, self.engine)
        if not data.empty:
            return data['code'].tolist()
        return None
    
    
    def init_database(self):
        self.logger.info("Initialize the database")
        if self.db_type:
            for query in self.__init_sqlite():
                with self.engine.connect() as conn:
                    self.logger.verbose(f"Excute query: {query}")
                    conn.execute(query)
        else:
            for query in self.__init_postgresql():
                with self.engine.connect() as conn:
                    self.logger.verbose(f"Excute query: {query}")
                    conn.execute(query)
    
    
    def __prepare_engine(self):
        if self.db_type:
            self.logger.verbose("Repare connection to SQLite")
            connect_string = fr"sqlite:///{DATA_PATH}"
        else:
            self.logger.verbose("Repare connection to PostgreSQL")
            connect_string = (
                f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@"
                f"{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
            )
        return create_engine(connect_string)

    
    def __init_sqlite(self):
        return [(
            f"CREATE TABLE IF NOT EXISTS stock_info (id integer PRIMARY KEY, code character(5) NOT NULL, name character(255) NOT NULL, pub_date date NOT NULL, first_vol real NOT NULL, pub_price real NOT NULL, curr_vol real NOT NULL, treasury_shares real NOT NULL, pub_vol real NOT NULL, fr_owner real NOT NULL, fr_owner_rat real NOT NULL, fr_vol_remain real NOT NULL, curr_price real NOT NULL, market_capital real NOT NULL, category character(255) NOT NULL, exchanges character(255) NOT NULL);"), (
            f"CREATE TABLE IF NOT EXISTS stock_price (id integer PRIMARY KEY, code character(5) NOT NULL, date date NOT NULL, ref_price real NOT NULL, diff_price real NOT NULL, diff_price_rat real NOT NULL, close_price real NOT NULL, vol integer NOT NULL, open_price real NOT NULL, highest_price real NOT NULL, lowest_price real NOT NULL, transactions integer NOT NULL, foreign_buy integer NOT NULL, foreign_sell integer NOT NULL);"
        )]
    

    def __init_postgresql(self):
        return [(
            f"CREATE TABLE IF NOT EXISTS public.stock_info (id serial PRIMARY KEY, code character(5) NOT NULL, name character(255) NOT NULL, pub_date date NOT NULL, first_vol real NOT NULL, pub_price real NOT NULL, curr_vol real NOT NULL, treasury_shares real NOT NULL, pub_vol real NOT NULL, fr_owner real NOT NULL, fr_owner_rat real NOT NULL, fr_vol_remain real NOT NULL, curr_price real NOT NULL, market_capital real NOT NULL, category character(255) NOT NULL, exchanges character(255) NOT NULL);"), (
            f"CREATE TABLE IF NOT EXISTS public.stock_price (id serial PRIMARY KEY, code character(5) NOT NULL, date date NOT NULL, ref_price real NOT NULL, diff_price real NOT NULL, diff_price_rat real NOT NULL, close_price real NOT NULL, vol integer NOT NULL, open_price real NOT NULL, highest_price real NOT NULL, lowest_price real NOT NULL, transactions integer NOT NULL, foreign_buy integer NOT NULL, foreign_sell integer NOT NULL);"
        )]
