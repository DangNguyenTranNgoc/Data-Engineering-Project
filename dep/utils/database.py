#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

MODULE_NAME = "dep.utils.database"
dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')

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

    def __init__(self) -> None:
        self.logger = logging.getLogger(MODULE_NAME)
        self.connect()


    def connect(self):
        """
        Connect to the PostgreSQL database server
        """
        self.conn = None
        try:
            # connect to the PostgreSQL server
            self.conn = psycopg2.connect(host=DATABASE_HOST,
                                    database=DATABASE_NAME,
                                    user=DATABASE_USER,
                                    password=DATABASE_PASSWORD)
            # create a cursor
            self.cur = self.conn.cursor()
            # execute a statement
            self.logger.info('PostgreSQL database version:')
            self.cur.execute('SELECT version()')
            # display the PostgreSQL database server version
            db_version = self.cur.fetchone()
            self.logger.info(db_version[0])
        except (Exception, psycopg2.DatabaseError) as error:
            self.close_connect()
            raise DatabaseException(error)


    def close_connect(self):
        """
        Close database connection
        """
        try:
            self.cur.close()
            self.conn.close()
            self.logger.verbose('Database connection closed.')
        except (Exception, psycopg2.DatabaseError) as error:
            raise DatabaseException(error)


    def get_one(self, query:str):
        """
        Execute query and get one result
        """
        self.__exec(query)
        return self.cur.fetchone()

    
    def get_all(self, query:str):
        """
        Execute query and get all result
        """
        self.__exec(query)
        return self.cur.fetchall()


    def __exec(self, query:str):
        try:
            if (self.conn is None) or (self.cur is None):
                raise DatabaseException("Connection not created, please run function 'connect'")
            self.logger.verbose(f"Excute query `{query}`")
            self.cur.execute(query)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.close_connect()
            raise DatabaseException(error)
    

    
