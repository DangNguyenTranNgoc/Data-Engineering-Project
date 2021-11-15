#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import yaml
import logging
import tempfile
from enum import Enum
from datetime import date, datetime

from dep.utils.lock_file import FileLock, FileLockException

DATA_DIR = f"data"
FILE_NAME = "metadata.yaml"
MODULE_NAME = "dep.utils.data_info_handler"
LOCKFILE_TIMEOUT = 60 # Wait for lock file 1 minute


class DataInfoHandlerException(Exception):
    """
    Base class for errors in StockInfoCrawler
    """
    def __init__(self, value):
        self.value = value


    def __str__(self):
        return repr(self.value)


class DataInfoTypeException(TypeError):
    """
    Base class for type check
    """
    def __init__(self, expected:type, receive):
        self.expected = expected
        self.receive = receive


    def __str__(self):
        return repr(f"Expected {self.expected}, but receive {type(self.receive)}.")


class MetaDataType(Enum):
    """
    Type of metadata
    """
    STOCK_INFO = "stock-info"
    STOCK_PRICE = "stock-price"


class MetaDataKey(Enum):
    """
    Define key for metadata file.
    """
    DATE = "date"
    FROM_DATE = "from-date"
    TO_DATE = "to-date"
    FILE_PATH = "file-path"
    RECORD_NUMER = "record-number"


"""
Clock metadata file if needed
https://programmer.group/a-python-implementation-of-lock-file.html
Save file:
1/ Normal
- Crate .lock file
- Save metadata file.
- Delete .lock file
2/ UnNormal
- Wait for .lock file release.
    - If timeout => write to tmp file => throw Exception
- Crate .lock file
- Save metadata file.
- Delete .lock file
"""
class DataInfoHandler():
    """
    Handler for data/data_info.yaml file.
    This file is store metadate for stock info csv files and stock price csv files crawled by automation.
    """
    def __init__(self) -> None:
        self.log = logging.getLogger(MODULE_NAME)
        self.__data_dir = DATA_DIR
        self.__file_name = FILE_NAME
        self.__file = f"{self.__data_dir}{os.sep}{self.__file_name}"
        self.metadata = self.load_data()


    def load_data(self) -> dict:
        """
        Load data from default file to self.data
        """
        if os.path.isfile(self.__file):
            try:
                self.log.verbose(f"Load metadata at {self.__file}.")
                with open(self.__file, "r") as file:
                    metadata = yaml.safe_load(file)
                self.__validate_metadata(metadata)
            except Exception as ex:
                self.log.error(f"Exception occur: {ex}")
                raise ex
        else:
            self.log.verbose(f"File {self.__file} is not existed. Create new data.")
            metadata = {MetaDataType.STOCK_INFO.value: {},
                        MetaDataType.STOCK_PRICE.value: {}}
        return metadata


    def get_first_record(self, metadata_type:MetaDataType, stock_id:str) -> dict:
        """
        Get the first record (the first item) of a stock id.
        If stock is not existed, return None
        """
        if not isinstance(metadata_type, MetaDataType):
            raise DataInfoTypeException(MetaDataType, type(metadata_type))
        if not stock_id in self.metadata[metadata_type.value]:
            return None
        self.log.verbose(f"Get the first record of '{stock_id}', type '{metadata_type.value}'")
        return self.metadata[metadata_type.value][stock_id][0]


    def pop_record(self, metadata_type:MetaDataType, stock_id:str) -> dict:
        """
        Return the first record (the first item) of a stock id if existed then remove it.
        """
        if not isinstance(metadata_type, MetaDataType):
            raise DataInfoTypeException(MetaDataType, type(metadata_type))
        metadata_type = metadata_type.value
        if not stock_id in self.metadata[metadata_type]:
            return None
        record = self.metadata[metadata_type][stock_id].pop()
        return record


    def save_file(self):
        """
        Save data to default file.
        If file existed:
        - Rename current file to *.old
        - Dump data
        - Remove *.old file
        - If execption occur, remove dumped file and rename *.old to metadata file
        If file not existed:
        - Dump data
        - If execption occur, remove dumped file.
        """
        with FileLock("save_metadata", timeout=LOCKFILE_TIMEOUT):
            try:
                self.__validate_metadata(self.metadata)
                if os.path.isfile(self.__file):
                    self.log.verbose(f"File {self.__file} is existed. Backup it.")
                    try:
                        old_file = f"{self.__file}.old"
                        os.rename(self.__file, old_file)
                        with open(self.__file, "w") as file:
                            yaml.dump(self.metadata, file, default_flow_style=False, sort_keys=False)
                        self.log.verbose(f"Done. Remove old file.")
                        os.remove(old_file)
                    except Exception as ex:
                        self.log.error(f"Exception occur: {ex}")
                        os.remove(self.__file)
                        os.rename(old_file, self.__file)
                        raise ex
                else:
                    self.log.verbose(f"File {self.__file} is not existed. Create new.")
                    try:
                        with open(self.__file, "w") as file:
                            yaml.dump(self.metadata, file, default_flow_style=False, sort_keys=False)
                    except Exception as ex:
                        self.log.error(f"Exception occur: {ex}")
                        os.remove(self.__file)
                        raise ex
            except FileLockException as ex:
                self.log.warning(f"Waiting for release file timeout, create temp file")
                with open(tempfile.NamedTemporaryFile(), "w") as file:
                    yaml.dump(self.metadata, file, default_flow_style=False, sort_keys=False)


    def append_metadata(self, *, metadata_type:MetaDataType, stock_id:str, crawl_date:datetime,
                        from_date:date, to_date:date,
                        file_path:str, record_number:int):
        """
        Append new metadata of a file to default file
        """
        if not isinstance(metadata_type, MetaDataType):
            raise DataInfoTypeException(MetaDataType, type(metadata_type))
        if not isinstance(stock_id, str):
            raise DataInfoTypeException(str, type(stock_id))
        if not isinstance(crawl_date, datetime):
            raise DataInfoTypeException(datetime, type(crawl_date))
        if not isinstance(from_date, date):
            raise DataInfoTypeException(date, type())
        if not isinstance(to_date, date):
            raise DataInfoTypeException(date, type(to_date))
        if not isinstance(file_path, str):
            raise DataInfoTypeException(str, type(file_path))
        if not isinstance(record_number, int):
            raise DataInfoTypeException(int, type(record_number))
        record = self.__record_builder(date=crawl_date, from_date=from_date, to_date=to_date,
                                       file_path=file_path, record_number=record_number)
        if not stock_id in self.metadata[metadata_type.value]:
            self.log.verbose(f"Key {stock_id} is not existed! Create new")
            self.metadata[metadata_type.value][stock_id] = []
            self.metadata[metadata_type.value][stock_id].append(record)
        else:
            self.metadata[metadata_type.value][stock_id].append(record)


    def show_data(self):
        """
        Print data pretty. This function for testing.
        """
        self.log.info(yaml.dump(self.metadata))


    def __record_builder(self, *, date:datetime, from_date:date, to_date:date,
                         file_path:str, record_number:int):
        """
        Verify and build a record (a dict)
        """
        if (date.date() < to_date):
            raise DataInfoHandlerException(
                f"date ({date}) must greater than to-date ({to_date})."
            )
        if (to_date < from_date):
            raise DataInfoHandlerException(
                f"to-date ({to_date}) must greater than to-date ({from_date})."
            )
        if not os.path.isfile(file_path):
            raise DataInfoHandlerException(f"File {file_path} is not existed.")
        return {
            MetaDataKey.DATE.value: date,
            MetaDataKey.FROM_DATE.value: from_date,
            MetaDataKey.TO_DATE.value: to_date,
            MetaDataKey.FILE_PATH.value: file_path,
            MetaDataKey.RECORD_NUMER.value: record_number
        }


    def __validate_metadata(self, data:dict) -> bool:
        if not isinstance(data, dict):
            raise DataInfoTypeException(dict, data)
        for data_type in MetaDataType:
            if not data_type.value in data:
                raise DataInfoHandlerException(f"Missing '{data_type.value}' in metadata.")
            if not isinstance(data[data_type.value], dict):
                raise DataInfoTypeException(dict, data[data_type.value])
            if len(data[data_type.value]) == 0:
                continue
            null_stock = []
            for idx, stock in data[data_type.value].items():
                if not isinstance(stock, list):
                    raise DataInfoTypeException(list, stock)
                if len(stock) == 0:
                    null_stock.append(idx)
                    continue
                for record in stock:
                    self.__validate_record(record)
                    if not os.path.isfile(record[MetaDataKey.FILE_PATH.value]):
                        self.log.warning(f"File {record[MetaDataKey.FILE_PATH.value]} is not existed. Remove record.")
                        stock.remove(record)
            if null_stock:
                self.log.warning(f"Remove stock id with null: {', '.join(null_stock)}.")
                for stock in null_stock:
                    del data[data_type.value][stock]


    def __validate_record(self, record:dict):
        for key in MetaDataKey:
            if not key.value in record:
                raise DataInfoHandlerException(f"Missing '{key}' in record.")
            if not isinstance(record[MetaDataKey.DATE.value], datetime):
                raise DataInfoTypeException(datetime, record[MetaDataKey.DATE.value])
            if not isinstance(record[MetaDataKey.FROM_DATE.value], date):
                raise DataInfoTypeException(date, record[MetaDataKey.FROM_DATE.value])
            if not isinstance(record[MetaDataKey.TO_DATE.value], date):
                raise DataInfoTypeException(date, record[MetaDataKey.TO_DATE.value])
            if not isinstance(record[MetaDataKey.FILE_PATH.value], str):
                raise DataInfoTypeException(str, record[MetaDataKey.FILE_PATH.value])
            if not isinstance(record[MetaDataKey.RECORD_NUMER.value], int):
                raise DataInfoTypeException(int, record[MetaDataKey.RECORD_NUMER.value])
            # Check date
            if (record[MetaDataKey.DATE.value].date() < record[MetaDataKey.TO_DATE.value]):
                raise DataInfoHandlerException(
                    f"date ({record[MetaDataKey.DATE.value]}) must greater than to-date ({record[MetaDataKey.TO_DATE.value]})."
                )
            if (record[MetaDataKey.TO_DATE.value] < record[MetaDataKey.FROM_DATE.value]):
                raise DataInfoHandlerException(
                    f"to-date ({record[MetaDataKey.TO_DATE.value]}) must greater than to-date ({record[MetaDataKey.FROM_DATE.value]})."
                )
