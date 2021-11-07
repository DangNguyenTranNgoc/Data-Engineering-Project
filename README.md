# Data-Engineering-Project
Mini data pipeline for Data Engineering subject

## Functional

### Crawler

#### Crawl stock price

- Crawl all stock
- Crawl from the latest date in DB to the latest date in website

Using:

- Move console to the Data-Engineering-Project folder
- Run:python -m dep.crawler.stock_price [command]
```
With: usage: stock_price.py [-h] -i STOCK_ID [-d DATA_DIR] [-l] [-f FILE_NAME]

Tool crawl stock price from cophieu68

optional arguments:
  -h, --help            show this help message and exit
  -i STOCK_ID, --id STOCK_ID
                        The id of the stock
  -d DATA_DIR, --data-dir DATA_DIR
                        The location store data
  -l, --latest          Is crawl from the latest date in DB?
  -f FILE_NAME, --file-name FILE_NAME
                        The name of the exported data file
```
