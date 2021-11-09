# Data-Engineering-Project
Mini data pipeline for Data Engineering subject

## Functional

### 🤖 Crawler

#### 💸 Crawl stock price 💸

- Crawl all stock

It will gather all of the company's stock price data dating back to the first day the stock was publicly traded and save it in the target folder. The file will be in `csv` format, with the name `<company name>_stock_price.csv`.

Command `python -m dep.crawler.stock_price -i aaa`

- Crawl from the latest date in DB to the latest date in website

Example: In the DB, the latest day is 2021-10-20 and the latest date in website is 2021-11-20. It'll gather all the stock price data of the company from 2021-10-21 to 2021-11-20.

Command `python -m dep.crawler.stock_price -l -i aaa`

For more infomation and usage, run `python -m dep.crawler.stock_price -h`

#### 📜 Crawl stock info 📜

- Crawl by category and exchanges

Example: To crawl all corporate information in `HOSE` exchange that belong to the `bds` (Bất Động Sản) category.

Run command `python -m dep.crawler.stock_info -c bds -e hose`

It'll crawl all data suitable to conditions and store in `.csv` format at default folder (data folder) with name `info_bds_hose_2021-11-09_144249.csv`.

- Crawl all infomation

Run command `python -m dep.crawler.stock_info -a`

It'll crawl all data and store in `.csv` format at default folder (data folder) with name `info_all_2021-11-09_144249.csv`.

For more infomation, run `python -m dep.crawler.stock_info -h`
