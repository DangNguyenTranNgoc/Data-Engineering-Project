# Data-Engineering-Project
Mini data pipeline for Data Engineering subject

## Functional

### 💾 Repare 🔧

The database using is PostgreSQL. The create database and table command is on file `create_table.sql` at `data` folder.

The needed libraries info stored in file `requirement.txt`. Install it before start.

Database info is stored in file `.env`. Change it for suitable with the current system.

### 🤖 Crawler 🤖


#### 🚀 Automation point for tool (like Airflow) 🚀

Located in `dep.auto`. File `crawl_price.py` is ready for run with stock's id (i.e. fox, aaa, abc, ...) from user input.

#### 💸 Crawl stock price 💸

- Crawl all stock

It will gather all of the company's stock price data dating back to the first day the stock was publicly traded and save it in the target folder. The file will be in `csv` format, with the name `<company name>_stock_price.csv`.

Command `python -m dep.crawler.stock_price -i aaa`

- Crawl from the date input to the latest date in website

Example: User input 2021-10-20 and the latest date in website is 2021-11-20. It'll gather all the stock price data of the company from 2021-10-21 to 2021-11-20.

Command `python -m dep.crawler.stock_price -i aaa --from-date 20-10-2021`

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
