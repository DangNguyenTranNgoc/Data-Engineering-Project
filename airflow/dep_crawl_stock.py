from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dep.auto.crawl_price import CrawlPriceAutomation

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='dep_crawl_stock',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:


    def print_context(ds, **kwargs):
        """
        Print the Airflow context and ds variable from the context.
        """
        crawl_price_auto = CrawlPriceAutomation()
        crawl_price_auto.database.init_database()
        crawl_price_auto.crawl_all_stock_from_db()


    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )


    run_this
