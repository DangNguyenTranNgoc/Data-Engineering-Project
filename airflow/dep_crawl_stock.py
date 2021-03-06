from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from dep.auto.crawl_price import CrawlPriceAutomation

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='dep_crawl_stock',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['dep'],
) as dag:


    def crawl_stock():
        """
        Crawl stock with stock code get from database
        """
        crawl_price_auto = CrawlPriceAutomation()
        # Get enviroment variable
        host = Variable.get("DATABASE_HOST")
        port = Variable.get("DATABASE_PORT")
        database_name = Variable.get("DATABASE_NAME")
        user = Variable.get("DATABASE_USER")
        password = Variable.get("DATABASE_PASSWORD")
        crawl_price_auto.database.set_param_from_airflow(host, port, database_name, user, password)
        # Create tabel if needed
        # crawl_price_auto.database.init_database()
        crawl_price_auto.crawl_all_stock_from_db()


    run_this = PythonOperator(
        task_id='crawl_stock',
        python_callable=crawl_stock,
    )


    run_this
