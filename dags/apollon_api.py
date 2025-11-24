import sys
import pendulum
local_tz = pendulum.timezone("Europe/Berlin")
from datetime import timedelta
from pathlib import Path  
from airflow.decorators import dag, task

sys.path.append(Path("/usr/local/airflow/include/scripts/"))
 
from include.tasks.dag_tasks import get_today_transactionSales
from include.tasks.dag_tasks import get_stockes_from_api
from include.tasks.dag_tasks import get_article_stock_all_stores
from include.tasks.dag_tasks import stock_diff
  
# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'email_on_failure': True,
    #'email_on_retry': True,    
    'retries': 1,  # Number of times to retry on failure
    'retry_delay': timedelta(seconds=10),  # Delay between retries
    #'email': ['beikzadeh@etos.de']       
}

@dag(default_args=default_args, start_date=pendulum.datetime(2025, 1, 1, tz=local_tz), schedule_interval='0 9 * * *', catchup=False )
def ETL():
    # Task 1        
    @task(task_id='get_stocks_from_API')
    def stock_():
        get_stockes_from_api(customer='22001')

    # Task 2              
    @task(task_id='pivot_article_stock_all_stores')
    def stock_all_stores():
        get_article_stock_all_stores(customer='22001')

    # Task 3
    @task(task_id='get_yesterday_transaction_sales')
    def transaction_sales():
        get_today_transactionSales(customer='22001')

    # Task 4
    @task(task_id='two_days_stock_diff')
    def stock_transactions():        
        stock_diff(customer='22001')

    stock_() >> (stock_all_stores() , transaction_sales()) >> stock_transactions()

ETL()
