from plugins.g_utils import *
from include.stock_api import stock_api
from include.sales import salesDatabase
from include.stocks import stocksDatabase

customer = '22001'

# Task 1
def get_stockes_from_api(customer: str):
    stock_api(customer=customer).get_stockes_from_api()          
    
# Task 2
def get_article_stock_all_stores(customer: str) -> pl.DataFrame:           
    stocksDatabase(customerId=customer).daily_stocks().save()

#Task 3
def get_today_transactionSales(customer: str):        
    salesDatabase(customerId=customer).daily_sales().save()

# Task 4
def stock_diff(customer: str = customer):
    stocksDatabase(customerId=customer).two_daily_stock_changes().save()
    