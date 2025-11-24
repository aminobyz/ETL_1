from pathlib import Path
from datetime import datetime

import polars as pl 
from sqlalchemy import create_engine 
from collections import Counter

# Check if the a directory exists 
def check_Path_exits(root_name: str, customerId: str, current_year: str, current_month: str , current_day: str):
    # Check if the transactionSales directory exists
    directory_path = Path(f"/usr/local/airflow/include/data/{customerId}/{root_name}/{current_year}/{current_month}/{current_day}")
    if not directory_path.exists():
        directory_path.mkdir(parents=True, exist_ok=True)

        print(f"\033[32mDirectory {directory_path} created.\033[0m")
    else:
        print(f"\033[33mDirectory {directory_path} already exists.\033[0m")

    return directory_path 

# Get current date details
def get_current_date():     
    date = datetime.now()
    return date.strftime('%Y-%m-%d %H:%M:%S'), date.strftime('%Y-%m-%d'), date.strftime("%Y%m%d"), date.strftime('%Y'), date.strftime('%B'), date.strftime('%d')


def get_last_two_working_days(customerId: str):
        last_20_executions_date = (pl.scan_parquet(f"/usr/local/airflow/include/data/{customerId}/last_20_executions_date.parquet")
                                                                .with_columns(pl.col('execution_timestamp').dt.date().alias('date')).sort("execution_timestamp").collect())
        print(f"last_20_executions_date: {last_20_executions_date}")

        _, current_year, current_month, current_day, current_timestamp, current_date = last_20_executions_date.row(-1)
        # print(last_20_executions_date.row(-1))
        last_execution_date_path = f"/usr/local/airflow/include/data/{customerId}/stocks/{current_year}/{current_month}/{current_day}"
        #date = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S').date()

        if current_date not in last_20_executions_date['date'].to_list() :
             assert f"\033[31mThe current date {current_date} is not in the last_20_executions_date list, please check the last_20_executions_date.parquet file.\033[0m"

        if not Path(f"{last_execution_date_path}/{current_date.strftime('%Y%m%d')}.parquet").is_file():
            print(f"\033[33mStock data for the current execution date {current_date} already does not exist at {last_execution_date_path}, looking for the last two working days before {current_date}.\033[0m")

        print(f"\033[32mCurrent execution date found: {current_date}\033[0m")
             
        # # last execution date
        last_20_executions_date_list =last_20_executions_date['date'].to_list()       
        a_day_before = []
       
        for i in range(1, 21):
                i_day_before = last_20_executions_date.filter(pl.col("date") == current_date).select(pl.col('date') - pl.duration(days=i))['date'].to_list()[0]    
                # print(i_day_before)
                i_day_before_path = f"/usr/local/airflow/include/data/{customerId}/stocks/{i_day_before.strftime('%Y')}/{i_day_before.strftime('%B')}/{i_day_before.strftime('%d')}"
                # print(Path(i_day_before_path).exists())
                if i_day_before in last_20_executions_date_list and Path(f"{i_day_before_path}/{i_day_before.strftime('%Y%m%d')}.parquet").is_file(): #########################################################   
                    # print(i_day_before)
                    a_day_before.extend([i_day_before])
                    break
        print(f"\033[33mlast execution date found: {current_date}\033[0m")
        print(f"\033[33mday before found: {a_day_before}\033[0m")
         
        print(f"\033[32mlast execution date: {current_date}, day before: {a_day_before}\033[0m")
        if a_day_before:
            return current_date, a_day_before[0], i_day_before_path
        else:             
            return current_date, a_day_before, i_day_before_path

def get_active_stors(customer: str):
        # TODO get active stores
        if not Path(f"/usr/local/airflow/include/stocks/{customer}/activeStors.parquet").is_file():
            # print(f"\033[33mWARNING please determine the active stores otherweis all ]")
            dic = {'2412': 1, 
                    '2416': 3, 
                    '2421': 4, 
                    '2424': 5, 
                    '2426': 30,
                    '2428': 32, 
                    '2414': 6, 
                    '2415': 7, 
                    '2418': 9, 
                    '2423': 11,
                    '2427': 31, 
                    '2432': 16,
                    '2420': 18,
                    '2440': 13, 
                    '2425': 12,
                    '2413': 99}
            
            # Convert values to strings but keep dict shape
            stores = {k: str(v) for k, v in dic.items()}
            # print(list(sorted(stores)))
            

            df = pl.DataFrame(stores)
            df.write_parquet(f"/usr/local/airflow/include/data/{customer}/activeStors_{customer}.parquet")     
       
        df = pl.scan_parquet(f"/usr/local/airflow/include/data/{customer}/activeStors_{customer}.parquet").collect()
        print(f"\033[31mCurrent active stores {dic}\033[0m")
        active_stores = list(map(str, sorted(df.row(0))))
        return active_stores

def get_all_stores(customer: str):
        # TODO get active stores
        file_path = f"/usr/local/airflow/include/stocks/{customer}/allStors_{customer}.parquet"
        if Path(file_path).exists():
            Path(file_path).unlink()
            print(f"\033[33mall stores file {file_path} exists, it will be overwritten.\033[0m")
        uri = "mysql://etos-needle:neeDle-250618-!@91.190.225.20:33060/globalStock"
        engine = create_engine(uri)
               
        query = f"""SELECT customerId, custStoreId, storeNumber
                            FROM customerStore
                            WHERE customerId = {customer} """
        
        # Query databes using URI    
        df = pl.read_database(query, engine.connect())
        df.write_parquet(Path(file_path))     
        print(f"\033[32mAll stores of the customer {customer} fetch from globalStock database, saved as allStors_{customer}.parquet in /usr/local/airflow/include/stocks/{customer}/\033[0m")
        
        all_stores = df.sort("storeNumber").with_columns(pl.col("storeNumber").cast(pl.String))#["storeNumber"].to_list()
        # print(f"\033[33mAll stores: {all_stores}\033[0m")
        store_numbers = all_stores.select("storeNumber")["storeNumber"].to_list()
        diff = list((Counter(store_numbers) - Counter(set(store_numbers))).elements())
        if diff:
             print(f"\033[31mThere are duplicate store numbers in all stores list: {diff}\033[0m")        
        return all_stores

