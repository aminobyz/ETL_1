import requests 
import os
import time 
import polars as pl
import numpy as np

from pathlib import Path
from datetime import date  
from joblib import Parallel, delayed 
from sqlalchemy import create_engine 

from plugins.g_utils import *
 
 
class stock_api:
    """Class to fetch stock data from an external API and save it locally.
    Attributes:
        customer (str): Customer identifier.
    Methods:
        get_stockes_from_api(): Fetches stock data from the API and saves it as a parquet file.
        _connect_to_api(params: dict): Helper method to fetch stock data for given parameters.
        _procedure(batched_articles): Batches the data to be processed later in the get_stockes_from_api method.
        _fetch_articles_from_globalStock(): Fetches article IDs from the globalStock database.
        _save_execution_date(current_timestamp, current_year, current_month, current_day): it Saves the execution date of DAG each time it runs.

    """
    def __init__(self, customer: str = '22001'):
        self.customer = customer

    def _save_execution_date(self, current_timestamp: pl.Datetime, current_year: str, current_month: str, current_day: str) -> None:        

        last_20_executions_date_path =f"/usr/local/airflow/include/data/{self.customer}/last_20_executions_date.parquet"

        if Path(last_20_executions_date_path).is_file():
            last_20_executions_date = pl.scan_parquet(Path(last_20_executions_date_path)).collect().tail(20)

        else:
            last_20_executions_date = pl.DataFrame()
        current_execution_date_df = pl.DataFrame(data=[[self.customer, current_year, current_month, current_day, current_timestamp]], orient="row", schema=[("customerId", pl.String), 
                                                                                                                                                ("execution_year", pl.String), 
                                                                                                                                                ("execution_month", pl.String),                                                                                            
                                                                                                                                                ("execution_day", pl.String),
                                                                                                                                                ("execution_timestamp", pl.Datetime)])
                
        (pl.concat([last_20_executions_date, current_execution_date_df])
                                            .group_by("customerId", "execution_year", "execution_month", "execution_day")
                                            .agg(pl.col("execution_timestamp").max())
                                            .sort("execution_timestamp")
                                            .write_parquet(last_20_executions_date_path))
        
        print(f"\033[32mSaving current execution time on {current_timestamp}, year: {current_year}, month: {current_month}, day: {current_day} at last_20_executions_date_path\033[0m")
        print(f"\033[32mLast 20 Executions data: {last_20_executions_date.height}\033[0m")


    def _connect_to_api(self, params: dict):
        # API token
        api_token = "***"

        # The API endpoint URL
        api_url = "***"

        # Create a dictionary for the headers, including the Authorization token
        headers = {
            "Authorization": f"BASIC {api_token}",
            "Content-Type": "application/json"
        }
        # Send the GET request with the headers and parameters
        response = requests.get(api_url, headers=headers, params=params)
        
        # Check for HTTP errors (e.g., 401 Unauthorized, 404 Not Found)
        response.raise_for_status()
        # The request was successful, parse the JSON data
        json_data = response.json()        
        
        # Process the data     
        df = pl.DataFrame(json_data[0])
        return df

    def _procedure(self, batched_articles):
        start = time.time()
        pid = os.getpid() 
        time.sleep(1)
        skipped_articles = []
        df = pl.DataFrame()
        # print(f"Processing articles: {batched_articles}")
        for article_id in batched_articles:
            params = {
                    "id": article_id,    
                }
            try:
                fetched_df = self._connect_to_api(params=params)
                df = pl.concat([df, fetched_df])
            except requests.exceptions.RequestException as e:
                print(f"\033[31mAn Error has been occurred during fettching of the article_id {article_id}: {e}\033[0m")
                skipped_articles.append(article_id)
                continue   

            except Exception as e:
                print(f"\033[31mAn unexpected has been occurred during fettching of the article_id {article_id}: {e}. Skipping.\033[0m")
                continue       

        end = time.time()
        exec_time = end - start
        print(f"PID {pid} finished batch of size {len(batched_articles)} in {exec_time:.2f} sec")      
        return df , exec_time, skipped_articles   

    #  Fetch articles from globalStock database
    def _fetch_articles_from_globalStock(self):
        _, current_date, file_name, current_year, current_month, current_day = get_current_date()
        # print(f"Fetching articles for customer {customer} on {current_date} {type(current_date)}")
        path = check_Path_exits(customerId=self.customer, root_name="articles", current_year=current_year, current_month=current_month, current_day=current_day)
        
        # Ensure the directory exists
        if not Path(f"{path}/number_of_articles.parquet").is_file():
            article_number_df = pl.DataFrame(schema={'n_fetched_articles': pl.Int64, 'fetched_date': pl.Date})
            print(f"\033[33mWARNING The articles file does not exist for {self.customer} and it has been created.\033[0m")            
        else:
            print(f"\033[33mThe articles file already exists for {self.customer}.\033[0m")   
            # call articles
            article_number_df = pl.scan_parquet(f"{path}/number_of_articles.parquet").collect()

        uri = "***"
        engine = create_engine(uri)

        schema = pl.Schema({"customerId": pl.Int32,                        
                            "custArtId": pl.Int32,                         
                            })     
        
        query = f"""SELECT customerId, custArtId
                            FROM article 
                            WHERE customerId = {self.customer} """
        # Query databes using URI    
        df = pl.read_database(query, engine.connect(), schema_overrides=schema)
        
        df.write_parquet(file=f"{path}/article_{file_name}.parquet")
        print(f"\033[32mSaving current fetched articles from globalStock on {path} as article_{file_name}.parquet\033[0m")
        article_number_df = pl.concat([article_number_df, pl.DataFrame({'n_fetched_articles': df.height, 'fetched_date': date.fromisoformat(current_date)})])
        article_number_df.write_parquet(f"{path}/number_of_articles.parquet")
        print(f"\033[32mSaving total number of fetched articles on {path} as number_of_articles.parquet\033[0m")
        return df["custArtId"].to_numpy()
    
    def get_stockes_from_api(self):
        current_timestamp, current_date, file_name, current_year, current_month, current_day = get_current_date()
        print(f"\033[32mIngestion of current article stock started!\033[0m")
        articles_22001 = self._fetch_articles_from_globalStock()        
        
        num_cores = os.cpu_count()        
        data = articles_22001
        
        finall_df = pl.DataFrame()
        i = 1
        while True:
            print(f"\033[32m{i}th iteration\033[0m")
            if i == 1:
                chunks = np.array_split(data, num_cores)                  
            else:
                chunks = np.array_split(data, len(data)) 
            results = Parallel(n_jobs=num_cores, backend="threading")(
                                    delayed(self._procedure)(chunk) for chunk in chunks        
        )   
            # Unpack results
            batch_results, batch_times, skipped_articles = zip(*results)
            print("Execution times per batch:", batch_times)               
            print(skipped_articles)
            df = pl.concat(batch_results)    
            # print(df)
            finall_df = pl.concat([finall_df, df])

            data = np.array([item for sublist in skipped_articles if sublist for item in sublist])
            
            i += 1
            if data.size == 0:
                print("\033[32mAll articles have been fetched through the API. Producer Success!.\033[0m")
                break 
            else:                 
                print(f"\033[33mWARNING: Articles {data} have been not fettched through API in {i - 1}th iteration\033[0m")
                print(f"\033[33mRetrying the procedure for {data.size} articles\033[0m")
        
        folder_path  = Path(f"/usr/local/airflow/include/data/{self.customer}/stocks/{current_year}/{current_month}/{current_day}")
        # ensure folder exists
        folder_path.mkdir(parents=True, exist_ok=True)
        
        if Path(f"{folder_path}/{file_name}.parquet").is_file():
                Path(f"{folder_path}/{file_name}.parquet").unlink()
                print(f"{folder_path}/{file_name}.parquet exists!")
                print(f"{folder_path}/{file_name}.parquet overwrite!")

        finall_df.unnest('stock').with_columns(pl.lit(current_timestamp).str.to_datetime().alias('creDate')).write_parquet(f"{folder_path}/{file_name}.parquet")
        print(f"\033[32mSaving Current stock data on {folder_path} as {file_name}.parquet\033[0m")
        print(f"\033[32mLength of collected Dataset: {finall_df.height}\033[0m")
        print(finall_df.unnest('stock').with_columns(pl.lit(current_timestamp).str.to_datetime().alias('creDate')))

        print(f"saving current execution time into last_20_executions_date table")
        self._save_execution_date(current_timestamp=datetime.fromisoformat(current_timestamp), current_year=current_year, current_month=current_month, current_day=current_day)
