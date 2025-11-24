
from abc import abstractmethod
import datetime

from datetime import date 
import os
import time 
import traceback
import requests 
from pathlib import Path
import polars as pl
import numpy as np
from joblib import Parallel, delayed 
from sqlalchemy import create_engine 

from include.customer import customer_data
from plugins.g_utils import  get_last_two_working_days
from plugins.g_utils import check_Path_exits
from plugins.g_utils import get_current_date


class Wrapper:
    """A wrapper class for handling chaining methods.
    Attributes:
        values: The data values to be processed.
        path: The file path for saving data.
        file_name: The name of the file to save data as.
    Methods:
        to_list(): Converts the values to a list.   
        __iter__(): Allows iteration over the values.
        __repr__(): Returns a string representation of the values.  
        save(): Saves the values to a parquet file at the specified path.
    """
    def __init__(self, values, path: str, file_name: str):
        self.values = values
        self.path = path
        self.file_name = file_name

    def to_list(self) -> list:
        return list(self.values)

    def __iter__(self):   # so you can loop directly
        return iter(self.values)

    def __repr__(self):
        return str(self.values)
    
    def save(self) -> None:       
        if not self.values.is_empty():             
            print(f"\033[32mData saved on: {self.path} as {self.file_name}.parquet\033[0m")
            self.values.write_parquet(f"{self.path}/{self.file_name}.parquet")

class customer_stocks(customer_data):
    """Abstract base class for customer stocks operations.
    Inherits from:
        customer_data: A class to manage customer data.
    attributes:
        customerId (str): The ID of the customer. default is '22001'.
    methods:
        save(): Saves data to a specified path.
        read_parquet_stocks_files(): Reads parquet files containing stock data. 
        daily_stocks(): Fetches and processes daily stocks.
        daily_sales(): Fetches and processes daily sales.
    """
    def __init__(self, customerId: str = '22001'):
        self.customerId = customerId
        self.center_warehous_id = '2413'
        self.center_warehous = '99'

    @abstractmethod
    def save(self) -> None:
        """Saves data to a specified path.
        Returns:
            None
        """
        pass

    @abstractmethod
    def read_parquet_stocks_files(self) -> None:
        """Reads parquet files containing stock data.
        Returns:
            None.
        """   

    def daily_stocks(self) -> pl.DataFrame:
        """Daily stock of all stores.
        Returns:
            pl.DataFrame: A DataFrame containing daily stock data which will be saved on the specified path.
        """         

    def daily_sales(self) -> None:
        """Daily sales of all stores.
        Returns:
            None
        """
        raise NotImplementedError

class stocksDatabase(customer_stocks):
    """Class to fetch stocks data from the gStock database.
    Inherits from:
        customer_stocks: An abstract base class for customer stocks operations. 
    attributes:
        customerId (str): The ID of the customer. default is '22001'.
    methods:
        read_parquet_stocks_files(): Reads parquet files containing stock data.
        daily_stocks(): Fetches and processes daily stocks.
        two_daily_stock_changes(): Compares stock changes between two days. 
        save(path): Saves the daily stocks data to the specified path.
    """
    def __init__(self, customerId: str = '22001'):
        super().__init__(customerId=customerId)
        # Get the last two days that dag has run automatically for fetching the stocks. 
            # Note: this may not be today and yesterday due to weekends or holidays.
        self.current_execution_date, self.a_day_before, self.a_day_before_path = get_last_two_working_days(customerId=self.customerId)       
        print(f"current execution date: {self.current_execution_date}, day before: {self.a_day_before}")      
        
    def read_parquet_stocks_files(self) -> pl.DataFrame:
        # Read today's fetched stock (the last execution of the dag) parquet files.
        source1 = f"/usr/local/airflow/include/data/{self.customerId}/stocks/{self.current_execution_date.strftime('%Y')}/{self.current_execution_date.strftime('%B')}/{self.current_execution_date.strftime('%d')}/{self.current_execution_date.strftime('%Y%m%d')}.parquet"

        if Path(source1).is_file():   
            # If the parquet file that contains today's date exists, read and manipulate it.
            stocks_current_execution_day = pl.scan_parquet(source1).select(pl.col('articleId'),
                                                                                pl.col('branch').cast(pl.String),
                                                                                pl.col('size'),
                                                                                pl.col('sizeIndex'),
                                                                                pl.col('amount').alias('current_amount'),
                                                                                pl.col('creDate').alias('current_amount_date'))

            return stocks_current_execution_day.collect()
        else:   
            # If the parquet file does not exist, it would raise an error. 
            # reasons:
                # - The DAG has just run and but the data have not been save on the destination path.
                # - The data has been deleted or moved.
            raise FileNotFoundError(f"\033[31mNo stock data found for the current execution date: {self.curent_date_str}\033[0m")

    def _create_pivoted_stocks(self, articleIds: list[int]) -> pl.DataFrame:
        """Creates a pivoted DataFrame for the given article IDs. 
                the rows of the pivoted DataFrame are article sizes and article Ids, columns are branches and values are current stock amounts.
        Args:
            articleIds (list[int]): A list of article IDs to process.
        Returns:
            pl.DataFrame: A pivoted DataFrame containing stock data for the specified article IDs.
        """
        start = time.time()
        pid = os.getpid() 
        time.sleep(1)
        df = pl.DataFrame()
        for article in articleIds:
            # loop over each article ID to create a pivot DataFrame of the article's stock.                           
            try:
                pivot = self.df.filter(pl.col("articleId") == article).pivot(on='branch', index=['size', 'articleId'], values='current_amount')
                df = pl.concat([df, pivot], how='vertical')
            except Exception as e:
                print(f"\033[33mAn unexpected has been occurred during fettchin : {e}.\033[0m")                        
                print("Full traceback:")
                traceback.print_exc()
                continue

        end = time.time()
        exec_time = end - start
        print(f"PID {pid} finished batch of size {len(articleIds)} in {exec_time:.2f} sec")  
        return df
    
    def daily_stocks(self) -> pl.DataFrame:
        num_cores = os.cpu_count()
        self.df = self.read_parquet_stocks_files()      
        print(f"Dataframe read from parquet files: {self.df}")         

        if not self.df.is_empty():
            # Proceed only if the DataFrame is not empty.
            articleIds = self.df['articleId'].unique().to_list()
            chunks = np.array_split(articleIds, num_cores)

            results = Parallel(n_jobs=num_cores, backend="threading")(
                                    delayed(self._create_pivoted_stocks)(chunk) for chunk in chunks        
        ) 
        print(f"results: {pl.concat(results) }")
        # Combine the results from all parallel processes.
        self.results = pl.concat(results)   

        join_df = self.df.lazy().join(self.results.lazy(), on=['articleId', 'size'], how='left').collect()
        # Define schema for dataframe.
        letters = tuple("0123456789")
        schema = { col: pl.Int8 for col in join_df.columns if col.startswith(letters)}
        schema['size'] = pl.Int16
        schema['sizeIndex'] = pl.Int16
        schema['current_amount'] = pl.Int8
        self.join_df = join_df.cast(schema)
        
        return self

    def two_daily_stock_changes(self):
        # Compare stock changes between two days.
        # Proceed only if there is a day before to compare with.
        if self.a_day_before:
            # Find the path where the previous DAG run stored stock parquet files in.
            current_execution_date_path = f"/usr/local/airflow/include/data/{self.customerId}/stocks/{self.current_execution_date.strftime('%Y')}/{self.current_execution_date.strftime('%B')}/{self.current_execution_date.strftime('%d')}"
            # Find the path where the two DAG run prior created the stock parquet files.
            a_day_before_path = f"/usr/local/airflow/include/data/{self.customerId}/stocks/{self.a_day_before.strftime('%Y')}/{self.a_day_before.strftime('%B')}/{self.a_day_before.strftime('%d')}"
            # read only data of stores that are active for the customer.
            active_stores = pl.scan_parquet(Path(f"/usr/local/airflow/include/data/{self.customerId}/activeStors_{self.customerId}.parquet")).collect()        
            active_stores_list = list(active_stores.row(0))
            active_stores_except_warehous = list(active_stores.select(pl.all().exclude(self.center_warehous_id)).row(0))         
            
            pre_pivot_path = Path(a_day_before_path).joinpath("pi*.parquet")         
            pre_pivot = (pl.scan_parquet(pre_pivot_path)
                                                    .filter(pl.col("branch").is_in(active_stores_except_warehous))
                                                    .select(["articleId", "branch", "size", "sizeIndex", "current_amount","current_amount_date"] + active_stores_list)
                                                    .collect())
            # Rename columns names.
            new_col_names = { col: 'pre_stock_Store_' + col for col in pre_pivot.columns if col not in ["articleId", "branch", "size", "sizeIndex", "current_amount", "current_amount_date"] }
            new_col_names["current_amount_date"] = "pre_amount_date"
            new_col_names["current_amount"] = "pre_amount"
            pre_pivot = pre_pivot.rename(new_col_names)         
            
            curr_pivot_path = Path(current_execution_date_path).joinpath("pi*.parquet")
            curr_pivot = (pl.scan_parquet(curr_pivot_path)
                                                    .filter(pl.col("branch").is_in(active_stores_except_warehous))
                                                    .select(["articleId", "branch", "size", "sizeIndex", "current_amount","current_amount_date"] + active_stores_list)
                                                    .collect())
            # Rename columns names.
            new_col_names = { col: 'curr_stock_Store_' + col for col in curr_pivot.columns if col not in ["articleId", "branch", "size", "sizeIndex", "current_amount", "current_amount_date"] }
            new_col_names["current_amount_date"] = "curr_amount_date"
            new_col_names["current_amount"] = "curr_amount"
            curr_pivot = curr_pivot.rename(new_col_names)

            # Join current and previous pivoted stock DataFrames to find stock changes over the last two days.
            df = curr_pivot.lazy().join(pre_pivot.lazy(),
                                left_on=["articleId", "branch", "size", "sizeIndex", "curr_amount"],
                                right_on=["articleId", "branch", "size", "sizeIndex", "pre_amount"], 
                                how='anti').join(pre_pivot.lazy(), on=["articleId", "branch", "size", "sizeIndex"], how='left')
            
            df2 = df.filter(~(pl.col("pre_amount").is_null()), 
            pl.col("curr_amount") >= 0)
            
            print(f"df2: {df2.collect() }")
            path = check_Path_exits(root_name="stocks", customerId=self.customerId, current_year=self.current_execution_date.strftime('%Y'),
                                                                                current_month=self.current_execution_date.strftime('%B'),
                                                                                current_day=self.current_execution_date.strftime('%d'))
            return Wrapper(df2.collect(), path=path,
                           file_name=f"dailyStockChanges_{self.current_execution_date.strftime('%Y%m%d')}_vs_{self.a_day_before.strftime('%Y%m%d')}")
        else:
            print(f"\033[33mthere is no pivotArtStoresStock parquet file before day`s{self.current_execution_date} for the customer {self.customerId}, skipping this time.\033[0m")
            return Wrapper(pl.DataFrame(), path="No_path", file_name="No_file")

    def save(self, path=None):
        if path is None:
            file_name = f"pivotArtStoresStock_{self.current_execution_date.strftime('%Y%m%d')}"
            
            path = check_Path_exits(root_name="stocks", customerId=self.customerId, current_year=self.current_execution_date.strftime('%Y'),
                                                                                current_month=self.current_execution_date.strftime('%B'),
                                                                                current_day=self.current_execution_date.strftime('%d'))

            Path(path).mkdir(parents=True, exist_ok=True)

        if Path(f"{path}/{file_name}.parquet").is_file():
                Path(f"{path}/{file_name}.parquet").unlink()         

        self.join_df.write_parquet(f"{path}/{file_name}.parquet")
        print(f"\033[32mStocks data saved on: {path} as {file_name}.parquet\033[0m")
    
