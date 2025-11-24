
from abc import abstractmethod
import polars as pl
from include.customer import customer_data
from plugins.g_utils import check_Path_exits, get_last_two_working_days


class customer_sales(customer_data):
    """Abstract base class for customer sales operations.
    Inherits from:
        customer_data: A class to manage customer data.
    """
    def __init__(self, customerId: str = '22001'):
        self.customerId = customerId

    @abstractmethod
    def save(self) -> None:
        """Saves data to a specified path.
        Returns:
            None
        """
        pass
    @abstractmethod
    def read_gs_database(self) -> None:
        """Reads data from the gStock database.
        Returns:
            None.
        """
        
    @abstractmethod
    def query(self) -> str:
        """Defines query to fetch sales data.
        Returns:
            a SQL query string.
        """

    def daily_stocks(self) -> pl.DataFrame:
        """Daily stock of all stores.
        Returns:
            pl.DataFrame: A DataFrame containing daily stock data which will be saved on the specified path.
        """
        raise NotImplementedError

    def daily_sales(self) -> None:
        """Daily sales of all stores.
        Returns:
            None
        """
        pass

class salesDatabase(customer_sales):
    """Class to fetch sales data from the gStock database.
    Inherits from:
        customer_sales: An abstract base class for customer sales operations.
    attributes:
        customerId (str): The ID of the customer. default is '22001'.
    methods:
        read_gs_database(): Connects to the gStock database.
        query(): Defines the SQL query to fetch sales data from the gStock database.
        daily_sales(): Fetches and processes daily sales data.
        save(path): Saves the daily sales data to the specified path.
        
    """
    def __init__(self, customerId: str = '22001'):
        super().__init__(customerId=customerId)
        _, self.a_day_before, _ = get_last_two_working_days(customerId=self.customerId)
       
    def read_gs_database(self):
        print(f'\033[32mConnecting to gStock to fetch transactions sales for customer {self.customer}\033[0m')

    def query(self):
        query = f"""SELECT customerId, custStoreId, custArtId, custSizeId, quantity, bookingDate
                            FROM transactionSales 
                            WHERE bookingDate = {self.a_day_before.strftime('%Y%m%d')}
                            AND customerId = {self.customerId}"""
        return query

    def daily_sales(self):
        """Daily sales of all stores.
        Returns:
            None.
        """
        engine = self._engine()
        schema = pl.Schema({"customerId": pl.Int32,
                            "custStoreId": pl.Int32,
                            "custArtId": pl.Int32,
                            "custSizeId":  pl.Int16,
                            "quantity": pl.Int16,
                            "bookingDate": pl.Int32,
                            })
        df = pl.read_database(self.query(), engine.connect(), schema_overrides=schema)
        print(f"\033[32mData fetched successfully from the database.\033[0m")
        self._df = (df.select(pl.all().exclude("customerId"))
                                .filter(pl.col("quantity") > 0)
                                .group_by(["custStoreId", "custArtId", "custSizeId", "bookingDate"])
                                .agg(pl.sum("quantity").alias("num_daily_sales"))
                                .sort(["num_daily_sales"]))
        return self

    def save(self, path=None):
        """Saves daily sales data to a specified path.
        Args:
            path (str, optional): The path where the data will be saved. Defaults to None.
        Returns:
            None.
        """
        if path is None:
            path = check_Path_exits(customerId=self.customerId, root_name="sales", current_year=self.a_day_before.strftime('%Y'), current_month=self.a_day_before.strftime('%B'), current_day=self.a_day_before.strftime('%d'))
              
        self._df.write_parquet(file=path,
                                use_pyarrow=True,
                                pyarrow_options={"partition_cols": ["custStoreId"]})  
        print(f"\033[32mSaving yesterday's sales data on {path}\033[0m")
         



    