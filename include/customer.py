
from abc import abstractmethod  
from include.gstock import globalStock


class customer_data(globalStock):
    """Abstract base class for customer data operations.
    Inherits from:
        globalStock: A class to manage global stock data.
    """ 

    @abstractmethod
    def daily_stocks(self):
        """Daily stock of all stores.
        """
        pass

    @abstractmethod
    def daily_sales(self):
        """Daily sales of all stores.
        """
        pass
