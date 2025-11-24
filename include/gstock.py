from abc import ABC
from sqlalchemy import create_engine 


class globalStock(ABC):

    def _engine(self):
        uri = "*****"
        engine = create_engine(uri)
        return engine
    

