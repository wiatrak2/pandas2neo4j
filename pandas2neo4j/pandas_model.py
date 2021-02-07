import pandas as pd
from py2neo import ogm

class PandasModel(ogm.Model):
    def __init__(self, row: pd.Series):
        for column in [col for col in row.keys() if hasattr(self, col)]:
            self.__setattr__(column, row[column])
