from abc import ABC, abstractmethod


class Expectation(ABC):
    def __init__(self, column, metric, add_info={}):
        self.column = column
        self.metric = metric
        self.add_info = add_info

    @abstractmethod
    def test(self, ge_df):
        pass
