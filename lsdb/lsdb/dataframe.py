import pandas as pd

class LSeries(pd.Series):
    @property
    def _constructor(self):
        return LSeries

    @property
    def _constructor_expanddim(self):
        return LDataFrame

class LDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return LDataFrame

    @property
    def _constructor_sliced(self):
        return LSeries

