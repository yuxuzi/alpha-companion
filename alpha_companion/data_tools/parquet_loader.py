"""
PipelineLoader accepting a Parquet as input.
"""
from functools import partial

from interface import implements
import pandas as pd
import numpy as np

from zipline.lib.adjusted_array import AdjustedArray
from zipline.pipeline.loaders.base import PipelineLoader
import  pyarrow.dataset as ds
from pathlib import Path
class ParquetLoader(implements(PipelineLoader)):
    """
    A PipelineLoader that reads its input from DataFrames.

    Mostly useful for testing, but can also be used for real work if your data
    fits in memory.

    Parameters
    ----------
    column : zipline.pipeline.data.BoundColumn
        The column whose data is loadable by this loader.
    baseline : pandas.DataFrame
        A DataFrame with index of type DatetimeIndex and columns of type
        Int64Index.  Dates should be labelled with the first date on which a
        value would be **available** to an algorithm.  This means that OHLCV
        data should generally be shifted back by a trading day before being
        supplied to this class.


    """

    def __init__(self, path):
        self.pa_dataset=ds.dataset(path)

    def get_data(self):
        Column



    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        """
        Load data from our parquet file.
        """
        start, end = dates[0], dates[-1]
        fields =['date','sid'] +[col.name for col in columns]

        df = self.pa_dataset.to_table(
            columns=fields,
            filter=(ds.field('date') > start)
                   & (ds.field('date') < end)). \
            to_pandas().set_index(['date', 'sid']).unstack(0).\
            reindex(sids)


        if mask is not None:
            mask_all=np.tile(np.transpose(~mask), len(columns))
            df.mask( mask_all, pd.NA)

        def get_column(col, missing):
            dat = df.xs(col,axis=1).T.fillna(missing).values


            return AdjustedArray(
                # Pull out requested columns/rows from our baseline data.
                data=dat,
                adjustments={},
                missing_value=missing,
            ),

        return {column.name: get_column(column.name, column.missing_value) for column in columns}


if __name__ == "__main__":
    from typing import Any
    from dataclasses import dataclass

    loader = ParquetLoader(path='c:/Data/data')


    @dataclass
    class Column:
        name: str
        missing_value: Any = np.nan


    columns = [Column(a) for a in 'ABC']
    df = loader.load_adjusted_array(None, columns, dates=[pd.Timestamp('2018-01-04'), pd.Timestamp('2018-01-10')],
                                    sids=[1,2,3], mask=None)
    print(df)
