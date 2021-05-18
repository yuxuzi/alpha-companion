import json
import pandas as pd
import pyarrow.dataset as ds
from pathlib import Path
class DataSource:
    def __init__(path):
        self.basedir = Path(path)
        self.pa_dataset = ds.dataset(basedir / 'data')
        self._metadata = json.loads(self.pa_dataset.schema.metadata)
        self.lookup = pd.read_parquet(basedir/'asset').set_index('sid')
        self._pq_columns = {}
        self._pd_columns = {}
        self.start_date=pd.Timestamp(self._metadata[b'start_date'])
        self.enda_date=pd.Timestamp(self._metadata[b'end_date'])





    def map_ids(target='sid'):
        sids, symbols = self.lookup['sid'],self.lookup['symbol']
        if target == 'sid':
            return dict(zip(symbols,sids))
        else:
            return dict(zip(sids, symbols))


    @property
    def calandar(self):
        return self._metadata[b'calander']

    def load_panads(df, name):
        type = df.dtypes[0]

        pass

    def infer_dataset(self):
        for field in self.dataset.schema:
            name = a.name
            dtype = a.type.to_pandas_dtype()
            if name not in ('date', 'sid'):
                # handling missing data later
                self._pq_columns[k] = Column(v)

        return type("Dataset", (DataSet,), {**self._pq_columns, **self._pd_columns})

    def get_loader(self, columns):
        if column in self._pq_columns:
            return ParquetLoader(self.datapath)
