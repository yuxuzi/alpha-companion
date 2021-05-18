import pandas as pd
import pyarrow.dataset as ds
from zipline.assets import (
    AssetDBWriter,
    AssetFinder)

from trading_calendars import get_calendar
from pathlib import Path
from alpha_companion.data_tools.parquet_daily_bar import ParquetDailyBarReader
from alpha_companion.data_tools.parquet_loader import ParquetLoader
from zipline.data.bundles import core as bundles


class ParquetBundle:
    def __init__(self,
                 path,
                 ):
        self.path = path
        self.dataset =pa_dataset= ds.dataset(Path(path) / 'data')
        start_date = pd.Timestamp(self._metadata[b'start_date'])
        end_date = pd.Timestamp(self._metadata[b'end_date'])

        self.calendar = calendar = get_calendar(self._metadata[b'calender'])
        self.asset_finder = AssetFinder(path)
        lookup = pd.read_parquet(Path(path) / 'asset').set_index('sid')
        sessions = calendar.sessions_in_range(start_date, end_date)
        self.daily_bar_reader = ParquetDailyBarReader(pa_dataset, lookup, sessions, calendar)

        self.pipeline_loader=ParquetLoader(pa_dataset)

    def register(self, name):

        @bundles.register(name, create_writers=False)
        def ingest(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   ):
            self.ingest()

    def ingest(self):
        asset_db_writer = AssetDBWriter(self.path, self.asset_finder)
        metadata = pd.read_parquet(self.path / 'asset').assign(
            auto_close_date=lambda df: df["end_date"] - pd.offsets.Day(1),
            exhange='NYSE')
        asset_db_writer.write(bonds=metadata)

    def update(self):
        pass




