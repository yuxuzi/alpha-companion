

import numpy as np
import pandas as pd
import  pyarrow.dataset as ds
import pyarrow as pa
from pathlib import Path
from trading_calendars import TradingCalendar
from zipline.lib.adjusted_array import AdjustedArray
from zipline.data.bar_reader import OHLCV, NoDataOnDate, NoDataForSid
from zipline.data.session_bars import CurrencyAwareSessionBarReader
from zipline.utils.input_validation import expect_types, validate_keys
from zipline.utils.pandas_utils import check_indexes_all_same
from trading_calendars import get_calendar






DEFAULT_VOLUME=1e9

class ParquetDailyBarReader(CurrencyAwareSessionBarReader):
    """

    A SessionBarReader backed by a dictionary of in-memory DataFrames.

    Parameters
    ----------
    frames : dict[str -> pd.DataFrame]
        Dictionary from field name ("open", "high", "low", "close", or
        "volume") to DataFrame containing data for that field.
    calendar : str or trading_calendars.TradingCalendar
        Calendar (or name of calendar) to which data is aligned.
    currency_codes : pd.Series
        Map from sid -> listing currency for that sid.
    verify_indices : bool, optional
        Whether or not to verify that input data is correctly aligned to the
        given calendar. Default is True.
    """
    @expect_types(
        path=str,
        verify_indices=bool,
        currency_codes=pd.Series,
    )
    def __init__(self,dataset,lookup,sessions, calendar):
        self.dataset=dataset
        self.lookup=lookup
        self.sessions= sessions
        self.calendar=calendar




    def load_raw_arrays(self, columns, start_dt, end_dt, assets):
        if start_dt not in self._sessions:
            raise NoDataOnDate(start_dt)
        if end_dt not in self._sessions:
            raise NoDataOnDate(end_dt)
        for col in columns:
            if col not in OHLCV:
                raise ValueError(f"Column {col} is not supported to load by ParquetDailyBarReader")

        out=[]
        data=None

        for col in columns:
            if  col == 'volume':
                dates=self._sessions(start_dt, end_dt)
                out.append( np.array((len(dates), len(assets))))
            else:
                if data is None:
                    data=self.dataset.to_table(
                    columns=['date', 'sid', 'close'],
                    filter=(ds.field('date') >= start_dt)
                           & (ds.field('date') <= end_dt)). \
                    to_pandas().set_index(['date', 'sid']).unstack(0). \
                    reindex(assets).T.values
                out.append(data)
        return out



    def last_available_dt(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The last session for which the reader can provide data.
        """
        self.sessions[-1]


    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
        the data.  Can be None (if the writer didn't specify it).
        """
        self.calandar


    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The first trading day (session) for which the reader can provide
            data.
        """
        self.sessions[0]
    def get_last_traded_dt(self, asset, dt):
        """
        Get the latest minute on or before ``dt`` in which ``asset`` traded.

        If there are no trades on or before ``dt``, returns ``pd.NaT``.

        Parameters
        ----------
        asset : zipline.asset.Asset
            The asset for which to get the last traded minute.
        dt : pd.Timestamp
            The minute at which to start searching for the last traded minute.

        Returns
        -------
        last_traded : pd.Timestamp
            The dt of the last trade for the given asset, using the input
            dt as a vantage point.
        """
        start, end=self.lookup.loc[asset, ['start', 'end']].values
        if dt < start:
            return pd.NaT
        elif dt >= end:
            return end
        else:
           idx= np.searchsorted ( self.sessions , dt )
           self.sessions[idx]


    def get_value(self, sid, dt, field):
        """
        Retrieve the value at the given coordinates.

        Parameters
        ----------
        sid : int
            The asset identifier.
        dt : pd.Timestamp
            The timestamp for the desired data point.
        field : string
            The OHLVC name for the desired data point.

        Returns
        -------
        value : float|int
            The value at the given coordinates, ``float`` for OHLC, ``int``
            for 'volume'.

        Raises
        ------
        NoDataOnDate
            If the given dt is not a valid market minute (in minute mode) or
            session (in daily mode) according to this reader's tradingcalendar.
        """
        return self.dataset.to_table( columns=field,filter=(ds.field('date') ==dt )).to_pydict()[field][0]







