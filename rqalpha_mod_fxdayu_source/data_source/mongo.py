# encoding: utf-8
from datetime import date, datetime, time

import motor.motor_asyncio
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.model.instrument import Instrument
from rqalpha.utils.datetime_func import *
from rqalpha.utils.py2 import lru_cache

from rqalpha_mod_fxdayu_source.data_source.common import CacheMixin
from rqalpha_mod_fxdayu_source.data_source.common.minite import MiniteBarDataSourceMixin
from rqalpha_mod_fxdayu_source.data_source.common.odd import OddFrequencyBaseDataSource
from rqalpha_mod_fxdayu_source.utils import Singleton
from rqalpha_mod_fxdayu_source.utils.asyncio import get_asyncio_event_loop
from rqalpha_mod_fxdayu_source.utils.converter import MongoConverter

INSTRUMENT_TYPE_MAP = {
    INSTRUMENT_TYPE.CS: "stock",
    INSTRUMENT_TYPE.INDX: "index",
}


class NoneDataError(BaseException):
    pass


class MongoDataSource(OddFrequencyBaseDataSource, MiniteBarDataSourceMixin):
    __metaclass__ = Singleton

    def __init__(self, path, mongo_url):
        super(MongoDataSource, self).__init__(path)
        from rqalpha_mod_fxdayu_source.share.mongo_handler import MongoHandler
        self._handler = MongoHandler(mongo_url)
        self._client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)

    async def _do_get_bars(self, db, collection, filters, projection, fill=np.NaN):
        dct = {}
        l = 0
        async for doc in self._client[db][collection].find(filters, projection):
            _l = doc.pop('_l')
            l += _l
            for key, values in doc.items():
                if isinstance(values, list) and (len(values) == _l):
                    dct.setdefault(key, []).extend(values)
            for values in dct.values():
                if len(values) != l:
                    values.extend([fill] * l)
        df = pd.DataFrame(dct)
        if df.size:
            return df.sort_values("datetime")
        else:
            return None
    
    def _get_bars_in_days(self, instrument, frequency, params):
        s_date = params[0]["trade_date"]
        e_date = params[-1]["trade_date"]
        s_time = params[0]["start_time"] if "start_time" in params[0] else 0
        e_time = params[-1]["end_time"] if "end_time" in params[-1] else 150000
        s_dt_int = convert_date_to_int(s_date) + s_time
        e_dt_int = convert_date_to_int(e_date) + e_time
        code = instrument.order_book_id.split(".")[0]
        db = "quantaxis"
        collection = ""
        if INSTRUMENT_TYPE_MAP[instrument.enum_type] == "stock":
            collection = "stock_min"
        else:
            collection = "index_min"
        filters = {"datetime": {"$gte": convert_int_to_datetime(s_dt_int).isoformat(sep=" "), "$lte": convert_int_to_datetime(e_dt_int).isoformat(sep=" ")}, "code" : code, "type":"1min" }
        projection = {"_id": 0, "_d": 0}
        loop = get_asyncio_event_loop()
        bars = loop.run_until_complete(self._do_get_bars(db, collection, filters, projection))
        if bars is not None and bars.size:
            bars = MongoConverter.df2np(bars)
        else:
            bars = MongoConverter.empty()
        s_pos = np.searchsorted(bars["datetime"], s_dt_int)
        e_pos = np.searchsorted(bars["datetime"], e_dt_int, side="right")
        return bars[s_pos:e_pos]

    def raw_history_bars(self, instrument, frequency, start_dt=None, end_dt=None, length=None):
        # 转换到自建mongodb结构s
        if frequency.endswith("m"):
            return MiniteBarDataSourceMixin.raw_history_bars(
                self, instrument, frequency, start_dt=start_dt, end_dt=end_dt, length=length)
        else:
            code = instrument.order_book_id.split(".")[0]
            db = "quantaxis"
            if INSTRUMENT_TYPE_MAP[instrument.enum_type] == "stock":
                collection = "stock_day"
            else:
                collection = "index_day"

            data = self._handler.read(collection, db=db, code=code, index = "date", start=start_dt, end=end_dt, length=length, sort=[("date",1)]).reset_index()
            if data is not None and data.size:
                return MongoConverter.df2np(data)
            else:
                return MongoConverter.empty()


    def current_snapshot(self, instrument, frequency, dt):
        pass

    def _get_date_range(self, frequency):
        from pymongo import DESCENDING
        from datetime import datetime
        db = "quantaxis"
        key = "date" if frequency.endswith("d") else "datetime"
        collection = "index_day" if frequency.endswith("d") else "index_min"
        filter = {"code":"000001"} if frequency.endswith("d") else {"code":"000001","type":"1min"}
        try:
            start = self._handler.client.get_database(db).get_collection(collection).find(filter) \
				.sort(key).limit(1)[0][key]
            end = self._handler.client.get_database(db).get_collection(collection).find(filter) \
				.sort(key, direction=DESCENDING).limit(1)[0][key]
            start_dt = convert_dt_to_int(datetime.fromisoformat(start))
            end_dt = convert_dt_to_int(datetime.fromisoformat(end))
        except IndexError:
            raise RuntimeError("无法从MongoDb获取数据时间范围")
        return convert_int_to_date(start_dt).date(), convert_int_to_date(end_dt).date()

    @lru_cache(maxsize=10)
    def available_data_range(self, frequency):
        if frequency.endswith("d") or frequency.endswith("h"):
            return date(2012, 6, 1), date.today() - relativedelta(days=1)
        return self._get_date_range(frequency)


class MongoCacheDataSource(MongoDataSource, CacheMixin):
    def __init__(self, path, mongo_url):
        super(MongoCacheDataSource, self).__init__(path, mongo_url)
        CacheMixin.__init__(self)
