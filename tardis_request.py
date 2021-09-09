
from typing import List
from dataclasses import dataclass

from os import makedirs
from pathlib import Path

from tardis_client import TardisClient, Channel
import pandas as pd

@dataclass(frozen=True)
class DownloadRequest:
    date : str
    exchange : str
    msg_type : str
    symbols : List[str]
    cache_root_path : str
    api_key : str
    

class TardisGenerator:
    def __init__(self, request : DownloadRequest):
        self._build_cache_path(request)

        client = TardisClient(api_key=request.api_key,cache_dir=self.path)
        self.messages = client.replay(
            exchange = request.exchange,
            from_date = self._today_str(request.date),
            to_date = self._tomorrow_str(request.date),
            filters = [Channel(name=request.msg_type, symbols=request.symbols)]) 

    # This could probably be simplified a bit
    def _build_cache_path(self, request : DownloadRequest):
         now = pd.Timestamp.now().strftime('%Y%m%d%H%M%S')
         date_str = pd.Timestamp(request.date).strftime('%Y%m%d') 
         new_cache_dir_lbl = request.exchange + date_str + '_' + now
         self.path = Path(request.cache_root_path) / new_cache_dir_lbl
         makedirs(self.path)
    
    def _today_str(self, date : str):
        return pd.Timestamp(date).strftime('%Y-%m-%d')

    def _tomorrow_str(self, date : str):
        return (pd.Timestamp(date)+pd.Timedelta(days=1)).strftime('%Y-%m-%d')