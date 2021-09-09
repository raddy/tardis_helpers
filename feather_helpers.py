from dataclasses import dataclass

from pyarrow.feather import write_feather
from pandas import DataFrame

@dataclass
class FeatherInfo:
    path : str
    compression : str

def write_feather_frame(info : FeatherInfo, df : DataFrame):
    write_feather(df, info.path, info.compression)