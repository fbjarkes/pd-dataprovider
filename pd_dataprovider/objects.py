from dataclasses import dataclass
import pandas as pd
from datetime import datetime


@dataclass
class SymbolData:
    symbol: str
    timeframe: str
    transform: str
    start: str
    end: str
    rth_only: bool = True


@dataclass
class Data:
    df: pd.DataFrame = None
    symbol: str = ''
    timeframe: str = ''
    start: datetime = ''
    end: datetime = ''