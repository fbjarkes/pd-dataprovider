from dataclasses import dataclass
import pandas as pd
from datetime import datetime

@dataclass
class Data:
    df: pd.DataFrame = None
    symbol: str = None
    timeframe: str = None
    start: datetime = None
    end: datetime = None