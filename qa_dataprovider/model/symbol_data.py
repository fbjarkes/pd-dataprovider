from dataclasses import dataclass

@dataclass
class SymbolData:
    symbol: str
    timeframe: str
    transform: str
    start: str
    end: str
    rth_only: bool = True
