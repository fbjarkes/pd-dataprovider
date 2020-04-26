Wrapper for getting Pandas dataframes from various sources.

### Installation
```
$ git clone https://github.com/fbjarkes/qa-dataprovider.git
$ cd qa-dataprovider && pip install -r requirements.txt
$ ./setup.py develop 
```

### Tests
Run tests in tests folder:
```
tests/$ python -m unittest discover
```

### Data Model
Input:

```
    SymbolData('SPY', '60min', '60min', '2015-01-01', '2019-12-31')
```
Output:
```
    Data: {
        dataframe,
        symbol,
        timeframe,
        start,
        end
    }
```
