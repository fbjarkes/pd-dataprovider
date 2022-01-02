Wrapper for getting Pandas dataframes from various sources.

### Installation
```
$ git clone https://github.com/fbjarkes/pd-dataprovider.git
$ cd pd-dataprovider && pipenv install
```

### Tests
Run tests in tests folder:
```
$ pipenv shell
$ export PYTHONPATH=`pwd`
$ cd tests && python -m unittest discover
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
