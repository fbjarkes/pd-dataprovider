Wrapper for pandas-datareader. Python3 required.

### Installation
```
$ git clone https://github.com/fbjarkes/dataprovider.git
$ pip3 install -r dataprovider/requirements.txt
$ cd dataprovider 
$ ./setup.py install
or
$ sudo ./setup.py install
```

### Tests
```
$ ./tests/test_data.py
$ ./tests/test_timeframe.py
```

### Features
* Multithreaded downloads
* Transform timeframes
* Add quotes (possibly delayed)  
* Trading day of the year
