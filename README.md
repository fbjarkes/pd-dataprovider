Wrapper for pandas-datareader. Python3 required.

### Installation
```
$ git clone https://github.com/fbjarkes/qa-dataprovider.git
$ cd qa-dataprovider && pip3 install -r requirements.txt
$ ./setup.py develop 
```

### Tests
Run tests in tests folder:
```
tests/$ python3 -m unittest discover
```

### Features
* Multithreaded downloads
* Transform timeframes
* Add quotes (possibly delayed)  
* Trading day of the year
