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
Run tests in tests folder:
```
tests/$ python3 -m unittest discover
```

### Features
* Multithreaded downloads
* Transform timeframes
* Add quotes (possibly delayed)  
* Trading day of the year
