
from dataprovider.web_dataprovider import WebDataprovider


class TestDataprovider(WebDataprovider):
    def __init__(self):
        super(TestDataprovider, self).__init__(cache_name='test_data', expire_days=0)

