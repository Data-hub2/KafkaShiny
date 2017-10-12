import unittest
import json
import pandas as pd
import tennet.helper as helper
from datetime import datetime


class TennetTestCase(unittest.TestCase):

    def setUp(self):
        self.monday = datetime.strptime('2017-10-09 01:00:00.0001', '%Y-%m-%d %H:%M:%S.%f')
        self.any_other_day = datetime.strptime('2017-10-13 01:00:00.0001', '%Y-%m-%d %H:%M:%S.%f')
        self.monday_range = "&datefrom=06-10-2017&dateto=08-10-2017"
        self.any_other_day_range = "&datefrom=12-10-2017&dateto=12-10-2017"
        self.sample_df = pd.read_csv("test_resources/sample_imbalance_data.csv")

    def test_get_date_range(self):
        self.assertEquals(helper.get_date_range(self.monday), self.monday_range,
                          "The range generated for a monday is not correct")
        self.assertEquals(helper.get_date_range(self.any_other_day), self.any_other_day_range,
                          "The range generated for non monday is not correct")

    def test_create_tennet_url(self):
        self.assertEquals(helper.create_tennet_url(self.any_other_day_range),
                          "http://www.tennet.org/english/operational_management/export_data.aspx?" +
                          "exporttype=settlementprices&format=csv&datefrom=12-10-2017&dateto=12-10-2017&submit=1",
                          "The URL has not been generated properly")

    def test_parse_df(self):
        # Load the data once for the subsequent tests
        parsed_data = helper.parse_df(self.sample_df)
        self.assertEquals(type(parsed_data), type("example string"),
                          "The data frame parsing return type is not as expected")
        self.assertEquals(type(json.loads(parsed_data)), list,
                          "The string is not a valid JSON")
        self.assertEquals(len(json.loads(parsed_data)), 96,
                          "The number of elements of the dataframe are not as expected")
        self.assertEquals([*json.loads(parsed_data)[0].keys()],
                          ['date', 'ptu', 'upward_dispatch', 'downward_dispatch',
                           'take_from_system', 'feed_into_system', 'regulation_state', 'processed_time'],
                          "The list of columns in the dataframe are not correct")
