import pandas as pd
import requests as rq
import json
import re
from io import StringIO

def fetch_data():
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    params = {
        "starttime": '2017-01-01',
        "endtime": '2017-01-02',
        "format": 'csv'
    }

    with rq.get(url, params=params, stream=True) as r:
        df = pd.read_csv(StringIO(r.content.decode()))
        pass



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fetch_data()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
