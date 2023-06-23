import requests
import pandas as pd
import os, sys
import time, datetime
from mtmtool.pool import pooling
import re

@pooling(max_workers=10, pool_type="Thread")
def get_url(datestr):
    url = "https://e4ftl01.cr.usgs.gov/MOTA/MCD19A2.061/"
    print(datestr)
    _url = url+datestr+"/"
    resp = requests.get(url+datestr+"/")
    rule = ".*.hdf"
    file_url = re.findall(f'<a href="({rule})">', resp.text)
    if not file_url:
        raise ValueError(f"No file found! {datestr}")
    df = pd.DataFrame({"url": file_url, "filename": file_url})
    df["url"] = _url + df["url"]
    return df


def get_time_list(start, end):
    begin = datetime.date(*start)
    end = datetime.date(*end)
    for i in range((end - begin).days+1):
        day = begin + datetime.timedelta(days=i)
        yield day.strftime("%Y.%m.%d")

if __name__ == '__main__':
    for year in range(2012, 2022):
        for key in get_time_list((year, 1, 1), (year, 12, 31)):
            get_url(datestr=key)
        df_list = get_url.result()
        df = pd.concat(df_list)
        df.to_csv(f"./MAIAC{year}.csv", index=False)