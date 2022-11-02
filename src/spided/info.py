import logging
import os
import pandas as pd
import requests
import time
from multiprocessing.pool import ThreadPool
from spided.util import is_web_file_from_url

logging.basicConfig(level=logging.ERROR,
                    format='%(name)s %(asctime)s %(levelname)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
_logger.setLevel(logging.DEBUG)


def _get_info_from_url(url, method=None):
    _logger.info(url)
    if is_web_file_from_url(url):  # 判断是url是否是文件夹链接
        return
    if method is None:
        method = requests.get
    while True:
        try:
            r = method(url + ".json")
            _df = pd.read_json(r.text, dtype=False)
            time.sleep(1)
            break
        except Exception as e:
            _logger.error(str(e))
    _df.loc[:, "url"] = _df.loc[:, "name"].apply(lambda x: url + "/" + str(x))
    return _df


def loop_info(urls: list, thread_num=20, method=None):
    with ThreadPool(thread_num) as p:
        df_list = p.starmap(_get_info_from_url, ((url, method) for url in urls))
    if not len(df_list):
        return pd.DataFrame()
    df = pd.concat(df_list, axis=0)
    df_flag_isfile = df.loc[:, "name"].apply(is_web_file_from_url)
    df_file = df.loc[df_flag_isfile, :]
    df_dir = df.loc[~df_flag_isfile, :]
    if len(df_dir.loc[:, "url"]):
        df = pd.concat([df_file, loop_info(df_dir.loc[:, "url"])], axis=0)
    return df


if __name__ == '__main__':
    # An Example
    df = loop_info(["https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD04_L2/2022"])
    df.to_csv("res.csv", index=False)