import pandas as pd
from .util import get_file_name_from_url, is_web_file_from_url
from .check import compare_filesize
from .downloader import Downloader
from .log import create_stream_logger
from tenacity import retry, stop_after_attempt, wait_random
import time
import os
from concurrent.futures import ThreadPoolExecutor

logger = create_stream_logger("Download")


class StrategyTemplate:

    def __init__(self):
        return

    def add_downloader(self, downloader: Downloader):
        self.downloader = downloader

    def run(self):
        return


class StrategyCSV(StrategyTemplate):

    def __init__(self, df: pd.DataFrame, local_dir=".", max_threads=20, obj_csv=f"{int(time.time())}.csv"):
        self.origin_df = df
        self.df = df.copy()
        self.max_threads = max_threads
        self.pre_run(local_dir=local_dir)
        self.local_dir = local_dir
        self.obj_csv = obj_csv

    def pre_run(self, local_dir="."):
        if "url" not in self.df.columns:
            raise ValueError("url column is not in the dataframe!")
        if "size" not in self.df.columns:
            self.df["size"] = -100
        if "filename" not in self.df.columns:
            self.df["filename"] = self.df["url"].apply(get_file_name_from_url)
        if "filepath" not in self.df.columns:
            self.df["filepath"] = self.df["filename"].apply(lambda x: os.path.join(local_dir, x))
        if "params" not in self.df.columns:
            self.df["params"] = ""
        if "isfile" not in self.df.columns:
            self.df["isfile"] = False

    @retry(stop=stop_after_attempt(3), wait=wait_random(1, 2))
    def fetch_info(self):
        dir_indexes = self.df["isfile"] == False
        self.df.loc[dir_indexes, "isfile"] = self.df.loc[dir_indexes, "url"].apply(is_web_file_from_url)
        # 迭代请求文件的文件大小
        _df = self.df.query("isfile == False")
        # 多线程执行
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            df_list = list(executor.map(
                self.downloader._subnode_fileinfo,
                _df["url"],
            ))
        df_list.append(self.df.query("isfile == True"))
        if not len(df_list):
            return pd.DataFrame()
        df_file = pd.concat(df_list, axis=0)
        dir_indexes = self.df["isfile"] == False
        self.df.loc[dir_indexes, "isfile"] = self.df.loc[dir_indexes, "url"].apply(is_web_file_from_url)
        self.df = df_file
        if len(df_file.query("isfile == False")):
            self.fetch_info()

    @retry(stop=stop_after_attempt(2), wait=wait_random(1, 2))
    def fetch_size(self):
        # 迭代请求文件的文件大小
        logger.info(f"DataFrame File Size Finding ......")
        _df = self.df.query("size <= 0")
        # 多线程执行
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            _df["size"] = list(executor.map(
                self.downloader._stream_filesize,
                (_df["url"]).tolist(),
            ))
        self.df.update(_df)
        failed_length = len(self.df.query("size <= 0"))
        if failed_length > 0:
            raise ValueError(f"Some files ({failed_length}) size is not found!")

    @retry(stop=stop_after_attempt(2), wait=wait_random(1, 2))
    def fetch_content(self):
        # 迭代下载文件
        logger.info(f"DataFrame File URL Downloading ......")
        _df = self.df.query("size > 0")
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            executor.map(
                self.downloader._stream_download,
                ["GET"] * len(_df),
                _df["url"],
                _df["filepath"],
                [1024 * 1024] * len(_df),
                ["size"] * len(_df),
                _df["size"],
            )
        return self.df

    @retry(stop=stop_after_attempt(2), wait=wait_random(1, 2))
    def run(self):
        # 迭代请求文件的信息
        self.fetch_info()
        self.df.to_csv(self.obj_csv, index=False)
        # 迭代请求文件的文件大小
        filename_indexes = self.df["filename"].str.contains(".")
        self.df.loc[~filename_indexes, "filename"] = self.df.loc[~filename_indexes, "url"].apply(get_file_name_from_url)
        self.df["filepath"] = self.df["filename"].apply(lambda x: os.path.join(self.local_dir, x))
        self.fetch_size()
        self.df.to_csv(self.obj_csv, index=False)
        # 迭代下载文件
        self.fetch_content()
        flag_list = [compare_filesize(rows["filepath"], rows["size"]) for idx, rows in self.df.iterrows()]
        if all(flag_list):
            logger.info("All File Finished!")
        else:
            raise ValueError("Some File Download Failed!")
