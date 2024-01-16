import time
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from mtmtool.pool import MapPool, pooling
from mtmtool.path import auto_make_dirs

from .util import get_file_name_from_url, is_web_file_from_url
from .check import FileChecker
from .downloader import Downloader, logger


class StrategyTemplate:

    def __init__(self):
        return

    def add_downloader(self, downloader: Downloader):
        self.downloader = downloader
        self.stream_filesize = MapPool(self.downloader._stream_filesize, max_workers=self.max_threads, pool_type="thread")
        self.stream_fileinfo = MapPool(self.downloader._subnode_fileinfo, max_workers=self.max_threads, pool_type="thread")
        self.stream_download = MapPool(self.downloader._stream_download, max_workers=self.max_threads, pool_type="thread")
        
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
        self.config = {}


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

    def fetch_info(self):
        dir_indexes = self.df["isfile"] == False
        self.df.loc[dir_indexes, "isfile"] = self.df.loc[dir_indexes, "url"].apply(is_web_file_from_url)
        # 迭代请求文件的文件大小
        _df = self.df.query("isfile == False")
        # 判断是否有需要查询的文件
        if not len(_df):
            return pd.DataFrame()
        # 多线程执行
        for idx, row in _df.iterrows():
            url = row["url"]
            self.stream_fileinfo(url)
        df_list = list(self.stream_fileinfo.result(workers=self.max_threads, pool_type="thread"))
        # 合并结果
        df_list.append(self.df.query("isfile == True"))
        if not len(df_list):
            return pd.DataFrame()
        df_file = pd.concat(df_list, axis=0)
        df_file["isfile"] = df_file["url"].apply(is_web_file_from_url)
        self.df = df_file
        if len(df_file.query("isfile == False")):
            self.fetch_info()

    def fetch_size(self):
        # 迭代请求文件的文件大小
        logger.info(f"DataFrame File Size Finding ......")
        _df = self.df.query("size <= 0")
        # 判断是否有需要查询的文件
        if not len(_df):
            return
        # 多线程执行
        for url in _df["url"]:
            self.stream_filesize(url)
        _df["size"] = list(self.stream_filesize.result(workers=self.max_threads, pool_type="thread"))
        # 合并结果
        self.df.update(_df)
        failed_length = len(self.df.query("size <= 0"))
        if failed_length > 0:
            raise ValueError(f"Some files ({failed_length}) size is not found!")

    def fetch_content(self):
        # 迭代下载文件
        logger.info(f"DataFrame File URL Downloading ......")
        _df = self.df.copy()
        # 多线程执行
        for idx, row in _df.iterrows():
            url = row["url"]
            dst_dir = os.path.dirname(os.path.abspath(row["filepath"]))
            auto_make_dirs(dst_dir, is_dir=True)
            dst_filename = os.path.basename(os.path.abspath(row["filepath"]))
            params = {
                "method": "GET",
                "url": url,
                "chunk_size": 1024 * 1024,
                "filename": dst_filename,
                "filedir": dst_dir,
                "filepath": os.path.join(dst_dir, dst_filename),
                "filesize": row["size"],
            }
            self.stream_download(**params)
        results = self.stream_download.result(workers=self.max_threads, pool_type="thread")
        # _df["size"] = [result["filesize"] for result in results]
        self.df.update(_df)
        return results

    def run(self, isfetchinfo=True, isfetchsize=True, isfetchcontent=True):
        # 迭代请求文件的信息
        if isfetchinfo:
            self.fetch_info()
            self.df.fillna({"size": -100, "filename": "", "filepath": ""}, inplace=True)
            self.df.to_csv(self.obj_csv, index=False)
        # 迭代请求文件的文件大小
        if isfetchsize:
            self.df.fillna({"size": -100, "filename": "", "filepath":""}, inplace=True)
            filename_indexes = self.df["filename"].str.contains(".")
            self.df.loc[~filename_indexes, "filename"] = self.df.loc[~filename_indexes, "url"].apply(get_file_name_from_url)
            self.df["filepath"] = self.df["filename"].apply(lambda x: os.path.join(self.local_dir, x))
            self.fetch_size()
            self.df.to_csv(self.obj_csv, index=False)
        # 迭代下载文件
        if isfetchcontent:
            self.fetch_content()
            flag_list = [FileChecker.compare_filesize(rows["filepath"], rows["size"]) for idx, rows in self.df.iterrows()]
            self.df.to_csv(self.obj_csv, index=False)
            if all(flag_list):
                logger.info("All File Finished!")
            else:
                raise ValueError("Some File Download Failed!")

