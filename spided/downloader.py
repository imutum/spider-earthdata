import requests
import os
import logging
from spided.util import unzip, get_temp_dir, get_final_path_from_url, compare_filesize
from spided.info import loop_info, is_web_file_from_url
import pandas as pd
from multiprocessing.pool import ThreadPool
import datetime
import time
import os, glob
import re

logging.basicConfig(format='%(name)s %(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("Downloader")
logger.setLevel(logging.INFO)


class SessionWithHeaderRedirection(requests.Session):
    def __init__(self, username, password, auth_host):
        super().__init__()
        self.auth = (username, password)
        self.auth_host = auth_host

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers['Authorization']
        return


class Downloader:
    def __init__(self):
        self.session = requests.Session()

    def _stream_download(self, method, url, filepath):
        response = self.session.request(method, url, stream=True, timeout=300)
        response.raise_for_status()
        with open(filepath, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                fd.write(chunk)

    def _stream_filesize(self, url):
        try:
            logger.debug(f"Find Size: {url}")
            response = self.session.head(url, timeout=300)
            rsp_header = response.headers
            response.raise_for_status()
            return int(rsp_header.get("Content-Length", "-1"))
        except Exception as e:
            logger.error(e)
            return -100

    def _script_download(self, method, url, objdir, filesize=None):
        filename = get_final_path_from_url(url)
        tempdir = get_temp_dir(os.path.join(objdir, filename))
        filepath = os.path.join(tempdir, filename)
        # check file size
        if os.path.exists(filepath):
            if filesize is None:
                filesize = self._stream_filesize(url)
            flag = compare_filesize(filename, filesize, tempdir)
            if flag:
                logger.info(f"Download Finished! {filename}")
            else:
                logger.info(f"Redownload! {filename}")
            return
        # download file
        logger.info(f"Downloading {url}")
        self._stream_download(method, url, filepath)

        # unzip file
        if filesize is not None and compare_filesize(filename, filesize, tempdir):
            logger.debug(f"Download Success! {filename}")
            unzip(filepath, objdir)


class EarthData(Downloader):
    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password, cookie=None) -> None:
        super().__init__()
        self.username = username
        self.password = password
        self.session = SessionWithHeaderRedirection(username, password, self.AUTH_HOST)
        if cookie:
            self.session.cookies = cookie

    def login_web(self):
        resp = self.session.get("https://urs.earthdata.nasa.gov/home")
        authenticity_token = re.findall('<meta name="csrf-token" content="(.*?)" />', resp.text)[0]
        data = {
            "authenticity_token": authenticity_token,
            "username": self.username,
            "password": self.password,
            "commit": "Log in",
        }
        resp = self.session.post("https://urs.earthdata.nasa.gov/login", data=data)
        resp = self.session.get("https://urs.earthdata.nasa.gov/profile")
        return "Country" in resp.text

    def download_one(self, url, objdir, filesize=None):
        super()._script_download("GET", url, objdir, filesize=filesize)

    def query_size_from_dataframe(self, df: pd.DataFrame, threadnum=20, step_size=1000, temp_dir="_temp_size"):
        """get size url list"""
        def update_size(df_block, thread_error_num):
            if "size" not in df_block.columns:
                df_block["size"] = -100
            while True:
                df_block_error = df_block.query("size <= 0")
                if not len(df_block_error):
                    break
                with ThreadPool(thread_error_num) as p:
                    size_list = p.starmap(self._stream_filesize,
                                          [[row["url"]] for idx, row in df_block_error.iterrows()])
                df_block_error["size"] = size_list
                thread_error_num = max(int(thread_error_num / 2), 1)
                df_block.update(df_block_error)
                time.sleep(3)
            return df_block

        if "size" not in df.columns:
            df["size"] = -100
        df = df.reset_index()
        df.index.name = "index"
        if os.path.exists(temp_dir):
            for _csv in glob.glob(os.path.join(temp_dir, "*.csv")):
                _df = pd.read_csv(_csv, encoding="utf8", index_col="index")
                df.update(_df)
        if not len(df.query("size <= 0")):
            return
        logger.info(f"DataFrame File Size Finding ......")
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)
        length = len(df)
        for idx_start in range(0, length, step_size):
            temp_csv = os.path.join(temp_dir, f"{idx_start}_{idx_end}.csv")
            if os.path.exists(temp_csv):
                continue
            idx_end = idx_start + step_size if (idx_start + step_size) <= length else length
            df_block = update_size(df.loc[idx_start:idx_end, :], threadnum)
            df_block.to_csv(temp_csv, encoding="utf8", index_label="index")
            df.update(df_block)
        ts = str(int(datetime.datetime.now().timestamp()))
        logger.info(f"DataFrame File (With Size) Resave to {ts}.csv")
        df.to_csv(f"{ts}.csv", encoding="utf8", index=False)

    def download_from_dataframe(self,
                                df: pd.DataFrame,
                                objdir,
                                threadnum=20,
                                isDownload=True,
                                step_size=1000,
                                temp_dir="_temp_size"):
        self.query_size_from_dataframe(df, threadnum=threadnum, step_size=step_size, temp_dir=temp_dir)

        # get file url list
        logger.info(f"DataFrame Total Length: {len(df)}")
        df["isfile"] = df["url"].apply(is_web_file_from_url)
        df = df.loc[df["isfile"], :]
        df_t = df.loc[df["size"] <= 0, :]
        df_t.to_csv("ErrorFileSize.csv", encoding="utf8")
        df = df.loc[df["size"] > 0, :]
        logger.info(f"DataFrame File  Length: {len(df)}")
        df["name"] = df["url"].apply(get_final_path_from_url)

        # download
        if not isDownload:
            return
        with ThreadPool(threadnum) as p:
            p.starmap(self.download_one, [[row["url"], objdir, row["size"]] for idx, row in df.iterrows()])
        flag_list = [
                compare_filesize(
                    rows["name"], rows["size"], \
                    get_temp_dir(os.path.join(objdir, rows["name"]))
                ) > 0 \
                for idx, rows in df.iterrows()
            ]
        return all(flag_list)

    def loop_info(self, save_path, urls: list, thread_num=20):
        df = loop_info(urls, thread_num, method=self.session.get)
        df.to_csv(save_path, index=False)
