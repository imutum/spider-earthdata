import time
import threading
import os
from http.cookies import BaseCookie

import requests
from requests.utils import cookiejar_from_dict
from mtmtool.log import stream_logger

from .check import filecheck
from .util import *

logger = stream_logger("Down", log_level="DEBUG")


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

    def __init__(self, config=None):
        self.session = requests.Session()
        self.delay = 0
        self.lock = threading.Lock()
        self.config = config if config is not None else {}
        # use_url_content_filename: 是否使用url中的文件名作为下载文件的文件名

    def add_cookie(self, cookie):
        if isinstance(cookie, str):
            cookie_simplecookie_type = BaseCookie(cookie)
            cookie_dict = {i.key: i.value for i in cookie_simplecookie_type.values()}
            cookie = cookiejar_from_dict(cookie_dict, cookiejar=None, overwrite=True)
        self.session.cookies.update(cookie)


    def get_info_from_response(self, response):
        filename = get_content_filename(response.headers)
        filesize = get_content_length(response.headers)
        info_dict = {"response": response}
        if filename:
            info_dict["filename"] = filename
        if filesize > 0:
            info_dict["filesize"] = filesize
        return info_dict

    @filecheck
    def _stream_download(self, method, url, chunk_size=1024 * 1024, **kwargs):
        # 请求文件
        response = self.session.request(method, url, stream=True, timeout=300, allow_redirects=True)
        response.raise_for_status()
        infos = self.get_info_from_response(response)
        # 获取文件名
        filename = infos.get("filename", get_file_name_from_url(url))
        dst_dir = kwargs.get("filedir", "./")
        dst_filename = kwargs.get("filename", filename)
        if self.config.get("use_url_content_filename", False):
            dst_filename = filename
        dst_filepath = os.path.join(dst_dir, dst_filename)
        infos["filepath"] = dst_filepath
        # 下载文件
        logger.debug(f"Downloading File: {dst_filename}")
        with open(dst_filepath, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
        # 返回结果
        return infos

    def _stream_filesize(self, url):
        try:
            if self.delay > 0 :
                self.lock.acquire()
                logger.debug(f"Find Size: {url}")
                response = self.session.head(url, timeout=300, allow_redirects=True)
                time.sleep(self.delay)
                self.lock.release()
            else:
                logger.debug(f"Find Size: {url}")
                response = self.session.head(url, timeout=300, allow_redirects=True)
            response.raise_for_status()
            return get_content_length(response.headers)
        except Exception as e:
            logger.error(e)
            raise e

    def _subnode_fileinfo(self, url):
        logger.debug(f"Find Subnode Fileinfo {get_file_name_from_url(url)}")
        if is_web_file_from_url(url):  # 判断是url是否是文件夹链接
            return
        for method in [get_csv_from_url, get_json_from_url]:
            try:
                df = method(url)
                break
            except Exception as e:
                continue
        else:
            raise Exception("Can not get fileinfo from url!")
        df.loc[:, "url"] = df.loc[:, "name"].apply(lambda x: url + "/" + str(x))
        return df
