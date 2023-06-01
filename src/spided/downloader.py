import requests
from .log import create_stream_logger
from .check import check_file
from .util import get_file_name_from_url, is_web_file_from_url, get_csv_from_url, get_json_from_url
from requests.utils import cookiejar_from_dict
from http.cookies import BaseCookie

logger = create_stream_logger("Downloader")


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

    def add_cookie(self, cookie):
        if isinstance(cookie, str):
            cookie_simplecookie_type = BaseCookie(cookie)
            cookie_dict = {i.key: i.value for i in cookie_simplecookie_type.values()}
            cookie = cookiejar_from_dict(cookie_dict, cookiejar=None, overwrite=True)
        self.session.cookies.update(cookie)

    @check_file
    def _stream_download(self, method, url, filepath=None, chunk_size=1024 * 1024, check_method=None, fileinfo=None):
        logger.debug(f"Download Content: {get_file_name_from_url(url)}")
        response = self.session.request(method, url, stream=True, timeout=300, allow_redirects=True)
        response.raise_for_status()
        if filepath is None:
            raise ValueError("filepath is None!")
        with open(filepath, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

    def _stream_filesize(self, url):
        try:
            logger.debug(f"Find Size: {get_file_name_from_url(url)}")
            response = self.session.head(url, timeout=300, allow_redirects=True)
            rsp_header = response.headers
            response.raise_for_status()
            return int(rsp_header.get("Content-Length", "-1"))
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
