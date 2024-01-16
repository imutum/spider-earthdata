from spided.downloader import Downloader, SessionWithHeaderRedirection, logger
import requests
import re
import json
from urllib.parse import quote_plus
import time
from mtmtool.io import yaml
import pandas as pd
from spided.util import unzip

DefaultHeaders = """
Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7Accept-Encoding: gzip, deflate, br"
Accept-Language: "zh-CN,zh;q=0.9,en;q=0.8"
Connection: keep-alive
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36
sec-ch-ua: '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"'
sec-ch-ua-mobile: "?0"
sec-ch-ua-platform: "Windows"
"""


def build_payload_content(data: dict):
    return "data=" + quote_plus(json.dumps(data, separators=(",", ":")).encode("utf-8"))


def point_list_to_query_coordinates_dict(point_list: list):
    coordinates = [
        {
            "c": str(idx),  # 序号
            "a": "{:.4f}".format(float(point[0])),  # 纬度
            "o": "{:.4f}".format(float(point[1]))  # 经度
        } for idx, point in enumerate(point_list)
    ]
    return coordinates


class EarthExplorerDownloader(Downloader):
    AUTH_HOST = 'earthexplorer.usgs.gov'

    def __init__(self, username=None, password=None, cookie=None) -> None:
        super().__init__()
        self.username = username
        self.password = password
        if username and password:
            self.session = requests.Session()
            self.web_login(username, password)
        else:
            self.session = requests.Session()
        if cookie:
            self.add_cookie(cookie)
        self.session.headers.update(yaml.load(DefaultHeaders, Loader=yaml.Loader))

    def web_login(self, username, password):
        pattern = 'name="csrf" value="(.*?)">'
        url = "https://ers.cr.usgs.gov/login"
        resp = self.session.get(url)
        login_csrf = re.findall(pattern, resp.text)[0]
        data = {"username": username, "password": password, "csrf": login_csrf}
        resp = self.session.post(url, data=data, allow_redirects=False)
        if resp.status_code in [200, 302]:
            logger.info("登录成功！")
            return True
        logger.info("登录失败！")
        self.session.get("https://earthexplorer.usgs.gov/")
        return False

    def web_save_settings(self, data_tab: dict):
        url = "https://earthexplorer.usgs.gov/tabs/save"
        resp = self.session.post(url, data=build_payload_content(data_tab))
        if resp.status_code == 200 and resp.text == "1":
            logger.info("设置上传成功！")
            return True
        logger.info("设置上传失败！")
        return False

    def web_search_one_page(self, datasetId, resultsPerPage=10, pageNum=1):
        url = "https://earthexplorer.usgs.gov/scene/search"
        data = {
            "datasetId": datasetId,
            "resultsPerPage": resultsPerPage,
            "pageNum": pageNum,
        }
        resp = self.session.post(url, data=data)
        pattern = 'data-entityId="(.*?)"[\r\n ]*data-displayId="(.*?)"[\r\n ]*data-collectionId="(.*?)"'
        granual_lists = re.findall(pattern, resp.text)
        df = pd.DataFrame(granual_lists, columns=["entityId", "displayId", "collectionId"])
        df["page_num_current"] = pageNum
        page_num_max = re.findall('min="1" max="(.*?)" /> of', resp.text)
        try:
            df["page_num_max"] = page_num_max[0]
        except IndexError:
            df["page_num_max"] = -1
        return df

    def web_search_file_info(self, entityId, collectionId):
        url = f"https://earthexplorer.usgs.gov/scene/downloadoptions/{collectionId}/{entityId}"
        resp = self.session.post(url)
        pattern = 'data-entityId="(.*?)"[\r\n ]*data-productId="(.*?)" title="Download Product/File">'
        products = re.findall(pattern, resp.text)
        df = pd.DataFrame(products, columns=["subEntityId", "productId"])
        return df

    def web_fetch_download_url(self, productId, subEntityId):
        url = f"https://earthexplorer.usgs.gov/download/{productId}/{subEntityId}/"
        resp = self.session.get(url, allow_redirects=False)
        download_url = resp.json()["url"]
        return download_url

    def web_fetch_metadata_export_id(self, exportName="result", format="csv", datasetId=None):
        url = "https://earthexplorer.usgs.gov/export/create"
        data = {
            "exportName": exportName,
            "format": format,
            "datasetId": datasetId,
        }
        resp = self.session.post(url, data=data)
        return resp.json()["exportId"]

    def web_fetch_metadata_export_url(self, exportId):
        url = f"https://earthexplorer.usgs.gov/export/view/{exportId}/"
        resp = self.session.get(url)
        s = re.findall('Your request is being processed', resp.text)
        if len(s):
            time.sleep(10)
            return False
        else:
            url = re.findall('data-url="(.*?)">Download', resp.text)[0]
            return url

    def web_download_metadata(self, url, filename=None):
        resp = self.session.head(url)
        filename = resp.json()["filename"] if filename is None else filename
        self._stream_download("GET", url, filename)
        return filename

    def web_script_download_metadata(self, datasetId, exportName="result", format="csv"):
        """未完成，请勿使用"""
        export_id = self.web_fetch_metadata_export_id(exportName="rrrs", datasetId=datasetId)
        for _ in range(10):
            metadata_url = self.web_fetch_metadata_export_url(export_id)
            if metadata_url:
                break
        else:
            raise ValueError("获取metadata下载链接失败！")
        filename = self.web_download_metadata(metadata_url)
        unzip(filename, "./")

    def web_script_iter_search(self, datasetId, resultsPerPage=10):
        df = self.web_search_one_page(datasetId=datasetId, resultsPerPage=resultsPerPage)
        if len(df) and int(df["page_num_max"].values[0]) > 1:
            df_list = [df]
            for i in range(2, int(df["page_num_max"].values[0]) + 1):
                df = self.web_search_one_page(datasetId=datasetId, resultsPerPage=resultsPerPage, pageNum=i)
                logger.info(f"第{i}页")
                df_list.append(df)
            df = pd.concat(df_list)
        return df