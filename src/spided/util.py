import zipfile
import pandas as pd
import os
import re
from .log import create_stream_logger

logger = create_stream_logger("Util")


def get_final_path_from_url(url):
    return url[url.rfind('/') + 1:]


def get_file_name_from_url(url):
    return get_final_path_from_url(url).split("?")[0]


def is_web_file_from_url(url):
    last_text = url.split("/")[-1]  # 获取URL里最后一节字符串，用于判断是文件还是目录
    has_file_suffix = (re.search("(?:\.hdf)(?:\.tif)|(?:\.[a-z]{1,4})", last_text.lower()) is not None)
    # 判断是url是否是文件链接
    if ("." in last_text) and has_file_suffix:
        return True
    else:
        return False


def unzip(filepath, objdir):
    if filepath.endswith(".zip"):
        azip = zipfile.ZipFile(filepath)
        azip.extractall(objdir)


def get_temp_dir(objfilepath, zipdirname="zipfiles"):
    dir_path = os.path.dirname(objfilepath)
    if objfilepath.endswith(".zip"):
        zip_dir = os.path.join(dir_path, zipdirname)
        if not os.path.isdir(zip_dir):
            os.mkdir(zip_dir)
        return zip_dir
    else:
        return dir_path


def get_csv_from_url(url):
    return pd.read_csv(url + ".csv", dtype=str)


def get_json_from_url(url):
    return pd.read_json(url + ".json", dtype=False)