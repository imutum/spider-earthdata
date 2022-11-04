import zipfile
import logging
import os
import re

logging.basicConfig(level=logging.ERROR,
                    format='%(name)s %(asctime)s %(levelname)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.DEBUG)


def get_final_path_from_url(url):
    return url[url.rfind('/') + 1:]


def is_web_file_from_url(url):
    last_text = url.split("/")[-1]  # 获取URL里最后一节字符串，用于判断是文件还是目录
    if ("." in last_text) and (re.search("(?:\.hdf)|(?:\.[a-z]{3,4})", url) is not None):  # 判断是url是否是文件夹链接
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


def compare_filesize(web_file_name: str, web_file_size: int, localdir: str = "./") -> bool:
    """检查是否下载完成.

    Args:
        web_file_name (str): 文件名称
        web_file_size (int): 文件大小
        localdir (str, optional): 本地路径. Defaults to "./".

    Returns:
        bool: 下载完成返回True，正在下载或下载失败或没有该文件就返回False
    """

    local_file_path = os.path.join(localdir, web_file_name)
    if not os.path.exists(local_file_path):
        return False
    local_file_size = os.path.getsize(local_file_path)
    if web_file_size != local_file_size:
        try:
            os.remove(local_file_path)
        except Exception:
            return False
        return False
    else:
        return True
