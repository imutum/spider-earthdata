import os
from functools import wraps
from typing import Any
from collections.abc import Callable, Generator

from mtmtool.log import stream_logger


logger = stream_logger("FileCheck")


def filecheck(func:Callable=None):
    @wraps(func)
    def wrapper(*args, **kwargs):
        check_method = kwargs.get("check_method", "filesize")
        if check_method == "filesize":
            # 函数运行前检查文件大小
            filesize = kwargs.get("filesize", -1)
            filepath = kwargs.get("filepath", "")
            isRemoveFailFile = kwargs.get("isRemoveFailFile", True)
            if len(filepath) and filesize > 0:
                is_file_equal = FileChecker.check_file_by_size(filepath, filesize, isRemoveFailFile=isRemoveFailFile)
            else:
                is_file_equal = False
            # 如果文件大小不一致，才运行函数
            if not is_file_equal:
                result = func(*args, **kwargs)
            else:
                result = None
            # 函数运行后检查文件大小
            result_final = {}
            if isinstance(result, dict):
                filesize = result.get("filesize", filesize)
                filepath = result.get("filepath", filepath)
                is_file_equal = FileChecker.check_file_by_size(filepath, filesize)
                result_final = result.copy()
            result_final["is_file_equal"] = is_file_equal
            return result_final
        
    return wrapper


class FileChecker:
    @staticmethod
    def check_file_by_size(filepath: str, filesize: int, isRemoveFailFile: bool=True) -> bool:
        # check file exist
        if not os.path.exists(filepath):
            return False
        # check file size
        is_equal_filesize = FileChecker.compare_filesize(filepath, filesize)
        if is_equal_filesize:
            info_text = "File Downloaded ({:.4f}MB)!".format(filesize / 1024 / 1024)
        else:
            info_text = "File Redownload Please!"
            if isRemoveFailFile:
                # 文件大小不一致，删除文件，重新下载
                try:
                    os.remove(filepath)
                except Exception:
                    logger.error(f"Remove {filepath} Failed!")
        logger.info(f"{info_text} {os.path.basename(filepath)}")
        return is_equal_filesize


    @staticmethod
    def compare_filesize(filepath: str, filesize: int) -> bool:
        """检查文件大小是否和给的一样

        Parameters
        ----------
        filepath : str
            文件路径
        filesize : int
            文件大小

        Returns
        -------
        bool
            是否相同
        """
        if not os.path.exists(filepath):
            return False
        local_file_size = os.path.getsize(filepath)
        if filesize != local_file_size:
            return False
        else:
            return True
    