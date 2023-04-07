import os
from functools import wraps
from .log import create_stream_logger

logger = create_stream_logger("Check")


def func_args2kwargs(func, *args, **kwargs):
    args = list(args)
    params = list(func.__code__.co_varnames)
    kwargs_new_dict = {}

    # Python 3.8+ only
    try:
        for _ in range(func.__code__.co_posonlyargcount):
            kwargs_new_dict[params.pop(0)] = args.pop(0)
    except Exception:
        pass
    # Python 3
    for _ in range(func.__code__.co_argcount - 1):
        kwargs_new_dict[params.pop(0)] = args.pop(0)

    for _ in range(func.__code__.co_kwonlyargcount):
        params.pop(0)

    kwargs_new_dict[params.pop(0)] = tuple(args) if len(args) > 1 else args[0]
    kwargs_new_dict.update(kwargs)
    return kwargs_new_dict


def check_file(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        nkwargs = func_args2kwargs(func, *args, **kwargs)
        flag = check_file_auto(**nkwargs)
        if flag:
            return None
        result = func(*args, **kwargs)
        check_file_auto(**nkwargs)
        return result

    return wrapper


def check_file_auto(**kwargs):

    check_method = kwargs.get("check_method", None)
    if check_method == "size":
        return check_filesize(**kwargs)
    if check_method == None:
        return False
    return


def check_filesize(**kwargs):

    # get kwargs
    filesize = kwargs.get("fileinfo", None)
    filepath = kwargs.get("filepath", None)
    # check kwargs, must need filesize and filepath
    if filesize is None:
        raise ValueError("Must need fileinfo!")
    if filepath is None:
        raise ValueError("Must need filepath!")
    # check file exist
    if not os.path.exists(filepath):
        return False
    # check file size
    flag = compare_filesize(filepath, filesize)
    info_text = "Please Redownload!" if not flag else "Download Finished!"
    logger.info(f"{info_text} {os.path.basename(filepath)}")
    return flag


def compare_filesize(filepath: str, filesize: int) -> bool:
    """检查是否下载完成.

    Args:
        web_file_name (str): 文件名称
        web_file_size (int): 文件大小
        localdir (str, optional): 本地路径. Defaults to "./".

    Returns:
        bool: 下载完成返回True，正在下载或下载失败或没有该文件就返回False
    """
    if not os.path.exists(filepath):
        return False
    local_file_size = os.path.getsize(filepath)
    if filesize != local_file_size:
        logger.debug(f"Download Failed! Remove {filepath}")
        try:
            os.remove(filepath)
        except Exception:
            logger.debug(f"Remove {filepath} Failed!")
            return False
        return False
    else:
        return True
