from mtmtool.download import SingleConnectionDownloaderThreadPool, download_from_dataframe
from mtmtool.path import auto_make_dirs
from mtmtool.io import read_yaml
import pandas as pd


# 读取配置文件
config = read_yaml("EarthData_NASA.yaml")
username = config["username"]
password = config["password"]

for year in range(2009, 2022):
    # 配置Dataframe
    # 设置参数
    raw_csv = f"MOD35_L2.{year}.csv"
    obj_csv = f"MOD35_L2.{year}.down.csv"
    obj_dir = f"MOD35_L2/{year}"

    df = pd.read_csv(raw_csv).copy()
    df["filename"] = df["name"]

    # 创建文件夹
    auto_make_dirs(obj_dir, is_dir=True)

    # 下载
    kwargs = {
        "max_threads": 16,
        "auth": (username, password),
        "check_integrity": True,
        "timeout": 20
    }
    pool = SingleConnectionDownloaderThreadPool(**kwargs)
    download_from_dataframe(pool, df, obj_dir)