from mtmtool.download import SingleConnectionDownloaderThreadPool, download_from_dataframe
from mtmtool.path import auto_make_dirs
from mtmtool.io import read_yaml
import pandas as pd
import os

# 设置参数
raw_csv = "objlist_new.csv"
obj_csv = "objlist_new.down.csv"
obj_dir = "objlist_new"

# 读取配置文件
config = read_yaml("EarthData_NASA.yaml")
username = config["username"]
password = config["password"]

# 配置Dataframe
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
for tag, _df in df.groupby("tag"):
    _obj_dir = os.path.join(obj_dir, tag)
    auto_make_dirs(_obj_dir, is_dir=True)
    download_from_dataframe(pool, _df, _obj_dir)
