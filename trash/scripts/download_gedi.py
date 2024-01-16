from spided import EarthDataDownloader, StrategyCSV
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    urls = []
    obj_csv = "final.csv"

    df = pd.read_csv(obj_csv) if os.path.exists(obj_csv) else pd.DataFrame.from_dict({"url": urls})
    stra = StrategyCSV(df, local_dir="./", obj_csv="final.csv", max_threads=10)
    ed = EarthDataDownloader(username, passwd)
    ed.delay = 1 # 设置请求间隔, 默认为1秒
    print("登录: ", ed.login_web())
    stra.add_downloader(ed)
    stra.run()
