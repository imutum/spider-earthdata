from spided import EarthDataDownloader, StrategyCSV
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    token = ""
    csv_path = "download-list.txt"
    obj_csv = "final.csv"
    
    df = pd.read_csv(obj_csv) if os.path.exists(obj_csv) else pd.read_csv(csv_path)
    df["url"] = df["url"] + "?token=" + token
    stra = StrategyCSV(df, local_dir="./", obj_csv=obj_csv, max_threads=10)
    ed = EarthDataDownloader(username, passwd)
    stra.add_downloader(ed)
    stra.run()
