from spided import EarthDataDownloader, StrategyCSV
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    token = ""
    csv_path = "download-list.txt"
    ed = EarthDataDownloader(username, passwd)
    stra = StrategyCSV(pd.read_csv(csv_path), local_dir="./", obj_csv="final.csv", max_threads=10)
    
    stra.df["url"] = stra.df["url"] + "?token=" + token
    stra.add_downloader(ed)
    stra.run()
