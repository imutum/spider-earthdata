from spided import EarthDataDownloader, StrategyCSV
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    urls = ["https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD35_L2/2022"]
    obj_csv = "final.csv"

    df = pd.read_csv(obj_csv) if os.path.exists(obj_csv) else pd.DataFrame.from_dict({"url": urls})
    stra = StrategyCSV(df, local_dir="./", obj_csv=obj_csv, max_threads=10)
    ed = EarthDataDownloader(username, passwd)
    stra.add_downloader(ed)
    stra.run()

