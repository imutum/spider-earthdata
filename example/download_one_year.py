from spided import EarthData
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    ed = EarthData(username, passwd)
    urls = ["https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD04_L2/2022"]
    csv_path = "r.csv"
    if not os.path.exists(csv_path):
        ed.loop_info(csv_path, urls, thread_num=20)
    while True:
        try:
            flag = ed.download_from_dataframe(pd.read_csv(csv_path), "./", threadnum=20)
            if flag:
                print("All File Finished!")
                break
        except Exception as e:
            print(e)