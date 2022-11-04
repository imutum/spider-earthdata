# Spider-Earthdata

A tool for downloading [EarthData](https://earthdata.nasa.gov/)

### Usage:
```
git clone <url>
pip install -e spider-earthdata
```

### Examples:
```
# download_one_year.py (in forder 'example') for "LAADS DAAC"
from spided import EarthData
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    csv_path = "r.csv"
    urls = ["https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD04_L2/2022"]


    ed = EarthData(username, passwd)
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
```

```
# download_order_csv.py (in forder 'example')  for "LP DAAC"
from spided import EarthData
import pandas as pd
import os
if __name__ == '__main__':
    username = ""
    passwd = ""
    csv_path = "r.csv"

    ed = EarthData(username, passwd)
    while True:
        try:
            flag = ed.download_from_dataframe(pd.read_csv(csv_path), "./", threadnum=20)
            if flag:
                print("All File Finished!")
                break
        except Exception as e:
            print(e)
```