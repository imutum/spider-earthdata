from mtmtool.io import read_yaml
from spided import EarthExplorerDownloader, StrategyCSV
from spided.platform.earthexplorer_usgs import point_list_to_query_coordinates_dict
from mtmtool.io import read_yaml
import pandas as pd
from spided.downloader import logger
import os


def fetch_download_url(ed: EarthExplorerDownloader, tab1, tab2, tab3):
    ed.web_save_settings(tab1)
    ed.web_save_settings(tab2)
    ed.web_save_settings(tab3)
    datasetId = tab3["selected"]
    df = ed.web_script_iter_search(datasetId=datasetId, resultsPerPage=100)
    if not len(df):
        return pd.DataFrame()
    # 获取文件下载链接
    df_list = []
    for idx, row in df.iterrows():
        df_sub = ed.web_search_file_info(row["entityId"], row["collectionId"])
        logger.info(f"文件信息获取：{row['entityId']}")
        df_sub[["entityId", "displayId", "collectionId"]] = [row["entityId"], row["displayId"], row["collectionId"]]
        df_sub = df_sub[df_sub["subEntityId"].str.contains("B10") | df_sub["subEntityId"].str.contains("QA_PIXEL")]
        for idx, row in df_sub.iterrows():
            df_sub.loc[idx, "url"] = ed.web_fetch_download_url(row["productId"], row["subEntityId"])
        df_list.append(df_sub)
    df = pd.concat(df_list)
    return df


if __name__ == '__main__':
    config_dict = read_yaml("earthexplorer.yaml")
    obj_csv = "download-list.csv"
    # 查询文件
    shp_points = [(34.0, 110.0), (34.0, 114.0), (40.0, 114.0), (40.0, 120.0), (50.0, 120.0), (50.0, 130.0),
                  (40.0, 130.0), (40.0, 121.0), (32.0, 121.0), (32.0, 120.0), (28.0, 120.0), (28.0, 110.0)]
    coordinates = point_list_to_query_coordinates_dict(shp_points)
    data_tab1 = {
        "tab": 1,
        "destination": 2,
        "coordinates": coordinates,
        "format": "dms",
        "dStart": "01/15/2021",
        "dEnd": "01/15/2021",
        "searchType": "Std",
        "includeUnknownCC": "1",
        "maxCC": 100,
        "minCC": 0,
        "months": ["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
        "pType": "polygon"
    }
    data_tab2 = {"tab": 2, "destination": 3, "cList": ["5e81f14f59432a27"], "selected": 0}
    data_tab3 = {
        "tab": 3,
        "destination": 4,
        "criteria": {
            "5e81f14f59432a27": {
                "61af93b8fad2acf5": ["8"],
                "5e81f14fff5055a3": ["T1"]
            }
        },
        "selected": "5e81f14f59432a27"
    }
    ed = EarthExplorerDownloader(username=config_dict["username"], password=config_dict["password"])
    if not os.path.exists(obj_csv):
        df_list = []
        for dates in [
                "01/15/2022", "02/15/2022", "03/15/2022", "04/15/2022", "05/15/2022", "06/15/2022", "07/15/2022",
                "08/15/2022", "09/15/2022", "10/15/2022", "11/15/2022", "12/15/2022"
        ]:
            logger.info("开始查询：" + dates)
            data_tab1["dStart"] = dates
            data_tab1["dEnd"] = dates
            df = fetch_download_url(ed, data_tab1, data_tab2, data_tab3)
            df_list.append(df)
        pd.concat(df_list).to_csv(obj_csv, index=False)
    # 下载文件
    df = pd.read_csv(obj_csv)
    df["isfile"] = True
    df["filename"] = df["subEntityId"].str[3:-4] + ".TIF"
    df.drop(columns=["size"], inplace=True)
    strgy = StrategyCSV(df, local_dir="./", obj_csv=obj_csv, max_threads=15)
    strgy.add_downloader(ed)
    strgy.run()
