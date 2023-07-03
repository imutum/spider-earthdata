import requests
from requests import Session
from spided import StrategyCSV, Downloader
from spided.platform.s5phub import S5PHubDownloader
import pandas as pd
import os, glob


if __name__ == '__main__':
    obj_csv = "final.csv" # 保存查询结果的csv文件，如果存在则读取，否则重新查询
    max_threads = 4 # 下载时的最大线程数（同时下载的文件数）
    dst_dir = "./" # 下载文件的保存目录, 如果不存在则创建
    page_limit = 100 # 每页查询的最大数量，可以根据需要修改，如果超出最大页数（200页）则会提示报错
    delay_seconds = 5 # 当出现429错误时，可增加等待的秒数，来避免过多的请求被拒绝
    
    # params是查询的具体参数，如果需要修改查询参数，请参考 https://s5phub.copernicus.eu/dhus/#/home，如果不包含对应的关键字则将不作为查询条件。比如，将processinglevel这一行注释掉，则在查询时将不会限制处理级别
    params = {
        "footprint": "POLYGON((63.072509078384826 44.60114111994912,54.71995269788564 35.585335506048665,75.67273301990708 27.37869064921705,81.9193029625881 36.307754908681446,63.072509078384826 44.60114111994912,63.072509078384826 44.60114111994912))", # 用于查询的多边形, WKT格式
        "ingestionDate": ["2023-06-01", "2023-06-15"], # 用于查询的时间范围
        "producttype": "L2__SO2___", # 用于查询的产品类型
        "platformname": "Sentinel-5", # 用于查询的卫星平台
        "processinglevel": "L2", # 用于查询的处理级别
        "processingmode": "Near real time", # 用于查询的处理模式
    }
    

    # ======以下是程序的主要逻辑，不需要修改======
    s5p = S5PHubDownloader()
    s5p.login_web()
    if os.path.exists(obj_csv):
        df = pd.read_csv(obj_csv)
    else:
        products = s5p.query(limit=page_limit, **params)
        products_ = []
        for product in products:
            product["filename"] = product["summary"][1].split(":")[1].strip()
            products_.append(product)
        df = pd.DataFrame.from_dict(products)
        df["isfile"] = True
        df["url"] = f"https://s5phub.copernicus.eu/dhus/odata/v1/Products('"+ df["uuid"] +"')/$value"
    # 如果不存在目标目录，则创建
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    stra = StrategyCSV(df, local_dir=dst_dir, obj_csv=obj_csv, max_threads=max_threads)
    s5p.delay = delay_seconds
    stra.add_downloader(s5p)
    stra.run()
    pass