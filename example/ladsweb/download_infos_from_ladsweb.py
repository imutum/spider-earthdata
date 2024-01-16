import asyncio
from datetime import date

import httpx
import requests
import pandas as pd
from dateutil.rrule import rrule, DAILY


limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)


def get_url_of_ladsweb_products(
    product: str,
    collection: str,
    dateRanges: str,
    areaOfInterest=None,
    dayCoverage=None,
    nightCoverage=None,
    dnboundCoverage=None,
):
    # https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product=MOD021KM&collection=61&dateRanges=2024-01-01&areaOfInterest=x-180y90,x180y-90&dayCoverage=true&nightCoverage=true&dnboundCoverage=true
    # https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product=MOD021KM&collection=61&dateRanges=2024-01-01..2024-01-01&areaOfInterest=x-180y90,x180y-90&dayCoverage=true&nightCoverage=true&dnboundCoverage=true
    # https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product=MOD021KM&collection=61&dateRanges=2024-01-01&areaOfInterest=x-180y90,x180y-90
    raw_url = f"https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/"
    params = {
        "product": product,
        "collection": collection,
        "dateRanges": dateRanges,
        "areaOfInterest": areaOfInterest,
        "dayCoverage": dayCoverage,
        "nightCoverage": nightCoverage,
        "dnboundCoverage": dnboundCoverage,
    }
    url = requests.models.Request(method="GET", url=raw_url, params=params).prepare().url
    return url.replace("?", "").replace("True", "true")


def get_str_coverage(params):
    # 获取覆盖范围的字符串，用于标记此次查询数据时的覆盖范围类型，比如Day/Night/DNB
    coverage_str_list = []
    if params["dayCoverage"] == True:
        coverage_str_list.append("Day")
    if params["nightCoverage"] == True:
        coverage_str_list.append("Night")
    if params["dnboundCoverage"] == True:
        coverage_str_list.append("DNB")
    if len(coverage_str_list) == 0:  # 如果没有设置覆盖范围，则默认为全部覆盖
        coverage_str_list = ["Day", "Night", "DNB"]
    return "/".join(coverage_str_list)


async def get_data_from_url(client: httpx.Client, url):
    resp = await client.get(url)
    assert resp.status_code == 200
    print(f"Get {resp.request.url.path} Success!!!")
    return pd.DataFrame.from_dict(resp.json()).T


async def batch_get_data_from_urls(urls):
    df_list = []
    task_list = []
    async with httpx.AsyncClient(limits=limits, timeout=None) as client:
        for url in urls:
            req = get_data_from_url(client, url)
            task = asyncio.create_task(req)
            task_list.append(task)
        df_list = await asyncio.gather(*task_list)
    df = pd.concat(df_list)
    return df


if __name__ == "__main__":
    # 获取MOD021KM产品的2021年的白天数据
    products = ["MOD021KM"]
    for product in products:
        for year in range(2021, 2022):
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            dates = list(rrule(DAILY, dtstart=start_date, until=end_date))
            dates_str = [date_t.strftime("%Y-%m-%d") for date_t in dates]
            # params 参数请前往 https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-search/advanced-search/ 进行查询
            params = {
                "product": product,
                "collection": "61",
                "areaOfInterest": "x-180y90,x180y-90",
                "dayCoverage": True,
                "nightCoverage": None,
                "dnboundCoverage": None,
            }
            urls = [get_url_of_ladsweb_products(dateRanges=f"{date_str}", **params) for date_str in dates_str]
            df = asyncio.run(batch_get_data_from_urls(urls))
            df["TimeCoverage"] = get_str_coverage(params)
            df["url"] = "https://ladsweb.modaps.eosdis.nasa.gov" + df["fileURL"]
            df.to_csv(f"{product}.{year}.csv", index=False)
