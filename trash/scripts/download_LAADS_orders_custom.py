import requests
from datetime import date, timedelta
from dateutil.rrule import rrule, DAILY
import pandas as pd
import time
import httpx
import asyncio
import datetime

limits = httpx.Limits(max_keepalive_connections=4, max_connections=4)


async def get_csv_from_url(client, url, datestr):
    resp = await client.get(url)
    assert resp.status_code == 200
    print(datestr)
    return pd.DataFrame.from_dict(resp.json()).T

async def csv_query(datestr, product, areaOfInterest, tag="MODIS"):
    df_list = []
    task_list = []
    # https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product={product}&collection=61&dateRanges={datestr}..{datestr}&areaOfInterest=x-180y90,x180y-90&dayCoverage=true
    async with httpx.AsyncClient(limits=limits, timeout=None) as client:
        url = f"https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product={product}&collection=61&dateRanges={datestr}..{datestr}&areaOfInterest={areaOfInterest}&dayCoverage=true"
        req = get_csv_from_url(client, url, datestr)
        task = asyncio.create_task(req)
        task_list.append(task)
        df_list = await asyncio.gather(*task_list)

    df = pd.concat(df_list)
    df["TimeCoverage"] = "Day"
    df["url"] = "https://ladsweb.modaps.eosdis.nasa.gov" + df["fileURL"]
    df["tag"] = tag
    df["datetime"] = pd.to_datetime(df["start"])
    return df
    # df.to_csv(f"{product}.{year}.csv", index=False)

df_list_all = []
raw_csv = "objlist.csv"
raw_df = pd.read_csv(raw_csv)
for idx, row in raw_df.iterrows():
    df_temp = []
    row["areaOfInterest"] = f"x{row['lon']}y{row['lat']},x{row['lon']}y{row['lat']-0.01}"
    objtime = datetime.datetime.strptime(row["date"] + " " + row["time"], "%Y-%m-%d %H:%M") - datetime.timedelta(hours=8)
    df_temp.append(asyncio.run(csv_query(row["date"], "MOD021KM", row["areaOfInterest"], row["tag"])))
    df_temp.append(asyncio.run(csv_query(row["date"],"MOD03", row["areaOfInterest"], row["tag"])))
    objtime_up = objtime + datetime.timedelta(minutes=40)
    objtime_down = objtime - datetime.timedelta(minutes=40)
    df_temp_new = pd.concat(df_temp)
    indexes = (df_temp_new["datetime"] < objtime_up) & (df_temp_new["datetime"] > objtime_down)
    df_temp_new = df_temp_new[indexes]
    df_list_all.append(df_temp_new)
df_all = pd.concat(df_list_all)
df_all.to_csv("objlist_new.csv", index=False)
pass