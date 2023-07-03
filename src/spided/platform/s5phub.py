import requests
from spided.downloader import Downloader, SessionWithHeaderRedirection, logger


class S5PHubDownloader(Downloader):
    MAXPAGES = 1000

    def login_web(self):
        data = {
            "login_username":"s5pguest",
            "login_password":"s5pguest",
        }
        resp = self.session.post("https://s5phub.copernicus.eu/dhus//login", data=data)
        
    def filter_keywords_to_string(self, **kwargs):
        list_shape = []
        list_date = []
        list_type = []
        if "footprint" in kwargs:
            list_shape.append(f"footprint:\"Intersects({kwargs['footprint']})\"")
        if "ingestionDate" in kwargs:
            list_date.append(f"ingestionDate:[{kwargs['ingestionDate'][0]}T00:00:00.000Z TO {kwargs['ingestionDate'][1]}T23:59:59.999Z ]")
        if "producttype" in kwargs:
            list_type.append(f"producttype:{kwargs['producttype']}")
        if "platformname" in kwargs:
            list_type.append(f"platformname:{kwargs['platformname']}")
        if "processinglevel" in kwargs:
            list_type.append(f"processinglevel:{kwargs['processinglevel']}")
        if "processingmode" in kwargs:
            list_type.append(f"processingmode:{kwargs['processingmode']}")
        str_shape = " AND ".join(list_shape)
        str_date = " AND ".join(list_date)
        str_type = " AND ".join(list_type)
        str_type = f"({str_type})"
        str_total = " ) AND ( ".join([i for i in [str_shape, str_date, str_type] if i != ""])
        str_total = f"( {str_total})"
        return str_total

    def query(self, limit = 100, **kwargs):
        url = "https://s5phub.copernicus.eu/dhus/api/stub/products"
        str_filter = self.filter_keywords_to_string(**kwargs)
        params = {
            "filter": str_filter,
            "offset": 0,
            "limit": limit,
            "sortedby": "ingestiondate",
            "order": "desc",
        }
        self.session.headers.update({"Accept": "application/json, text/plain, */*"})
        products = []
        offset = 0
        for _ in range(self.MAXPAGES):
            logger.info(f"querying page {offset//limit+1}")
            params["offset"] = offset
            resp = self.session.get(url, params=params)
            _json = resp.json()
            products += _json["products"]
            offset = len(products)
            if offset >= _json["totalresults"]:
                break
        else:
            raise Exception(f"too many pages (>{self.MAXPAGES} pages)! Please check your query or modify limit number!")
        assert len(products) == _json["totalresults"], "totalresults != len(products)"
        return products