from spided.downloader import Downloader, SessionWithHeaderRedirection
import requests
import re

class EarthDataDownloader(Downloader):
    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password, cookie=None, config=None) -> None:
        if config is None:
            config = {"delay": 1}
        super().__init__(config=config)
        self.username = username
        self.password = password
        self.session = SessionWithHeaderRedirection(username, password, self.AUTH_HOST)
        if cookie:
            self.session.cookies = cookie


    def login_web(self):
        resp = self.session.get("https://urs.earthdata.nasa.gov/home")
        authenticity_token = re.findall('<meta name="csrf-token" content="(.*?)" />', resp.text)[0]
        data = {
            "authenticity_token": authenticity_token,
            "username": self.username,
            "password": self.password,
            "commit": "Log in",
        }
        resp = self.session.post("https://urs.earthdata.nasa.gov/login", data=data)
        resp = self.session.get("https://urs.earthdata.nasa.gov/profile")
        return "Country" in resp.text