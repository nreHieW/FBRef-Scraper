from ScraperFC.fbref import FBref
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

import time
import requests


class FBRefWrapper(FBref):
    def __init__(self):
        super().__init__()

    def _driver_init(self) -> None:
        """Private, creates a headless selenium webdriver"""
        options = Options()
        options.add_argument("--incognito")
        options.add_argument("--headless")
        options.add_argument("--log-level=2")
        prefs = {"profile.managed_default_content_settings.images": 2}  # don't load images
        options.add_experimental_option("prefs", prefs)
        self.driver = self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    def _get(self, url: str) -> requests.Response:
        try:
            return super()._get(url)
        except Exception as e:
            print("Error in _get for url", url, e)
            time.sleep(10)
            return self._get(url)

    def _driver_get(self, url: str) -> None:
        try:
            return super()._driver_get(url)
        except Exception as e:
            print("Error in _driver_get for url", url, e)
            time.sleep(10)
            return self._driver_get(url)

    def quit(self) -> None:
        super()._driver_close()
