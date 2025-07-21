import sys
import importlib.util

# This is needed because importing sofascore causes issues with GitHub Actions See: https://github.com/nreHieW/FBRef-Scraper/actions/runs/16408123054/job/46357619338
# Import FBref directly from the fbref module to avoid ScraperFC's __init__.py
spec = importlib.util.find_spec("ScraperFC.fbref")
if spec is None:
    raise ImportError("Could not find ScraperFC.fbref module")
fbref_module = importlib.util.module_from_spec(spec)
sys.modules["ScraperFC.fbref"] = fbref_module
spec.loader.exec_module(fbref_module)
FBref = fbref_module.FBref

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

import time
import requests


class FBRefWrapper(FBref):
    def __init__(self):
        super().__init__()

    def __enter__(self):
        """Context manager entry point"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - ensures cleanup even if exception occurs"""
        self.quit()
        return False

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
