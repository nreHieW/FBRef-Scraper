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
FBrefRateLimitException = fbref_module.FBrefRateLimitException

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from cloudscraper import CloudScraper

import requests
import random
from utils.request_utils import setup_proxies


class FBRefWrapper(FBref):
    def __init__(self):
        super().__init__(wait_time=0.1)
        self.scraper = CloudScraper()
        self.working_proxies = setup_proxies()
        self.current_proxy = None

    def _get_next_proxy(self):
        """Get the next available proxy from the working list and return both URL and dict format"""
        if self.working_proxies:
            proxy_url = random.choice(self.working_proxies)
            proxy_dict = {"http": f"http://{proxy_url}", "https": f"http://{proxy_url}"}
            return proxy_url, proxy_dict
        return None, None

    def _remove_failed_proxy(self, proxy_url):
        """Remove a failed proxy from the working proxies list"""
        if proxy_url in self.working_proxies:
            self.working_proxies.remove(proxy_url)
            print(f"Removed failed proxy: {proxy_url}. {len(self.working_proxies)} proxies remaining.")

    def _driver_init(self) -> None:
        """Private, creates a headless selenium webdriver"""
        options = Options()
        options.add_argument("--incognito")
        options.add_argument("--headless")
        options.add_argument("--log-level=2")
        prefs = {"profile.managed_default_content_settings.images": 2}  # don't load images
        options.add_experimental_option("prefs", prefs)

        # Add proxy configuration to Chrome options if available
        if self.current_proxy:
            options.add_argument(f"--proxy-server=http://{self.current_proxy}")

        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    def _get(self, url: str, max_retries: int = 3) -> requests.Response:
        """Private, uses cloudscraper to get response with proxy rotation and failure handling."""
        for attempt in range(max_retries):
            try:
                proxy_url, proxy_dict = self._get_next_proxy()

                if proxy_dict:
                    print(f"Attempting request with proxy: {proxy_url}")
                    response = self.scraper.get(url, proxies=proxy_dict, timeout=30)
                else:
                    print("No proxy available, making direct request")
                    response = self.scraper.get(url, timeout=30)

                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    raise FBrefRateLimitException()
                else:
                    print(f"Request failed with status {response.status_code}")
                    if proxy_url:
                        self._remove_failed_proxy(proxy_url)

            except Exception as e:
                print(f"Request error with {proxy_url}: {e}")
                if proxy_url:
                    self._remove_failed_proxy(proxy_url)

        print("All proxy attempts failed, trying direct connection")
        try:
            response = self.scraper.get(url, timeout=30)
            if response.status_code == 429:
                raise FBrefRateLimitException()
            return response
        except Exception as e:
            print(f"Direct connection also failed: {e}")
            raise

    def _driver_get(self, url: str, max_retries: int = 3) -> None:
        """Private, calls driver.get() with proxy rotation and failure handling."""
        for attempt in range(max_retries):
            try:
                # Set current proxy for driver initialization
                self.current_proxy, _ = self._get_next_proxy()

                # Initialize driver with current proxy (if driver not already initialized)
                if not hasattr(self, "driver") or self.driver is None:
                    self._driver_init()

                self.driver.get(url)
                if "429 error" in self.driver.page_source:
                    self._driver_close()
                    raise FBrefRateLimitException()

                # If we get here, the request was successful
                return

            except Exception as e:
                print(f"Driver error with proxy {self.current_proxy}: {e}")
                if self.current_proxy:
                    self._remove_failed_proxy(self.current_proxy)

                if hasattr(self, "driver") and self.driver:
                    self._driver_close()

        print("All proxy attempts failed for driver, trying direct connection")
        self.current_proxy = None
        self._driver_init()
        self.driver.get(url)
        if "429 error" in self.driver.page_source:
            self._driver_close()
            raise FBrefRateLimitException()

    def quit(self) -> None:
        super()._driver_close()
