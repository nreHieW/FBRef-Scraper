# from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium import webdriver
import time

# from engines.request_utils import get_proxy
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

from utils import write_to_bq, check_size, is_ubuntu


HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"}

if __name__ == "__main__":
    if is_ubuntu():  # github actions
        print("Running on Ubuntu")
        from pyvirtualdisplay import Display

        display = Display(visible=0, size=(800, 800))
        display.start()
    # url = "144.76.68.148:10801"
    options = ChromeOptions()
    # proxy = get_proxy()
    proxy = {"http": "104.194.152.35:34567", "https": "104.194.152.35:34567"}
    # proxy = {"https": url, "http": url}
    print("Using proxy: {}".format(proxy))
    options.add_argument(f"user-agent={HEADERS['user-agent']}")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--proxy-server=%s" % proxy["https"])
    prefs = {"profile.managed_default_content_settings.images": 2}  # don't load images to make faster
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)  # create driver
    # options.headless = True
    # options = Options()
    # proxy = proxy["https"]
    # ip, port = proxy.split(":")
    # options.set_preference("network.proxy.type", 1)
    # options.set_preference("network.proxy.http", ip)
    # options.set_preference("network.proxy.http_port", int(port))
    # # options.set_preference("network.proxy.https", ip)
    # # options.set_preference("network.proxy.https_port", int(port))

    # options.set_preference("network.proxy.ssl", ip)
    # options.set_preference("network.proxy.ssl_port", int(port))
    # options.set_preference("general.useragent.override", HEADERS["user-agent"])

    # # self.driver = webdriver.Firefox(options=options, service=FirefoxService(executable_path="/usr/bin/geckodriver"))
    # driver = webdriver.Firefox(options=options)
    print("=====================")
    driver.get("https://deviceandbrowserinfo.com/info_device")
    print(driver.find_element(By.XPATH, "/html/body/main/section/div/p[4]").text)
    print("=====================")
    driver.get("https://www.whoscored.com/Regions/252/Tournaments/2/England-Premier-League")
    print(driver.find_element(By.TAG_NAME, "body").text)
    driver.quit()
