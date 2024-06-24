import requests
import time
import concurrent.futures
import pandas as pd
import random
from bs4 import BeautifulSoup
import re
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
import time
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

ip_pattern = r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"

HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"}


def setup_proxies():
    response = requests.get("https://www.sslproxies.org/", headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"})

    proxies = []
    soup = BeautifulSoup(response.text, "html.parser")
    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) == 0:
            continue
        proxies.append({"ip": tds[0].string, "port": tds[1].string})

    proxies = [x for x in proxies if "-" not in x]  # remove date
    response = requests.get("https://free-proxy-list.net/", headers=HEADERS)

    df = pd.read_html(response.text)[0]
    for _, row in df.iterrows():
        proxies.append(
            {
                "ip": row["IP Address"],
                "port": row["Port"],
            }
        )
    proxy_urls = [
        f"{proxy['ip']}:{proxy['port']}"
        for proxy in proxies
        if proxy["ip"] and proxy["port"] and "-" not in f"{proxy['ip']}:{proxy['port']}" and len(f"{proxy['ip']}:{proxy['port']}".split(":")) == 2 and len(f"{proxy['ip']}:{proxy['port']}".split(".")) == 4
    ]
    response = requests.get("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt")
    proxy_urls += response.text.split("\n")
    proxy_urls = list(set(proxy_urls))

    print(f"Found {len(proxy_urls)} proxies")
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        valid_proxies = list(executor.map(lambda proxy_url: test_proxy(proxy_url), proxy_urls))

    # Filter out None values
    valid_proxies = [proxy for proxy in valid_proxies if proxy]
    print(f"Testing {len(valid_proxies)} proxies")
    valid_proxies = [proxy for proxy in valid_proxies if test_whoscored(proxy, timeout=20)]
    valid_proxies = list(set(valid_proxies))
    return valid_proxies


def test_whoscored(proxy_url, timeout=60):
    try:
        ip, port = proxy_url.split(":")
        options = FirefoxOptions()
        options.set_preference("network.proxy.type", 1)
        options.set_preference("network.proxy.http", ip)
        options.set_preference("network.proxy.http_port", int(port))
        options.set_preference("network.proxy.ssl", ip)
        options.set_preference("network.proxy.ssl_port", int(port))
        options.set_preference("general.useragent.override", HEADERS["user-agent"])
        driver = webdriver.Firefox(options=options)
        driver.set_page_load_timeout(timeout)
        driver.get("https://www.whoscored.com")
        print("TESTING", proxy_url, driver.title)
        if "Football Statistics | Football Live Scores | WhoScored.com" in driver.title:
            driver.close()
            driver.quit()
            return proxy_url
        else:
            driver.close()
            driver.quit()
            return None
    except Exception as e:
        # print(f"An error occurred: {e}")
        return None


def get_my_ip(proxies=None, verbose: bool = True):
    try:
        response = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=5)
        response.raise_for_status()  # Raise an exception for HTTP errors
        ip_info = response.json()
        first = ip_info["origin"]
        # response = requests.get("https://deviceandbrowserinfo.com/info_device", proxies=proxies, timeout=5)
        # soup = BeautifulSoup(response.text, "html.parser")
        # second = soup.find("p", {"style": "white-space:pre;"}).text
        # second = re.findall(ip_pattern, second)[0]
        # if first == second:
        return first
    except requests.exceptions.RequestException as e:
        if verbose:
            print(f"An error occurred: {e}")
        return None


def test_proxy(proxy_url):
    proxy_dict = {"http": proxy_url, "https": proxy_url}
    c = 0
    for _ in range(10):
        ip = get_my_ip(proxies=proxy_dict, verbose=False)
        if ip and ip.startswith(proxy_url.split(":")[0]):
            c += 1
        else:
            return None
    if c == 10:
        return proxy_url
    return None


PROXIES = setup_proxies()
print(f"Found {len(PROXIES)} valid proxies")


def get_proxy():
    idx = random.randint(0, len(PROXIES) - 1)
    return {"http": PROXIES[idx], "https": PROXIES[idx]}


def get_request(url: str, timeout: int = 5, max_iter: int = 100, verbose: bool = True, proxy=None) -> requests.Response:
    # url = url.replace("https://", "http://")
    counter = 0
    while True:
        try:
            response = requests.get(url, headers=HEADERS, proxies=get_proxy() if proxy else None, timeout=timeout)
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                timeout = int(response.headers.get("Retry-After"))
                if verbose:
                    print(f"Rate limited for url {url}. Retrying in {timeout} seconds")
                if len(PROXIES) < 2:
                    time.sleep(timeout)
            else:
                counter += 1
                if verbose:
                    print(f"Retrying {counter} times for {url}. Status code: {response.status_code}")
                time.sleep(timeout)
            if counter >= max_iter:

                print(f"Max iterations reached for {url}")
                return None
        except Exception as e:
            if verbose:
                print(f"An error occurred: {e}")
            counter += 1


if __name__ == "__main__":
    print("Original:", get_my_ip())
    for proxy in PROXIES:
        proxy_dict = {"http": proxy, "https": proxy}
        print(f"Using {proxy}, IP:", get_my_ip(proxies=proxy_dict))
