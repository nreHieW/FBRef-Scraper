import requests
from bs4 import BeautifulSoup
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import urllib3

# Disable SSL warnings for proxy testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"}


def get_ip(proxy: dict = None):
    """Get the current IP address without using any proxy"""
    try:
        response = requests.get("https://api.ipify.org?format=json", proxies=proxy, headers=HEADERS, timeout=10)
        return response.json()["ip"]
    except Exception as e:
        return None


def test_proxy(proxy: dict, original_ip: str):
    """Test if a proxy is working"""
    try:
        proxy_ip = get_ip(proxy)
        if proxy_ip and original_ip != proxy_ip:
            return proxy_ip
        return None
    except Exception as e:
        print(f"Failed to test proxy: {e}")
        return None


def setup_proxies():

    response = requests.get("https://www.sslproxies.org/", headers=HEADERS)

    proxies = []
    soup = BeautifulSoup(response.text, "html.parser")
    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) == 0:
            continue
        proxies.append({"ip": tds[0].string, "port": tds[1].string})
    proxies = [x for x in proxies if x["ip"] and x["port"]]

    proxy_urls = [
        f"{proxy['ip']}:{proxy['port']}"
        for proxy in proxies
        if proxy["ip"]
        and proxy["port"]
        and "-" not in f"{proxy['ip']}:{proxy['port']}"
        and len(f"{proxy['ip']}:{proxy['port']}".split(":")) == 2
        and len(f"{proxy['ip']}:{proxy['port']}".split(".")) == 4
    ]
    response = requests.get("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt")
    proxy_urls += response.text.split("\n")

    response = requests.get("https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt")
    tmp = response.text.split("\n")
    proxy_urls += tmp

    proxy_urls = list(set([x for x in proxy_urls if x]))
    random.shuffle(proxy_urls)
    proxy_urls = [{"http": f"http://{proxy}", "https": f"http://{proxy}"} for proxy in proxy_urls][:10000]
    # original_ip = get_ip()
    # print(f"Original IP: {original_ip}")

    # working_proxies = []
    # with ThreadPoolExecutor(max_workers=100) as executor:
    #     futures = [executor.submit(test_proxy, proxy, original_ip) for proxy in proxy_urls]
    #     for future in tqdm(as_completed(futures), total=len(futures), desc="Testing proxies"):
    #         if future.result():
    #             working_proxies.append(proxy_urls[futures.index(future)])
    # proxy_urls = working_proxies
    # print(f"Found {len(proxy_urls)} working proxies")
    return proxy_urls


PROXIES = setup_proxies()


def get_proxies():
    return random.sample(PROXIES, len(PROXIES))


if __name__ == "__main__":
    print(setup_proxies())
