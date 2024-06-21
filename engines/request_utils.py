import requests
import time
import concurrent.futures
import pandas as pd
import random
from bs4 import BeautifulSoup

HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"}


def setup_proxies(test_url="http://www.google.com", timeout=5):
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        valid_proxies = list(executor.map(lambda proxy_url: test_proxy(proxy_url, test_url, timeout), proxy_urls))

    # Filter out None values
    valid_proxies = [proxy for proxy in valid_proxies if proxy]
    valid_proxies = list(set(valid_proxies))
    return valid_proxies


def test_proxy(proxy_url, test_url, timeout=3):
    try:
        proxy_dict = {"http": proxy_url, "https": proxy_url}
        response = requests.get(test_url, headers=HEADERS, proxies=proxy_dict, timeout=timeout)
        if response.status_code == 200:
            return proxy_url
    except Exception as e:
        pass
    return None


PROXIES = setup_proxies()
print(f"Found {len(PROXIES)} valid proxies")


def get_proxy():
    idx = random.randint(0, len(PROXIES) - 1)
    return {"http": PROXIES[idx], "https": PROXIES[idx]}


def get_request(url: str, timeout: int = 5, max_iter: int = 100, verbose: bool = True) -> requests.Response:
    # url = url.replace("https://", "http://")
    counter = 0
    while True:
        try:
            response = requests.get(url, headers=HEADERS, proxies=None)
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
        except requests.exceptions.ProxyError as e:
            counter += 1


if __name__ == "__main__":

    def get_my_ip(proxies=None):
        try:
            response = requests.get("http://httpbin.org/ip", proxies=proxies)
            response.raise_for_status()  # Raise an exception for HTTP errors
            ip_info = response.json()
            return ip_info["origin"]
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    print("Original:", get_my_ip())
    for proxy in PROXIES:
        proxy_dict = {"http": proxy, "https": proxy}
        print(f"Using {proxy}, IP:", get_my_ip(proxies=proxy_dict))
