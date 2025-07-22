import requests
from bs4 import BeautifulSoup
import random
import concurrent.futures
from tqdm import tqdm
import os
import urllib3

# Disable SSL warnings for proxy testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"}


def get_original_ip():
    """Get the current IP address without using any proxy"""
    try:
        response = requests.get("https://api.ipify.org?format=json", headers=HEADERS, timeout=10)
        return response.json()["ip"]
    except Exception as e:
        print(f"Failed to get original IP: {e}")
        return None


def test_proxy(proxy_url, original_ip=None, verbose=True):
    """Test if a proxy is working by checking if it changes our IP address"""
    if original_ip is None:
        original_ip = get_original_ip()
        if original_ip is None:
            return None

    # Set up proxy for both HTTP and HTTPS
    proxy_dict = {"http": f"http://{proxy_url}", "https": f"http://{proxy_url}"}

    # Test HTTP first (more reliable with basic proxies)
    for protocol in ["http", "https"]:
        try:
            # For HTTPS through proxies, disable SSL verification (many proxies have SSL issues)
            # For HTTP, verification parameter is ignored anyway
            verify_ssl = False if protocol == "https" else True

            response = requests.get(f"{protocol}://api.ipify.org?format=json", proxies=proxy_dict, headers=HEADERS, timeout=10, verify=verify_ssl)

            if response.status_code == 200:
                ip = response.json()["ip"]
                if ip and ip != original_ip:
                    if verbose:
                        print(f"✓ [{protocol}] Proxy {proxy_url} working: {original_ip} -> {ip}")
                    return proxy_url
                else:
                    if verbose:
                        print(f"✗ [{protocol}] Proxy {proxy_url} not changing IP: still {ip}")
            else:
                if verbose:
                    print(f"✗ [{protocol}] Proxy {proxy_url} returned status {response.status_code}")

        except requests.exceptions.SSLError as ssl_err:
            if protocol == "https":
                # For HTTPS SSL errors, try again with verify=False to see if proxy still works
                try:
                    response = requests.get(f"{protocol}://api.ipify.org?format=json", proxies=proxy_dict, headers=HEADERS, timeout=10, verify=False)  # Force disable SSL verification
                    if response.status_code == 200:
                        ip = response.json()["ip"]
                        if ip and ip != original_ip:
                            if verbose:
                                print(f"✓ [{protocol}] Proxy {proxy_url} working (SSL verification disabled): {original_ip} -> {ip}")
                            return proxy_url
                except Exception:
                    pass  # If this also fails, continue to next protocol

            if verbose:
                print(f"✗ [{protocol}] Proxy {proxy_url} SSL error: {ssl_err}")
            continue
        except Exception as e:
            if verbose:
                print(f"✗ [{protocol}] Proxy {proxy_url} error: {e}")
            continue

    return None


def setup_proxies():
    # First, get our original IP
    original_ip = get_original_ip()
    if original_ip is None:
        print("Failed to get original IP address. Cannot test proxies.")
        return []

    print(f"Original IP: {original_ip}")

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

    print(f"Found {len(proxy_urls)} proxies, testing a sample of 100")
    proxy_urls = random.sample(proxy_urls, min(100, len(proxy_urls)))

    # Test proxies with the original IP
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        valid_proxies = list(tqdm(executor.map(lambda url: test_proxy(url, original_ip), proxy_urls), total=len(proxy_urls), leave=False))

    # Filter out None values
    valid_proxies = [proxy for proxy in valid_proxies if proxy]
    print(f"Found {len(valid_proxies)} working proxies")
    random.shuffle(valid_proxies)
    return valid_proxies


if __name__ == "__main__":
    print(setup_proxies())
