from selenium import webdriver
import selenium.common.exceptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
import time
from bs4 import BeautifulSoup

from .request_utils import get_proxy, HEADERS, get_request
from utils import get_system_usage
import json
import os
from tqdm import tqdm
import re


HEADERS = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"}


class WhoScored:

    ############################################################################
    def __init__(self):
        # # whoscored scraper CANNOT be headless
        # options.add_argument("window-size=700,600")
        proxy = get_proxy()  # Use proxy
        # proxy = {"http": "185.222.115.104:31280", "https": "185.222.115.104:31280"}
        print("Using proxy: {}".format(proxy))
        proxy = proxy["https"]
        ip, port = proxy.split(":")
        options = FirefoxOptions()
        options.set_preference("network.proxy.type", 1)
        options.set_preference("network.proxy.http", ip)
        options.set_preference("network.proxy.http_port", int(port))
        options.set_preference("network.proxy.ssl", ip)
        options.set_preference("network.proxy.ssl_port", int(port))
        options.set_preference("general.useragent.override", HEADERS["user-agent"])
        options.set_preference("permissions.default.stylesheet", 2)
        options.set_preference("permissions.default.image", 2)
        options.page_load_strategy = "eager"
        options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", "false")
        try:
            self.driver = webdriver.Firefox(options=options)
        except Exception as e:
            print("Error starting webdriver. Trying again.")
            proxy = get_proxy()  # Use proxy
            # proxy = {"http": "185.222.115.104:31280", "https": "185.222.115.104:31280"}
            proxy = proxy["https"]
            ip, port = proxy.split(":")
            options.set_preference("network.proxy.type", 1)
            options.set_preference("network.proxy.http", ip)
            options.set_preference("network.proxy.http_port", int(port))
            options.set_preference("network.proxy.ssl", ip)
            options.set_preference("network.proxy.ssl_port", int(port))

            self.driver = webdriver.Firefox(options=options)

    ############################################################################
    def close(self):
        self.driver.close()
        self.driver.quit()

    def get(self, link):
        try:
            self.driver.get(link)
            # Click the cookies button
            self.click_cookie_button()

            # Check ram usage
            system_usage = get_system_usage()
            ram_amt_free = system_usage["ram"]["free"]  # in GB
            if ram_amt_free < 0.75:
                print(f"RAM free is {ram_amt_free}. Restarting webdriver.")
                self.close()
                self.__init__()
        except selenium.common.exceptions.TimeoutException:
            print("Timeout exception. Reinitializing webdriver.")
            self.close()
            self.__init__()

    def click_cookie_button(self):
        cookies_button = self.driver.find_elements(By.XPATH, "/html/body/div[1]/div/div/div/div[2]/div/button[2]")
        if cookies_button:
            self.click_button(cookies_button[0])

    def click_button(self, button_item: WebElement):
        try:
            button_item.click()
        except selenium.common.exceptions.ElementClickInterceptedException as e:
            print("ElementClickInterceptedException. Trying to click again.")
            re_match = re.search(r'obscures it.?<.+?id="([^"]+)"', str(e))
            if re_match:
                obstructing_element_id = re_match.group(1)
                obstructing_element = self.driver.find_element(By.ID, obstructing_element_id)
                self.driver.execute_script("arguments[0].style.visibility='hidden';", obstructing_element)
                button_item.click()
            else:
                self.driver.refresh()
                time.sleep(3)
                self.click_cookie_button()
                button_item.click()

    ############################################################################
    def get_season_link(self, year, league):

        links = {
            "EPL": "https://www.whoscored.com/Regions/252/Tournaments/2/England-Premier-League",
            "La Liga": "https://www.whoscored.com/Regions/206/Tournaments/4/Spain-LaLiga",
            "Bundesliga": "https://www.whoscored.com/Regions/81/Tournaments/3/Germany-Bundesliga",
            "Serie A": "https://www.whoscored.com/Regions/108/Tournaments/5/Italy-Serie-A",
            "Ligue 1": "https://www.whoscored.com/Regions/74/Tournaments/22/France-Ligue-1",
            "Argentina Liga Profesional": "https://www.whoscored.com/Regions/11/Tournaments/68/Argentina-Liga-Profesional",
            "EFL Championship": "https://www.whoscored.com/Regions/252/Tournaments/7/England-Championship",
            "EFL1": "https://www.whoscored.com/Regions/252/Tournaments/8/England-League-One",
            "EFL2": "https://www.whoscored.com/Regions/252/Tournaments/9/England-League-Two",
            # Edd Webster added these leagues (twitter: https://twitter.com/eddwebster)
            "Liga Nos": "https://www.whoscored.com/Regions/177/Tournaments/21/Portugal-Liga-NOS",
            "Eredivisie": "https://www.whoscored.com/Regions/155/Tournaments/13/Netherlands-Eredivisie",
            "Russian Premier League": "https://www.whoscored.com/Regions/182/Tournaments/77/Russia-Premier-League",
            "Brasileirao": "https://www.whoscored.com/Regions/31/Tournaments/95/Brazil-Brasileir%C3%A3o",
            "MLS": "https://www.whoscored.com/Regions/233/Tournaments/85/USA-Major-League-Soccer",
            "Super Lig": "https://www.whoscored.com/Regions/225/Tournaments/17/Turkey-Super-Lig",
            "Jupiler Pro League": "https://www.whoscored.com/Regions/22/Tournaments/18/Belgium-Jupiler-Pro-League",
            "Bundesliga II": "https://www.whoscored.com/Regions/81/Tournaments/6/Germany-Bundesliga-II",
            "Champions League": "https://www.whoscored.com/Regions/250/Tournaments/12/Europe-Champions-League",
            "Europa League": "https://www.whoscored.com/Regions/250/Tournaments/30/Europe-Europa-League",
            "FA Cup": "https://www.whoscored.com/Regions/252/Tournaments/29/England-League-Cup",
            "League Cup": "https://www.whoscored.com/Regions/252/Tournaments/29/England-League-Cup",
            "World Cup": "https://www.whoscored.com/Regions/247/Tournaments/36/International-FIFA-World-Cup",
            "European Championship": "https://www.whoscored.com/Regions/247/Tournaments/124/International-European-Championship",
            "AFCON": "https://www.whoscored.com/Regions/247/Tournaments/104/International-Africa-Cup-of-Nations",
            # End of Edd Webster leagues
        }

        if (league == "Argentina Liga Profesional" and year in [2016, 2021]) or league in ["Brasileirao", "MLS", "World Cup", "European Championship", "AFCON"]:
            year_str = str(year)
        else:
            year_str = "{}/{}".format(year - 1, year)

        # Repeatedly try to get the league's homepage
        done = False
        while not done:
            try:
                self.get(links[league])
                done = True
            except Exception as e:
                print(e)
                self.close()
                self.__init__()
                time.sleep(5)
        print("League page status: {}".format(self.driver.execute_script("return document.readyState")), "at", self.driver.title)

        # Wait for season dropdown to be accessible, then find the link to the chosen season
        for el in self.driver.find_elements(By.TAG_NAME, "select"):
            if el.get_attribute("id") == "seasons":
                for subel in el.find_elements(By.TAG_NAME, "option"):
                    if subel.text == year_str:
                        return "https://www.whoscored.com" + subel.get_attribute("value")
        return -1

    ############################################################################
    def get_match_links(self, year, league):

        # Go to season page
        season_link = self.get_season_link(year, league)
        if season_link == -1:
            print("Failed to get season link for {}-{} {}".format(year - 1, year, league))
            return -1

        # Repeatedly try to get to the season's homepage
        done = False
        while not done:
            try:
                self.get(season_link)
                done = True
            except Exception as e:
                print(e)
                self.close()
                self.__init__()
                time.sleep(5)
        print("Season page status: {}".format(self.driver.execute_script("return document.readyState")))

        links = []

        # Get the season stages and their URLs
        stage_dropdown_xpath = '//*[@id="stages"]'
        stage_elements = self.driver.find_elements(By.XPATH, "{}/{}".format(stage_dropdown_xpath, "option"))
        stage_urls = ["https://www.whoscored.com" + el.get_attribute("value") for el in stage_elements]
        if len(stage_urls) == 0:  # if no stages in dropdown, then the current url is the only stage
            stage_urls = [
                self.driver.current_url,
            ]
        print("Found {} stages".format(len(stage_urls)))
        # Iterate through the stages
        for stage_url in stage_urls:
            self.get(stage_url)

            print("{} status: {}".format(stage_url, self.driver.execute_script("return document.readyState")))

            done = False
            while True:
                initial = self.driver.page_source
                # there is a weird stale element exception that occurs here
                #     elements = self.driver.find_elements(By.TAG_NAME, "a")
                #     urls = [el.get_attribute("href") for el in elements]

                # Beautiful soup since we dont care for the elements, just the hrefs

                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                all_fixtures = soup.find_all(class_="Accordion-module_accordion__UuHD0")
                for dates in all_fixtures:
                    fixtures = dates.find_all(class_="Match-module_row__zwBOn")
                    for row in fixtures:
                        link_tag = row.find("a")
                        if link_tag and "Live" in link_tag.get("href"):
                            links.append("https://www.whoscored.com/" + link_tag["href"])

                links = list(set(links))
                # print(len(links), "matches found")
                prev_week_button = self.driver.find_element(By.ID, "dayChangeBtn-prev")
                self.click_cookie_button()  # This is to prevent some weird overlay thing from blocking the button
                self.click_button(prev_week_button)
                time.sleep(3)
                if initial == self.driver.page_source:  # if the page didn't change, then we've reached the end
                    break
        # print(list(set(links)))
        return list(set(links))

    def scrape_matches(self, year, league, path):

        # Read match links from file or get them with selenium
        save_filename = path
        if os.path.exists(save_filename):
            with open(save_filename, "r") as f:
                match_data = json.loads(f.read())
        else:
            match_data = self.get_match_links(year, league)
            if match_data == -1:
                return -1

        # Scrape match data for each link
        i = 0
        for link in tqdm(match_data, desc=f"Scraping {league} {year - 1}-{year} matches", total=len(match_data)):
            time.sleep(3)
            i += 1
            try_count = 0
            while match_data[link] == "":
                try_count += 1
                if try_count > 10:
                    print("Failed to scrape match {}/{} from {}".format(i, len(match_data), link))
                    return -1
                try:
                    # print("{}\rScraping match data for match {}/{} in the {}-{} {} season from {}".format(" " * 500, i, len(match_data), year - 1, year, league, link), end="\r")
                    match_data[link] = self.scrape_match(link)
                except Exception as e:
                    print("\n\nError encountered. Saving output and restarting webdriver.")
                    print(e)
                    with open(save_filename, "w") as f:
                        f.write(json.dumps(match_data))
                    self.close()
                    self.__init__()
                    time.sleep(5)

        # save output
        with open(save_filename, "w") as f:
            f.write(json.dumps(match_data))
        return match_data

    ############################################################################
    def scrape_match(self, link):
        self.get(link)
        scripts = list()

        for el in self.driver.find_elements(By.TAG_NAME, "script"):
            scripts.append(el.get_attribute("innerHTML"))

        for script in scripts:
            if 'require.config.params["args"]' in script:
                match_data_string = script

        match_data_string = (
            match_data_string.split(" = ")[1]
            .replace("matchId", '"matchId"')
            .replace("matchCentreData", '"matchCentreData"')
            .replace("matchCentreEventTypeJson", '"matchCentreEventTypeJson"')
            .replace("formationIdNameMappings", '"formationIdNameMappings"')
            .replace(";", "")
        )
        match_data = json.loads(match_data_string)

        return match_data
