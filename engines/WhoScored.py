from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import time
from IPython.display import clear_output
from .request_utils import get_proxy, HEADERS
import json
import os
from tqdm import tqdm
import undetected_chromedriver as uc


class WhoScored:

    ############################################################################
    def __init__(self):
        # options = Options()
        # # whoscored scraper CANNOT be headless
        # options.add_argument("window-size=700,600")
        proxy = get_proxy()  # Use proxy
        # options.add_argument('--proxy-server="http={};https={}"'.format(proxy, proxy))
        # prefs = {"profile.managed_default_content_settings.images": 2}  # don't load images to make faster
        # options.add_experimental_option("prefs", prefs)
        # self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)  # create driver
        options = uc.ChromeOptions()
        print("Using proxy: {}".format(proxy))
        options.add_argument('--proxy-server="http={};https={}"'.format(proxy, proxy))
        options.add_argument(f"user-agent={HEADERS['user-agent']}")
        options.add_argument("--window-size=1200,1200")
        options.add_argument("--ignore-certificate-errors")
        self.driver = uc.Chrome(options=options)
        self.driver.reconnect()

        # test
        self.driver.get("http://httpbin.org/ip")
        print(self.driver.page_source)
        self.driver.reconnect()

        clear_output()

    ############################################################################
    def close(self):
        self.driver.close()
        self.driver.quit()

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
                self.driver.get(links[league])
                done = True
            except:
                self.close()
                self.__init__()
                time.sleep(5)
        print("League page status: {}".format(self.driver.execute_script("return document.readyState")))
        print(self.driver.page_source)
        # Wait for season dropdown to be accessible, then find the link to the chosen season
        for el in self.driver.find_elements(By.TAG_NAME, "select"):
            if el.get_attribute("id") == "seasons":
                for subel in el.find_elements(By.TAG_NAME, "option"):
                    print(subel.text)
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
                self.driver.get(season_link)
                done = True
            except:
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

        # Iterate through the stages
        for stage_url in stage_urls:
            self.driver.get(stage_url)

            print("{} status: {}".format(stage_url, self.driver.execute_script("return document.readyState")))

            done = False
            while True:
                initial = self.driver.page_source
                elements = self.driver.find_elements(By.TAG_NAME, "a")
                links += [el.get_attribute("href") for el in elements if "Live" in el.get_attribute("href") and "Matches" in el.get_attribute("href")]

                links = list(set(links))
                prev_week_button = self.driver.find_element(By.ID, "dayChangeBtn-prev")
                prev_week_button.click()
                time.sleep(1)
                if initial == self.driver.page_source:  # if the page didn't change, then we've reached the end
                    break
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
        for link in tqdm(match_data):
            i += 1
            try_count = 0
            while match_data[link] == "":
                try_count += 1
                if try_count > 10:
                    print("Failed to scrape match {}/{} from {}".format(i, len(match_data), link))
                    return -1
                try:
                    print("{}\rScraping match data for match {}/{} in the {}-{} {} season from {}".format(" " * 500, i, len(match_data), year - 1, year, league, link), end="\r")
                    match_data[link] = self.scrape_match(link)
                except:
                    print("\n\nError encountered. Saving output and restarting webdriver.")
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
        self.driver.get(link)
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
