"""Edited version of sfc.FBRref.py"""

import numpy as np
import pandas as pd
from ScraperFC.shared_functions import xpath_soup, sources
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from bs4 import BeautifulSoup
import time
import re
import os
from datetime import datetime
from .request_utils import get_request
from tqdm import tqdm

MAX_WORKERS = 20
LINKS_CACHE_FPATH = "data/cache/fbref_links.txt"


class FBRef:
    """ScraperFC module for FBRef"""

    ####################################################################################################################
    def __init__(self):
        self.wait_time = 6  # in seconds, as of 30-Oct-2022 FBRef blocks if requesting more than 20 requests/minute

        options = Options()
        options.headless = True
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) " + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 " + "Safari/537.36")
        other_options = [
            "--headless",
            "--disable-gpu",
            "--window-size=1920,1200",
            "--ignore-certificate-errors",
            "--disable-extensions",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ]
        for o in other_options:
            options.add_argument(o)

        options.add_argument("--incognito")
        prefs = {"profile.managed_default_content_settings.images": 2}  # don't load images
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--log-level=3")
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        self.options = options
        self.driver.set_page_load_timeout(120000)

        self.stats_categories = {
            "standard": {
                "url": "stats",
                "html": "standard",
            },
            "goalkeeping": {
                "url": "keepers",
                "html": "keeper",
            },
            "advanced goalkeeping": {
                "url": "keepersadv",
                "html": "keeper_adv",
            },
            "shooting": {
                "url": "shooting",
                "html": "shooting",
            },
            "passing": {
                "url": "passing",
                "html": "passing",
            },
            "pass types": {
                "url": "passing_types",
                "html": "passing_types",
            },
            "goal and shot creation": {
                "url": "gca",
                "html": "gca",
            },
            "defensive": {
                "url": "defense",
                "html": "defense",
            },
            "possession": {
                "url": "possession",
                "html": "possession",
            },
            "playing time": {
                "url": "playingtime",
                "html": "playing_time",
            },
            "misc": {
                "url": "misc",
                "html": "misc",
            },
        }

    ####################################################################################################################
    def close(self):
        """Closes and quits the Selenium WebDriver instance."""
        self.driver.close()
        self.driver.quit()

    ####################################################################################################################
    def get(self, url):
        """ Custom get function just for the FBRef module. 
        
        Calls .get() from the Selenium WebDriver and then waits in order to\
        avoid a Too Many Requests HTTPError from FBRef. 
        
        Args
        ----
        url : str
            The URL to get
        Returns
        -------
        None
        """
        try:
            self.driver.get(url)
        except Exception as E:
            self.driver.close()
            self.driver.quit()
            self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=self.options)
            self.driver.set_page_load_timeout(120000)
            self.get(url)
        time.sleep(self.wait_time)

    ####################################################################################################################
    def requests_get(self, url):
        """Custom requests.get function for the FBRef module

        Calls requests.get() until the status code is 200.

        Args
        ----
        url : Str
            The URL to get
        Returns
        -------
        : requests.Response
            The response
        """
        response = get_request(url, timeout=self.wait_time)
        return response

    ####################################################################################################################
    def get_season_link(self, year, league):
        """ Returns the URL for the chosen league season.

        Args
        ----
        year : int
            Calendar year that the season ends in (e.g. 2023 for the 2022/23\
            season)
        league : str
            League. Look in shared_functions.py for the available leagues for\
            each module.
        Returns
        -------
        : str
            URL to the FBRef page of the chosen league season 
        """

        url = sources["FBRef"][league]["url"]
        finder = sources["FBRef"][league]["finder"]

        # go to the league's history page
        response = self.requests_get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        calendar_years = [
            str(year - 1) + "-" + str(year),
            str(year),
        ]  # list of 1- and 2-calendar years strings to work for any competition

        # Get url to season
        for tag in soup.find_all("th", {"data-stat": ["year", "year_id"]}):
            finder_found = np.any([f in tag.find("a")["href"] for f in finder if tag.find("a")])  # bool, if any finders are found in tag
            season_found = np.any([tag.getText() == s for s in calendar_years])  # bool, if 1- or 2-calendar years are found in tag
            if tag.find("a") and finder_found and season_found:
                return "https://fbref.com" + tag.find("a")["href"]

        print(f"No {league} {year} season is available on FBRef.")
        return -1  # if season URL is not found

    ####################################################################################################################
    def get_match_links(self, year, league):
        """ Gets all match links for the chosen league season.

        Args
        ----
        year : int
            Calendar year that the season ends in (e.g. 2023 for the 2022/23\
            season)
        league : str
            League. Look in shared_functions.py for the available leagues for\
            each module.
        Returns
        -------
        : list
            FBRef links to all matches for the chosen league season
        """

        season_link = self.get_season_link(year, league)
        if season_link == -1:
            return None

        # go to the scores and fixtures page
        split = season_link.split("/")
        first_half = "/".join(split[:-1])
        second_half = split[-1].split("-")
        second_half = "-".join(second_half[:-1]) + "-Score-and-Fixtures"
        fixtures_url = first_half + "/schedule/" + second_half
        response = self.requests_get(fixtures_url)
        soup = BeautifulSoup(response.content, "html.parser")

        # check if there are any scores elements with links. if not, no match links are present
        scores_links = [t.find(href=True) for t in soup.find_all("td", {"data-stat": "score"}) if t.find(href=True)]
        if len(scores_links) == 0:
            print(f"No match score elements with links found at {fixtures_url} for {league} {year}.")
            return None

        # find all of the match links from the scores and fixtures page that have the sources finder
        finders = sources["FBRef"][league]["finder"]
        match_links = ["https://fbref.com" + t["href"] for t in scores_links if t and np.any([f in t["href"] for f in finders])]

        match_links = list(set(match_links))
        if os.path.exists(LINKS_CACHE_FPATH):
            with open(LINKS_CACHE_FPATH, "r") as f:
                cached_links = f.read().split("\n")
            match_links = [x for x in match_links if x not in cached_links]

        return match_links

    def scrape_stats(self, year, league, stat_category, normalize=False):
        """ Scrapes a single stats category
        
        Adds team and player ID columns to the stats tables
        
        Args
        ----
        year : int
            Calendar year that the season ends in (e.g. 2023 for the 2022/23\
            season)
        league : str
            League. Look in shared_functions.py for the available leagues for\
            each module.
        stat_cateogry : str
            The stat category to scrape.
        normalize : bool
            OPTIONAL, default is False. If True, will normalize all stats to Per90.
        Returns
        -------
        : tuple
            tuple of 3 Pandas DataFrames, (squad_stats, opponent_stats,\
            player_stats).
        """

        # Verify valid stat category
        if stat_category not in self.stats_categories.keys():
            raise Exception(f'"{stat_category}" is not a valid FBRef stats category. ' + f"Must be one of {list(self.stats_categories.keys())}.")

        # Get URL to stat category
        season_url = self.get_season_link(year, league)
        old_suffix = season_url.split("/")[-1]
        new_suffix = f'{self.stats_categories[stat_category]["url"]}/{old_suffix}'
        new_url = season_url.replace(old_suffix, new_suffix)

        self.get(new_url)  # webdrive to link
        soup = BeautifulSoup(self.driver.page_source, "html.parser")  # get initial soup

        # Normalize button, if requested
        if normalize:
            # click all per90 toggles on the page
            per90_toggles = soup.find_all("button", {"id": re.compile("per_match_toggle")})
            for toggle in per90_toggles:
                xpath = xpath_soup(toggle)
                button_el = self.driver.find_element(By.XPATH, xpath)
                self.driver.execute_script("arguments[0].click()", button_el)
            # update the soup
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

        # Gather stats table tags
        squad_stats_tag = soup.find("table", {"id": re.compile("for")})
        opponent_stats_tag = soup.find("table", {"id": re.compile("against")})
        player_stats_tag = soup.find(
            "table",
            {"id": re.compile(f'stats_{self.stats_categories[stat_category]["html"]}')},
        )

        # Get stats dataframes
        squad_stats = pd.read_html(str(squad_stats_tag))[0] if squad_stats_tag is not None else None
        opponent_stats = pd.read_html(str(opponent_stats_tag))[0] if opponent_stats_tag is not None else None
        player_stats = pd.read_html(str(player_stats_tag))[0] if player_stats_tag is not None else None

        # Drop rows that contain duplicated table headers
        squad_stats = squad_stats[(~squad_stats[("Unnamed: 0_level_0", "Squad")].isna()) & (squad_stats[("Unnamed: 0_level_0", "Squad")] != "Squad")].reset_index(drop=True)
        opponent_stats = opponent_stats[(~opponent_stats[("Unnamed: 0_level_0", "Squad")].isna()) & (opponent_stats[("Unnamed: 0_level_0", "Squad")] != "Squad")].reset_index(drop=True)
        player_stats = player_stats[player_stats[("Unnamed: 0_level_0", "Rk")] != "Rk"].reset_index(drop=True)

        # Add team ID's
        if squad_stats is not None:
            squad_stats["Team ID"] = [tag.find("a")["href"].split("/")[3] for tag in squad_stats_tag.find_all("th", {"data-stat": "team"})[1:] if tag and tag.find("a")]
        if opponent_stats is not None:
            opponent_stats["Team ID"] = [tag.find("a")["href"].split("/")[3] for tag in opponent_stats_tag.find_all("th", {"data-stat": "team"})[1:] if tag and tag.find("a")]

        # Add player links and ID's
        if player_stats is not None:
            player_links = ["https://fbref.com" + tag.find("a")["href"] for tag in player_stats_tag.find_all("td", {"data-stat": "player"}) if tag and tag.find("a")]
            player_stats["Player Link"] = player_links
            player_stats["Player ID"] = [l.split("/")[-2] for l in player_links]

        return squad_stats, opponent_stats, player_stats

    ####################################################################################################################
    def scrape_all_stats(self, year, league, normalize=False):
        """ Scrapes all stat categories
        
        Runs scrape_stats() for each stats category on dumps the returned tuple\
        of dataframes into a dict.
        
        Args
        ----
        year : int
            Calendar year that the season ends in (e.g. 2023 for the 2022/23\
            season)
        league : str
            League. Look in shared_functions.py for the available leagues for\
            each module.
        normalize : bool
            OPTIONAL, default is False. If True, will normalize all stats to Per90.
        Returns
        -------
        : dict
            Keys are stat category names, values are tuples of 3 dataframes,\
            (squad_stats, opponent_stats, player_stats)
        """

        return_package = dict()
        for stat_category in self.stats_categories:
            stats = self.scrape_stats(year, league, stat_category, normalize)
            return_package[stat_category] = stats

        return return_package

    ####################################################################################################################
    def scrape_matches(self, year, league):
        """ Scrapes the FBRef standard stats page of the chosen league season.
            
        Works by gathering all of the match URL's from the homepage of the\
        chosen league season on FBRef and then calling scrape_match() on each one.

        Args
        ----
        year : int
            Calendar year that the season ends in (e.g. 2023 for the 2022/23\
            season)
        league : str
            League. Look in shared_functions.py for the available leagues for\
            each module.
        Returns
        -------
        : Pandas DataFrame
            If save is False, will return the Pandas DataFrame with the the stats. 
        """
        season = str(year - 1) + "-" + str(year)
        links = self.get_match_links(year, league)
        matches = pd.DataFrame()  # initialize df

        # scrape match data
        print(f"Scraping {len(links)} matches for {league} {season}.")
        time.sleep(self.wait_time)

        def fetch_and_concat(link):
            try:
                match = self.scrape_match(link)
                with open(LINKS_CACHE_FPATH, "a") as f:
                    f.write(link + "\n")
                return match
            except Exception as E:
                print(f"Failed scraping match {link}: {E}")
                return pd.DataFrame()  # Return an empty DataFrame on failure

        # with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        #     futures = {executor.submit(fetch_and_concat, link): link for link in links[:50]}

        #     with tqdm(total=len(futures), desc=f"Scraping {league} {season} matches") as pbar:
        #         for future in concurrent.futures.as_completed(futures):
        #             try:
        #                 match = future.result()
        #                 matches = pd.concat([matches, match], ignore_index=True)
        #             except Exception as E:
        #                 print(f"Error in future for link {futures[future]}: {E}")
        #             finally:
        #                 pbar.update(1)
        with tqdm(total=len(links), desc=f"Scraping {league} {season} matches") as pbar:
            for link in links:
                try:
                    match = fetch_and_concat(link)
                    matches = pd.concat([matches, match], ignore_index=True)
                except Exception as E:
                    print(f"Error scraping link {link}: {E}")
                finally:
                    pbar.update(1)
                    time.sleep(self.wait_time)

        # sort df by match date
        if matches.shape[0] > 0:
            matches = matches.sort_values(by="Date").reset_index(drop=True)

        return matches

    ####################################################################################################################
    def scrape_match(self, link):
        """ Scrapes an FBRef match page.
        
        Args
        ----
        link : str
            URL to the FBRef match page
        Returns
        -------
        : Pandas DataFrame
            DataFrame containing most parts of the match page if they're\
            available (e.g. formations, lineups, scores, player stats, etc.).\
            The fields that are available vary by competition and year.
        """
        response = self.requests_get(link)
        soup = BeautifulSoup(response.content, "html.parser")

        # Matchweek/stage ==============================================================================================
        stage_el = list(soup.find("a", {"href": re.compile("-Stats")}, string=True).parents)[0]
        stage_text = stage_el.getText().split("(")[-1].split(")")[0].strip()
        if "matchweek" in stage_text:
            stage = int(stage_text.lower().replace("matchweek", "").strip())
        else:
            stage = stage_text

        # Team names and ids ===========================================================================================
        team_els = [el.find("a") for el in soup.find("div", {"class": "scorebox"}).find_all("strong") if el.find("a", href=True) is not None][:2]
        home_team_name = team_els[0].getText()
        home_team_id = team_els[0]["href"].split("/")[3]
        away_team_name = team_els[1].getText()
        away_team_id = team_els[1]["href"].split("/")[3]

        # Scores =======================================================================================================
        scores = soup.find("div", {"class": "scorebox"}).find_all("div", {"class": "score"})

        # Formations ===================================================================================================
        lineup_tags = [tag.find("table") for tag in soup.find_all("div", {"class": "lineup"})]

        # Player stats =================================================================================================
        # Use table ID's to find the appropriate table. More flexible than xpath
        player_stats = dict()
        for i, (team, team_id) in enumerate([("Home", home_team_id), ("Away", away_team_id)]):
            summary_tag = soup.find_all("table", {"id": re.compile(f"stats_{team_id}_summary")})
            assert len(summary_tag) < 2
            summary_df = pd.read_html(str(summary_tag[0]))[0] if len(summary_tag) == 1 else None

            gk_tag = soup.find_all("table", {"id": re.compile(f"keeper_stats_{team_id}")})
            assert len(gk_tag) < 2
            gk_df = pd.read_html(str(gk_tag[0]))[0] if len(gk_tag) == 1 else None

            passing_tag = soup.find_all("table", {"id": re.compile(f"stats_{team_id}_passing$")})
            assert len(passing_tag) < 2
            passing_df = pd.read_html(str(passing_tag[0]))[0] if len(passing_tag) == 1 else None

            pass_types_tag = soup.find_all("table", {"id": re.compile(f"stats_{team_id}_passing_types")})
            assert len(pass_types_tag) < 2
            pass_types_df = pd.read_html(str(pass_types_tag[0]))[0] if len(pass_types_tag) == 1 else None

            defense_tag = soup.find_all("table", {"id": re.compile(f"stats_{team_id}_defense")})
            assert len(defense_tag) < 2
            defense_df = pd.read_html(str(defense_tag[0]))[0] if len(defense_tag) == 1 else None

            possession_tag = soup.find_all("table", {"id": re.compile(f"stats_{team_id}_possession")})
            assert len(possession_tag) < 2
            possession_df = pd.read_html(str(possession_tag[0]))[0] if len(possession_tag) == 1 else None

            misc_tag = soup.find_all("table", {"id": re.compile(f"stats_{team_id}_misc")})
            assert len(misc_tag) < 2
            misc_df = pd.read_html(str(misc_tag[0]))[0] if len(misc_tag) == 1 else None

            lineup_df = pd.read_html(str(lineup_tags[i]))[0] if len(lineup_tags) != 0 else None

            # Field player ID's for the stats tables -------------------------------------------------------------------
            # Note: if a coach gets a yellow/red card, they appear in the player stats tables, in their own row, at the
            # bottom.
            if summary_df is not None:
                player_ids = list()
                # Iterate across all els that are player/coach names in the summary stats table
                for tag in summary_tag[0].find_all("th", {"data-stat": "player", "scope": "row", "class": "left"}):
                    if tag.find("a"):
                        # if th el has an a subel, it should contain an href link to the player
                        player_id = tag.find("a")["href"].split("/")[3]
                    else:
                        # coaches and the summary row have now a subel (and no player id)
                        player_id = ""
                    player_ids.append(player_id)

                summary_df["Player ID"] = player_ids
                if passing_df is not None:
                    passing_df["Player ID"] = player_ids
                if pass_types_df is not None:
                    pass_types_df["Player ID"] = player_ids
                if defense_df is not None:
                    defense_df["Player ID"] = player_ids
                if possession_df is not None:
                    possession_df["Player ID"] = player_ids
                if misc_df is not None:
                    misc_df["Player ID"] = player_ids

            # GK ID's --------------------------------------------------------------------------------------------------
            if gk_df is not None:
                gk_ids = [tag.find("a")["href"].split("/")[3] for tag in gk_tag[0].find_all("th", {"data-stat": "player"}) if tag.find("a")]

                gk_df["Player ID"] = gk_ids

            # Build player stats dict ----------------------------------------------------------------------------------
            # This will be turned into a Series and then put into the match dataframe
            player_stats[team] = {
                "Team Sheet": lineup_df,
                "Summary": summary_df,
                "GK": gk_df,
                "Passing": passing_df,
                "Pass Types": pass_types_df,
                "Defense": defense_df,
                "Possession": possession_df,
                "Misc": misc_df,
            }

        # Shots ========================================================================================================
        both_shots = soup.find_all("table", {"id": "shots_all"})
        if len(both_shots) == 1:
            both_shots = pd.read_html(str(both_shots[0]))[0]
            both_shots = both_shots[~both_shots.isna().all(axis=1)]
        else:
            both_shots = None
        home_shots = soup.find_all("table", {"id": f"shots_{home_team_id}"})
        if len(home_shots) == 1:
            home_shots = pd.read_html(str(home_shots[0]))[0]
            home_shots = home_shots[~home_shots.isna().all(axis=1)]
        else:
            home_shots = None
        away_shots = soup.find_all("table", {"id": f"shots_{away_team_id}"})
        if len(away_shots) == 1:
            away_shots = pd.read_html(str(away_shots[0]))[0]
            away_shots = away_shots[~away_shots.isna().all(axis=1)]
        else:
            away_shots = None

        # Expected stats flag ==========================================================================================
        expected = "Expected" in player_stats["Home"]["Summary"].columns.get_level_values(0)

        # Build match series ===========================================================================================
        match = pd.Series(dtype=object)
        match["Link"] = link
        match["Date"] = datetime.strptime(
            str(soup.find("h1")).split("<br/>")[0].split("–")[-1].replace("</h1>", "").split("(")[0].strip(),  # not a normal dash
            "%A %B %d, %Y",
        ).date()
        match["Stage"] = stage
        match["Home Team"] = home_team_name
        match["Away Team"] = away_team_name
        match["Home Team ID"] = home_team_id
        match["Away Team ID"] = away_team_id
        match["Home Formation"] = player_stats["Home"]["Team Sheet"].columns[0].split("(")[-1].replace(")", "").strip() if player_stats["Home"]["Team Sheet"] is not None else None
        match["Away Formation"] = player_stats["Away"]["Team Sheet"].columns[0].split("(")[-1].replace(")", "").strip() if player_stats["Away"]["Team Sheet"] is not None else None
        match["Home Goals"] = int(scores[0].getText()) if scores[0].getText().isdecimal() else None
        match["Away Goals"] = int(scores[1].getText()) if scores[1].getText().isdecimal() else None
        match["Home Ast"] = player_stats["Home"]["Summary"][("Performance", "Ast")].values[-1]
        match["Away Ast"] = player_stats["Away"]["Summary"][("Performance", "Ast")].values[-1]
        match["Home xG"] = player_stats["Home"]["Summary"][("Expected", "xG")].values[-1] if expected else None
        match["Away xG"] = player_stats["Away"]["Summary"][("Expected", "xG")].values[-1] if expected else None
        match["Home npxG"] = player_stats["Home"]["Summary"][("Expected", "npxG")].values[-1] if expected else None
        match["Away npxG"] = player_stats["Away"]["Summary"][("Expected", "npxG")].values[-1] if expected else None
        match["Home xAG"] = player_stats["Home"]["Summary"][("Expected", "xAG")].values[-1] if expected else None
        match["Away xAG"] = player_stats["Away"]["Summary"][("Expected", "xAG")].values[-1] if expected else None
        match["Home Player Stats"] = pd.Series(player_stats["Home"]).to_frame()
        match["Away Player Stats"] = pd.Series(player_stats["Away"]).to_frame()
        match["Shots"] = pd.Series(
            {
                "Both": both_shots,
                "Home": home_shots,
                "Away": away_shots,
            }
        )

        match = match.to_frame().T  # series to dataframe

        return match
