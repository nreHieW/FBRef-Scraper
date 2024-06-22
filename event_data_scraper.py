from engines.WhoScored import WhoScored
import json
import os
import argparse
import pandas as pd
from utils import write_to_bq, check_size, is_ubuntu


if is_ubuntu():  # github actions
    print("Running on Ubuntu")
    from pyvirtualdisplay import Display

    display = Display(visible=0, size=(800, 800))
    display.start()

LINKS_CACHE_FPATH = "data/cache/whoscored_links.txt"


## Utils
def load_season(path):  # load a json file, returns data and match_urls
    with open(path, "r") as f:
        data = json.load(f)
    data = dict(data)
    match_urls = list(data.keys())
    count = 0
    for item in data:  # error check
        if len(data.get(item)) != 4:
            count += 1
    if count != 0:
        print("Error at", path)
        exit()
    return data, match_urls


def season_mappings(data, match_urls):  # return mappings of  PLAYER_NAMES and TEAM_IDS given a season's data
    PLAYER_NAMES = pd.DataFrame()
    TEAM_IDS = pd.DataFrame()
    for url in match_urls:
        match = data.get(url)
        match_centre = match.get("matchCentreData")
        player_names = pd.DataFrame.from_dict(match_centre.get("playerIdNameDictionary"), orient="index")
        team_ids = pd.DataFrame.from_dict({match_centre["home"].get("teamId"): str(match_centre["home"].get("name")), match_centre["away"].get("teamId"): str(match_centre["away"].get("name"))}, orient="index")

        PLAYER_NAMES = pd.concat([PLAYER_NAMES, player_names])
        TEAM_IDS = pd.concat([TEAM_IDS, team_ids])
    PLAYER_NAMES.drop_duplicates(inplace=True)
    TEAM_IDS.drop_duplicates(inplace=True)
    PLAYER_NAMES.reset_index(inplace=True)
    TEAM_IDS.reset_index(inplace=True)
    PLAYER_NAMES.columns = ["id", "player"]
    TEAM_IDS.columns = ["id", "team"]
    PLAYER_NAMES["id"].astype("int")
    return PLAYER_NAMES, TEAM_IDS


def parse_match(match, QUAL_DICT):  # pass a single match and returns a processed dataframe events and the QUAL_DICT()
    ##### Helper Functions #####
    def change_event_types(lst):
        new = []
        for item in lst:
            item = event_type.get(item)
            new.append(item)
        new = " ,".join(item for item in new)
        return new

    def format_qualifiers(series):
        lst = []
        for row in series:
            keys = []
            values = []
            if row:
                for item in row:
                    keys.append(item.get("type").get("displayName"))
                    if item.get("value"):
                        values.append(item.get("value"))
                    else:
                        values.append(True)
                    tmp = dict(zip(keys, values))
                lst.append(tmp)
            else:
                lst.append([])

        return lst

    def change_outcome(series):
        outcomes = []
        for item in series:
            if item == "Successful" or item == " Successful":
                outcomes.append(True)
            else:
                outcomes.append(False)
        return outcomes

    def change_period(series):
        new = []
        for item in series:
            if item == "PreMatch":
                new.append(0)
            elif item == "FirstHalf":
                new.append(1)
            elif item == "SecondHalf":
                new.append(2)
            elif item == "PostGame":
                new.append(3)
            elif item == "FirstPeriodOfExtraTime":
                new.append(4)
            elif item == "SecondPeriodOfExtraTime":
                new.append(5)
            elif item == "PenaltyShootout":
                new.append(6)
        return new

    def change_zones(series):
        new = []
        for row in series:
            if type(row) == str:
                new.append(row[0])
            else:
                new.append(None)
        return new

    def process_qualifiers(dataframe):
        series = dataframe.loc[:, "qualifiers"]
        for i, row in enumerate(series):
            tmp = row
            if type(row) is dict:
                to_del = []
                exist = []
                for k, v in tmp.items():
                    if v != True and v != "0":
                        if k in dataframe.columns:
                            dataframe[k][i] = v
                        else:
                            dataframe[k] = pd.Series([], dtype="float64")
                            dataframe[k][i] = v
                        to_del.append(k)
                    else:
                        exist.append(k)
                for key in to_del:
                    del row[key]
                series[i] = exist
            else:
                row = None
        return dataframe

    def map_qual(series, qual):
        qual = {v: k for k, v in qual.items()}
        col = []
        for row in series:
            newrow = []
            for item in row:
                if item in qual:
                    newrow.append(qual.get(item))
                else:
                    qual[item] = int(len(qual)) + 1
                    newrow.append(int(len(qual)))
            col.append(newrow)
        return col, {v: k for k, v in qual.items()}  # invert back

    ##################

    match_id = match.get("matchId")
    event_type = {v: k for k, v in match.get("matchCentreEventTypeJson").items()}
    events = pd.DataFrame(match.get("matchCentreData")["events"])

    # processing starts here
    events["period"] = [x.get("displayName") for x in events["period"]]
    events["type"] = [x.get("displayName") for x in events["type"]]
    events["outcomeType"] = [x.get("displayName") for x in events["outcomeType"]]
    events["matchId"] = match_id
    events["satisfiedEventsTypes"].apply(change_event_types)
    events["outcomeType"] = change_outcome(events["outcomeType"])
    events["qualifiers"] = format_qualifiers(events["qualifiers"])
    events = process_qualifiers(events)
    events["period"] = change_period(events["period"])
    events.drop(columns=["cardType", "endX", "endY", "expandedMinute", "goalMouthZ", "goalMouthY", "blockedX", "blockedY", "id", "relatedEventId"], axis=1, inplace=True, errors="ignore")
    events.columns = [x[0].upper() + x[1:] for x in events.columns]
    events.dropna(how="all", axis=1, inplace=True)
    events.drop(columns=["JerseyNumber", "PlayerPosition", "FormationSlot", "TeamPlayerFormation", "InvolvedPlayers", "TeamFormation"], axis=1, inplace=True, errors="ignore")
    events["Qualifiers"], QUAL_DICT = map_qual(events["Qualifiers"], QUAL_DICT)
    events["Zone"] = change_zones(events["Zone"])
    events.drop("RelatedEventId", axis=1, inplace=True, errors="ignore")
    tochange = ["PlayerCaughtOffside", "Second", "OppositeRelatedEvent"]

    for name in events.columns:
        if "Id" in name:
            tochange.append(name)
    try:
        events[tochange] = events[tochange].astype("Int64", errors="ignore")
    except:
        tochange.remove("PlayerCaughtOffside")
        events[tochange] = events[tochange].astype("Int64", errors="ignore")
    return events, QUAL_DICT


def get_match_info(match, REF_DICT, STADIUMS):  # pass in a single match ,REF DICT,STADIUM DICT returns the matchid dataframe with formations and the mapped Referee and stadium Dict

    # schema is match_id time, attendance, venue, ref, home_id,home_info, away_id, away_info where info cols are dicts

    ###HELPER FUNCTIONS #######
    def get_formations(match, team):  # team is 'home' or 'away'
        start = []
        formation = []
        end = []
        match_formations = dict()
        for item in match.get("matchCentreData").get(team).get("formations"):
            formation.append(item.get("formationId"))
            start.append(item.get("startMinuteExpanded"))
            end.append(item.get("endMinuteExpanded"))
        match_formations = pd.DataFrame([formation, start, end]).T
        match_formations.columns = ["formation", "start", "end"]
        match_formations = match_formations.drop_duplicates(subset="formation", keep="last")[["formation", "end"]].set_index("formation").reset_index().to_dict(orient="records")
        return match_formations

    def map_ref(ref, ref_dict):
        ref_dict = {v: k for k, v in ref_dict.items()}
        if ref == "":
            return None, {v: k for k, v in ref_dict.items()}
        else:
            if ref in ref_dict:
                mapped_ref = ref_dict.get(ref)
            else:
                ref_dict[ref] = int(len(ref_dict)) + 1
                mapped_ref = int(len(ref_dict))

        return mapped_ref, {v: k for k, v in ref_dict.items()}  # invert back

    def map_stadiums(stadium, stadium_dict):
        stadium_dict = {v: k for k, v in stadium_dict.items()}
        if stadium == "":
            return None, {v: k for k, v in stadium_dict.items()}
        else:
            if stadium in stadium_dict:
                mapped_stadium = stadium_dict.get(stadium)
            else:
                stadium_dict[stadium] = int(len(stadium_dict)) + 1
                mapped_stadium = int(len(stadium_dict))

            return mapped_stadium, {v: k for k, v in stadium_dict.items()}

    ##############

    time = match.get("matchCentreData").get("timeStamp")
    attendance = match.get("matchCentreData").get("attendance")
    venue = match.get("matchCentreData").get("venueName")
    ref = match.get("matchCentreData").get("referee", dict()).get("name", "")  # returns empty if ref data is not avail
    ref, REF_DICT = map_ref(ref, REF_DICT)
    venue, STADIUMS = map_stadiums(venue, STADIUMS)
    home_id = match.get("matchCentreData").get("home").get("teamId")
    home_manager = match.get("matchCentreData")["home"].get("managerName")
    home_avg_age = match.get("matchCentreData")["home"].get("averageAge")
    away_manager = match.get("matchCentreData")["away"].get("managerName")
    away_id = match.get("matchCentreData").get("away").get("teamId")
    away_avg_age = match.get("matchCentreData")["away"].get("averageAge")
    match_id = match.get("matchId")
    home_formations = get_formations(match, "home")
    away_formations = get_formations(match, "away")
    home_info = {"manager": home_manager, "AvgAge": home_avg_age, "formation": home_formations}
    away_info = {"manager": away_manager, "AvgAge": away_avg_age, "formation": away_formations}
    if match.get("matchCentreData").get("htScore") == "":
        home_ht_score = 0
        away_ht_score = 0
    else:
        home_ht_score = match.get("matchCentreData").get("htScore").split(" : ")[0]
        away_ht_score = match.get("matchCentreData").get("htScore").split(" : ")[1]
    if match.get("matchCentreData").get("ftScore") == "":
        home_ft_score = 0
        away_ft_score = 0
    else:
        home_ft_score = match.get("matchCentreData").get("ftScore").split(" : ")[0]
        away_ft_score = match.get("matchCentreData").get("ftScore").split(" : ")[1]
    match_info = pd.DataFrame([match_id, time, attendance, venue, ref, home_id, home_info, away_id, away_info, home_ht_score, away_ht_score, home_ft_score, away_ft_score]).T
    match_info.columns = ["MatchId", "Time", "Attendance", "Venue", "Referee", "Home", "Home_Info", "Away", "Away_Info", "Home_HT_Score", "Away_HT_Score", "Home_FT_Score", "Away_FT_Score"]
    match_info[["MatchId", "Venue", "Referee"]] = match_info[["MatchId", "Venue", "Referee"]].astype("Int64", errors="ignore")
    match_info["Time"] = pd.to_datetime(match_info["Time"])
    match_info[["Home_HT_Score", "Away_HT_Score", "Home_FT_Score", "Away_FT_Score"]] = match_info[["Home_HT_Score", "Away_HT_Score", "Home_FT_Score", "Away_FT_Score"]].fillna(0).astype("int")
    return match_info, REF_DICT, STADIUMS


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape WhoScored Data")
    parser.add_argument("--start", type=int, help="Start year", default=2024)
    parser.add_argument("--end", type=int, help="End year", default=2024)
    parser.add_argument("--leagues", nargs="+", default=["Bundesliga", "La Liga", "Serie A", "Ligue 1", "EPL"])
    args = parser.parse_args()
    YEARS = list(range(args.start, args.end + 1))
    LEAGUES = args.leagues

    scraper = WhoScored()
    with open(LINKS_CACHE_FPATH, "r") as f:
        cached_urls = f.read().split()
    for league in LEAGUES:
        for year in YEARS:
            league_name = league.replace(" ", "_")
            filename = f"data/{league_name}_{year}_match_data.json"
            if not os.path.exists(filename):
                links = scraper.get_match_links(year, league)
                links = [x for x in links if x not in cached_urls]
                match_data = dict(zip(links, [""] * len(links)))

                with open(filename, "w") as f:
                    json.dump(match_data, f)
                # print(f"Found a total of {len(links)} new matches for", league, year)
            _ = scraper.scrape_matches(year, league, filename)
    scraper.close()

    # PARSING
    if os.path.exists("data/lookup/Qualifiers.json"):
        with open("data/lookup/Qualifiers.json", "r") as f:
            QUAL_DICT = json.load(f)
    else:
        QUAL_DICT = dict()

    if os.path.exists("data/lookup/Referees.json"):
        with open("data/lookup/Referees.json", "r") as f:
            REF_DICT = json.load(f)
    else:
        REF_DICT = dict()

    if os.path.exists("data/lookup/Stadiums.json"):
        with open("data/lookup/Stadiums.json", "r") as f:
            STADIUM_DICT = json.load(f)
    else:
        STADIUM_DICT = dict()

    if os.path.exists("data/lookup/Players.json"):
        ALL_PLAYERS = pd.read_json("data/lookup/Players.json", typ="series").reset_index()
        ALL_PLAYERS.columns = ["id", "player"]
    else:
        ALL_PLAYERS = pd.DataFrame()

    if os.path.exists("data/lookup/Teams.json"):
        ALL_TEAMS = pd.read_json("data/lookup/Teams.json", typ="series").reset_index()
        ALL_TEAMS.columns = ["id", "team"]
    else:
        ALL_TEAMS = pd.DataFrame()

    paths = []
    for league in LEAGUES:
        for year in YEARS:
            print("Parsing", league, year, "Match Data")
            path = f"data/{league}_{year}_match_data.json"
            paths.append(path)
            events = pd.DataFrame()

            data, match_urls = load_season(path)
            if len(match_urls) == 0:
                print("No Matches Found For", league, year)
                continue

            player_names, team_ids = season_mappings(data, match_urls)
            ALL_PLAYERS = pd.concat([ALL_PLAYERS, player_names])
            ALL_PLAYERS.drop_duplicates(inplace=True)
            ALL_PLAYERS.reset_index(drop=True, inplace=True)

            ALL_TEAMS = pd.concat([ALL_TEAMS, team_ids])
            ALL_TEAMS.drop_duplicates(inplace=True)
            ALL_TEAMS.reset_index(drop=True, inplace=True)

            all_matches = pd.DataFrame()
            # loop through all matches in the season, getting the events and info
            for url in match_urls:
                match = data.get(url)
                match_events, QUAL_DICT = parse_match(match, QUAL_DICT)
                events = pd.concat([events, match_events])
                match_info, REF_DICT, STADIUM_DICT = get_match_info(match, REF_DICT, STADIUM_DICT)
                all_matches = pd.concat([all_matches, match_info])

            all_matches.reset_index(drop=True, inplace=True)

            ## lines below needed as of 14/4
            events[[x for x in events.columns if "Is" in x]] = events[[x for x in events.columns if "Is" in x]].fillna("False").replace({"False": False, "True": True})
            events[["Length", "PassEndX", "PassEndY", "BlockedX", "GoalMouthZ", "BlockedY", "GoalMouthY", "Angle"]] = events[["Length", "PassEndX", "PassEndY", "BlockedX", "GoalMouthZ", "BlockedY", "GoalMouthY", "Angle"]].astype("float64")
            events.drop("Foul", axis=1, inplace=True, errors="ignore")
            try:
                events["ShotAssist"] = events["ShotAssist"].astype("Int64", errors="ignore")
            except:
                pass

            # write the events table
            table_name = league + "_" + year
            events["Qualifiers"] = events["Qualifiers"].apply(lambda x: [int(i) for i in x])
            events["SatisfiedEventsTypes"] = events["SatisfiedEventsTypes"].apply(lambda x: [int(i) for i in x])

            events = events.apply(pd.to_numeric, errors="ignore", downcast="integer")

            write_to_bq(events, table_name, "Event_Data", write_type="APPEND")  # append

            # write the matches table
            write_to_bq(all_matches, "Matches", "Lookup_Tables", write_type="APPEND", fix_type=False)
            # write scraped to text
            with open(LINKS_CACHE_FPATH, "a") as f:
                for item in match_urls:
                    f.write("%s\n" % item)
            os.remove(path)
        check_size(dataset_name="Event_Data")
    # write the DICT tables

    qualifiers = pd.DataFrame.from_dict(QUAL_DICT, orient="index").reset_index()
    qualifiers.columns = ["id", "qualifier"]
    with open("data/lookup/Qualifiers.json", "w") as f:
        json.dump(QUAL_DICT, f)

    refs = pd.DataFrame.from_dict(REF_DICT, orient="index").reset_index()
    refs.columns = ["id", "referee"]
    with open("data/lookup/Referees.json", "w") as f:
        json.dump(REF_DICT, f)

    stadiums = pd.DataFrame.from_dict(STADIUM_DICT, orient="index").reset_index()
    stadiums.columns = ["id", "stadium"]
    with open("data/lookup/Stadiums.json", "w") as f:
        json.dump(STADIUM_DICT, f)

    # write the id tables
    with open("data/lookup/Players.json", "w") as f:
        d = ALL_PLAYERS.set_index("id").to_dict().get("player")
        json.dump(d, f)

    with open("data/lookup/Teams.json", "w") as f:
        d = ALL_TEAMS.set_index("id").to_dict().get("team")
        json.dump(d, f)

    write_to_bq(qualifiers, "Qualifiers", "Lookup_Tables", write_type="WRITE_TRUNCATE")
    write_to_bq(refs, "Referees", "Lookup_Tables", write_type="WRITE_TRUNCATE")
    write_to_bq(stadiums, "Stadiums", "Lookup_Tables", write_type="WRITE_TRUNCATE")
    write_to_bq(ALL_PLAYERS, "Players", "Lookup_Tables", write_type="WRITE_TRUNCATE")
    write_to_bq(ALL_TEAMS, "Teams", "Lookup_Tables", write_type="WRITE_TRUNCATE")

    for path in paths:
        os.remove(path)
