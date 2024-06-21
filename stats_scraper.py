import argparse
from engines.fbref import FBRef
import time
import pandas as pd
from utils import *
from engines.request_utils import get_request
import os
import concurrent.futures

OVERALL_TIMEOUT = 100


def scrape_year(year: int, leagues: list, scraper: FBRef):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(leagues)) as executor:
        future_to_league = {executor.submit(scrape_league, year, league, scraper): league for league in leagues}
        all_results = {}
        for future in concurrent.futures.as_completed(future_to_league):
            league = future_to_league[future]
            # try:
            result_dict = future.result()
            for key in result_dict:
                if key not in all_results:
                    all_results[key] = result_dict[key]
                else:
                    all_results[key] = pd.concat([all_results[key], result_dict[key]], ignore_index=True)
            # except Exception as e:
            #     print(f"Error scraping league {league}: {e}")

    ids = dict(zip(all_results["squad"]["Standard Squad"].tolist(), all_results["squad"]["Standard Team_ID"].tolist()))
    print(f"Total Team IDs {year} {league}: ", len(ids))
    squad_logs = get_match_level(ids, year)
    all_results["squad_logs"] = squad_logs
    try:
        all_results["shots"]["Minute"] = all_results["shots"]["Minute"].astype(str)
    except Exception as e:
        print(all_results["shots"].columns)
        print(e)

    return parse_results(all_results)


def parse_results(results: dict):
    for k, v in results.items():
        results[k] = parse_columns(v)
    squad = results["squad"]
    against = results["against"]
    players = results["player_stats"]
    squad_gks = results["squad_gks"]
    against_gks = results["against_gks"]
    players_gk = results["player_gk"]
    shots = results["shots"]
    squad_logs = results["squad_logs"]
    player_logs = results["player_logs"]

    player_logs_league = player_logs[(player_logs["Stage"].str.contains("Matchweek"))]
    squad_logs_league = squad_logs[squad_logs["Round"].str.contains("Matchweek")]

    player_squad_logs_mapping = find_closest_matches(player_logs_league["Home_Team"].unique().tolist(), squad_logs_league["Squad"].unique().tolist())
    player_logs_league["Home_Team"] = player_logs_league["Home_Team"].map(player_squad_logs_mapping)
    player_logs_league["Away_Team"] = player_logs_league["Away_Team"].map(player_squad_logs_mapping)

    player_logs_league["Defense_Blocks"] = player_logs_league["Summary_Performance_Blocks"]
    player_logs_league["Defense_Tackles_Tkl"] = player_logs_league["Summary_Performance_Tkl"]
    player_logs_league["Defense_Int"] = player_logs_league["Summary_Performance_Int"]

    player_names = player_logs_league["Summary_Player"].unique().tolist()
    stats_team_mapping = {}
    for player in player_names:
        stats_team_mapping[player] = players[players["Standard_Player"] == player]["Standard_Squad"].values

    player_logs_league["Squad"] = player_logs_league.apply(lambda x: find_team(x, stats_team_mapping), axis=1)

    # Create mapping from logs to stats -> replace home and away teams with stats teams
    logs_to_stats = {}
    logs_teams = player_logs_league["Home_Team"].unique().tolist()
    stats_teams = players["Standard_Squad"].unique().tolist()
    for team in logs_teams:
        most_similar = find_most_similar_string(team, stats_teams)
        logs_to_stats[team] = most_similar
        stats_teams.remove(most_similar)

    assert len(logs_to_stats) == len(logs_teams)
    assert sorted(logs_to_stats.keys()) == sorted(squad_logs_league["Squad"].unique().tolist())
    print(f"Created mapping of {len(logs_to_stats)} teams from logs to stats")

    player_logs_league["Home_Team"] = player_logs_league["Home_Team"].map(logs_to_stats)
    player_logs_league["Away_Team"] = player_logs_league["Away_Team"].map(logs_to_stats)

    player_logs_league["Match_String"] = player_logs_league.apply(lambda row: "".join(sorted([row["Home_Team"], row["Away_Team"]])), axis=1)
    squad_logs_league["Match_String"] = squad_logs_league.apply(lambda row: "".join(sorted([row["Squad"], row["Opponent"]])), axis=1)

    merged_df = player_logs_league.merge(squad_logs_league[["Squad", "Match_String", "Poss"]], on=["Match_String", "Squad"], how="left")

    # for each player, drop duplicates based on stage
    merged_df = merged_df.drop_duplicates(subset=["Stage", "Summary_Player", "Squad"])

    # for logging purposes
    for team in merged_df["Squad"].unique().tolist():
        print(f"Found {len(merged_df[merged_df['Squad'] == team])} matches for {team}")

    print(f"Length of merged df {len(merged_df)} and length of player logs {len(player_logs_league)}")
    # assert len(merged_df) == len(player_logs_league), f"Length of merged df {len(merged_df)} does not match length of player logs {len(player_logs_league)}"
    # Length of merged df 56077 does not match length of player logs 56079

    to_adjust_metrics = [x for x in merged_df.columns if ("Defense" in x) or (x.startswith("Passing"))]
    to_adjust_metrics = [x for x in to_adjust_metrics if "pct" not in x.lower()]
    for metric in to_adjust_metrics:
        merged_df[f"Padj_{metric.replace('Defense', 'Defensive')}"] = merged_df.apply(lambda row: possession_adjust(row, metric), axis=1)

    print(merged_df.columns.tolist())
    padj_df = merged_df[["Summary_Player", "Summary_Player_ID"] + [f"Padj_{x}" for x in [y.replace("Defense", "Defensive") for y in to_adjust_metrics]]].groupby(["Summary_Player", "Summary_Player_ID"]).sum().reset_index()
    player_df = players.merge(padj_df, left_on=["Standard_Player", "Standard_Player_ID"], right_on=["Summary_Player", "Summary_Player_ID"], how="left")

    to_normalize = [x for x in player_df.columns if ("90" not in x) and ("pct" not in x.lower()) and ("playing_time" not in x.lower())]
    minutes = "Standard_Playing_Time_90s"
    player_df = player_df.apply(pd.to_numeric, errors="ignore")
    for col in to_normalize:
        try:
            player_df[col + "_Per_90"] = player_df[col] / player_df[minutes]
        except Exception as e:
            print(col, e)

    # To get accurate positioning

    sheet_id = "1GjjS9IRp6FVzVX5QyfmttMk8eYBtIzuZ_YIM0VWg8OY"
    mapping_df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv", on_bad_lines="skip")
    mapping_df["fbref_id"] = mapping_df["UrlFBref"].apply(lambda x: x.split("players/")[1].split("/")[0])
    player_df = player_df.merge(mapping_df, how="left", left_on="Standard_Player_ID", right_on="fbref_id")
    player_df = player_df.rename(columns={"TmPos": "Position"})
    player_df.drop(["UrlFBref", "fbref_id", "UrlTmarkt"], axis=1, inplace=True)

    return {
        "squad": squad,
        "against": against,
        "players": player_df,
        "squad_gks": squad_gks,
        "against_gks": against_gks,
        "players_gk": players_gk,
        "shots": shots,
        "squad_logs": squad_logs,
        "player_logs": player_logs,
    }


def scrape_league(year: int, league: str, scraper: FBRef):
    # TODO: error handling
    # TODO: Logging
    # data is a dictionary with keys as category names, values are typles of 3 dataframes
    # (squad, opponent, player_stats)

    data = scraper.scrape_all_stats(year, league)
    squad, squad_gks = parse_stats(data, 0)
    squad["League"] = league
    squad_gks["League"] = league
    against, against_gks = parse_stats(data, 1)
    against["League"] = league
    against_gks["League"] = league
    player_stats, player_gk = parse_stats(data, 2)
    player_stats["League"] = league
    player_gk["League"] = league

    matches = scraper.scrape_matches(year, league).dropna()
    print(f"Found {len(matches)} completed matches", end="\r", flush=True)
    shots = []
    for _, row in matches.iterrows():
        home_players = parse_match_stats(row["Home Player Stats"], row.to_frame())
        away_players = parse_match_stats(row["Away Player Stats"], row.to_frame())
        player_logs = pd.concat([player_logs, home_players, away_players], ignore_index=True)
        indiv_shots = parse_shots(row["Shots"].loc["Both"], row.to_frame())
        shots.append(indiv_shots)
    shots = pd.concat(shots, ignore_index=True)
    return {"squad": squad, "squad_gks": squad_gks, "against": against, "against_gks": against_gks, "player_stats": player_stats, "player_gk": player_gk, "player_logs": player_logs, "shots": shots}


### UTILS ###
def parse_stats(data, index):
    def merge_dfs(dfs):
        if not dfs:
            return pd.DataFrame()

        df = dfs[0]
        for x in dfs[1:]:
            left_col = df.columns.tolist()[0]
            right_col = x.columns.tolist()[0]
            df[left_col] = df[left_col].fillna(0)
            df = pd.merge(df, x, how="left", left_on=left_col, right_on=right_col)

        df = df.loc[:, ~df.columns.duplicated()]
        df = df.T.drop_duplicates().T
        return df

    def fix_columns(df, key):
        cols = df.columns.tolist()
        parsed = [col[1] if "Unnamed" in col[0] else col[0] if col[0] == col[1] else f"{col[0]} {col[1]}" for col in cols]
        df.columns = [f"{key.title()} {col.strip().replace(' ', '_')}" for col in parsed]
        return df

    dfs = [fix_columns(data[key][index].copy(), key) for key in data if key not in ["goalkeeping", "advanced goalkeeping"]]
    gk_dfs = [fix_columns(data[key][index].copy(), key) for key in data if key in ["goalkeeping", "advanced goalkeeping"]]

    df = merge_dfs(dfs)
    gk_df = merge_dfs(gk_dfs)

    return df, gk_df


def parse_match_stats(one: pd.Series, main):
    raw = one.iloc[1:].to_dict(orient="index")
    dfs = []
    for key in raw:
        curr = raw.get(key)[0]
        cols = curr.columns.tolist()
        parsed = []
        for col in cols:
            if "Unnamed" in col[0]:
                parsed.append(col[1])
            elif col[0] == col[1]:
                parsed.append(col[0])
            else:
                parsed.append(col[0] + " " + col[1])
        curr.columns = [key.title() + " " + x.strip().replace(" ", "_") for x in parsed]
        dfs.append(curr)
    parsed = pd.concat(dfs, axis=1).iloc[:-1]
    parsed = pd.concat(dfs, axis=1).iloc[:-1]
    for x in ["Date", "Stage", "Home Team", "Away Team", "Home Goals", "Away Goals"]:
        parsed[x.strip().replace(" ", "_")] = main.loc[x].item()
    parsed["Date"] = pd.to_datetime(parsed["Date"])
    return parsed


def parse_shots(one: pd.Series, main):
    cols = one.columns.tolist()
    parsed = []
    for col in cols:
        if "Unnamed" in col[0]:
            parsed.append(col[1])
        elif col[0] == col[1]:
            parsed.append(col[0])
        else:
            parsed.append(col[0] + " " + col[1])
    one.columns = [x.strip().replace(" ", "_") for x in parsed]
    for x in ["Date", "Stage", "Home Team", "Away Team", "Home Goals", "Away Goals"]:
        one[x.strip().replace(" ", "_")] = main.loc[x].item()
    one["Date"] = pd.to_datetime(one["Date"])
    return one


def get_match_level(teams, season):  # get individual match level data
    def fix_penalty(df):
        penfor = df["GF"].apply(lambda x: str(x).split()[-1].replace("(", "").replace(")", "") if (len(str(x).split()) > 1) else 0)
        penagainst = df["GA"].apply(lambda x: str(x).split()[-1].replace("(", "").replace(")", "") if (len(str(x).split()) > 1) else 0)
        df["GF"] = df["GF"].apply(lambda x: str(x).split("(")[0])
        df["GA"] = df["GA"].apply(lambda x: str(x).split("(")[0])
        df.insert(10, "penfor", penfor)
        df.insert(11, "penagainst", penagainst)
        return df

    dfs = []
    for team in teams:
        tmp = get_match_logs(teams.get(team), season, team)
        dfs.append(tmp)

    df = pd.concat(dfs, ignore_index=True)
    df = df.dropna(thresh=35).fillna(0)
    df = df.T.drop_duplicates().T
    df = fix_penalty(df)
    r = df.drop("Date", axis=1).apply(pd.to_numeric, errors="ignore")
    r["Date"] = df["Date"]
    return r.drop_duplicates()


def get_match_logs(id, year, team):
    print("Scraping", team, "for", year, end="\r", flush=True)
    prefixes = ["shooting", "keeper", "passing", "passing_types", "gca", "defense", "possession", "misc"]
    starting_url = f"https://fbref.com/en/squads/{id}/{year-1}-{year}/matchlogs/all_comps/"
    dfs = []
    for prefix in prefixes:
        newurl = starting_url + prefix
        tmp = pd.read_html(get_request(new_url).content)[0]
        cols = tmp.columns.tolist()
        parsed = []
        for col in cols:
            if len(col[0].split()) > 1:
                parsed.append(prefix + " " + col[1])
            elif col[0] == col[1]:
                parsed.append(prefix + " " + col[0])
            else:
                parsed.append(prefix + " " + col[0] + " " + col[1])
        tmp.columns = [x.strip().replace(" ", "_").title() for x in parsed]
        dfs.append(tmp)
    df = pd.concat(dfs, axis=1)
    df = df.drop([x for x in df.columns.tolist() if ("Notes" in x) | ("Match_Report" in x)], axis=1)
    new_url = starting_url + "schedule"
    tmp = pd.read_html(new_url)[0].drop(["Match Report", "Notes"], axis=1, errors="ignore")
    df = tmp.merge(df, how="right", left_on="Date", right_on="Shooting_Date")
    df.insert(1, "Squad", team)
    df = df.replace("Champions Lg", "Champions League").replace("Europa Lg", "Europa League")
    df = df.drop(df.tail(1).index)
    return df


def parse_columns(df: pd.DataFrame):
    oldcols = df.columns.tolist()
    new = [
        x.replace("#", "_No_").replace(" ", "_").replace("(", "").replace("-", "_").replace(")", "").replace("%", "_Pct_").replace("1/3", "One_Third").replace("/", "_per").replace("+", "_Plus_").replace(":", "_").replace("__", "_")
        for x in oldcols
    ]
    new = [x.strip("_") for x in new]
    df.columns = new
    df = df.T.drop_duplicates().T
    df = df.drop_duplicates()
    df.drop(["Standard_Matches", "Standard_Rk", "Standard Matches"], axis=1, inplace=True, errors="ignore")
    return df.loc[:, ~df.columns.duplicated()]


def find_team(row, mapping):
    log_player = row["Summary_Player"]
    home, away = row["Home_Team"], row["Away_Team"]
    stats_team = mapping[log_player]
    if home in stats_team:
        return home
    else:
        return away


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, help="Start year", default=2024)
    parser.add_argument("--end", type=int, help="End year", default=2024)
    parser.add_argument("--leagues", nargs="+", help="Leagues included are for eg ['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga', 'Eredivisie', 'Primeira Liga']", default=["EPL", "La Liga"])
    # parser.add_argument("--leagues", nargs="+", help="Leagues included are for eg ['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga', 'Eredivisie', 'Primeira Liga']", default=["EPL", "La Liga", "Serie A", "Ligue 1", "Bundesliga"])

    parser.add_argument("--write_type", type=str, help="Write Type", default="WRITE_TRUNCATE")
    args = parser.parse_args()
    years = range(args.start, args.end + 1)
    fbref_scraper = FBRef()

    os.makedirs("data", exist_ok=True)
    for year in years:
        results = scrape_year(year, args.leagues, fbref_scraper)
        for k, v in results.items():
            v.to_csv(f"data/{year}_{k}.csv", index=False)

    fbref_scraper.close()
