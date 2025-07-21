import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, List
from io import StringIO

import pandas as pd

from engines.fbref import FBRefWrapper
from utils.data_utils import merge_dataframes, format_column_names, find_closest_matches, find_most_similar_string
from utils.types import ScrapeLeagueResult, StatsScraperResult
from utils.bq_utils import WriteType, write_to_bq


def _process_stats(stats: List[Tuple[str, pd.DataFrame]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    general_stats = []
    gk_stats = []
    for key, stat_data in stats:
        stat_data = format_column_names(stat_data, key)
        if "goalkeeping" in key:
            gk_stats.append(stat_data.copy())
        else:
            general_stats.append(stat_data.copy())
    return merge_dataframes(general_stats), merge_dataframes(gk_stats)


def _parse_match_stats_for_players(player_stats: pd.Series):
    dfs = [format_column_names(df, key) for key, df in player_stats.items()]
    # Drop the last total row
    return pd.concat(dfs, axis=1).iloc[:-1]


def _find_team(row, mapping):
    log_player = row["Summary_Player"]
    home, away = row["Home_Team"], row["Away_Team"]
    stats_team = mapping[log_player]
    if home in stats_team:
        return home
    else:
        return away


def _possession_adjust(row: pd.Series, metric: str):
    opp_possession = 100 - row["Poss"]
    assert opp_possession >= 0, f"{row['Poss']} is not a valid possession value for metric {metric}"
    return row[metric] / opp_possession * 50


def process_results(results: StatsScraperResult) -> StatsScraperResult:
    # Clean column names and remove duplicates across all dataframes
    for k, v in results.items():
        v: pd.DataFrame
        oldcols = v.columns.tolist()
        new = [
            x.replace("#", "_No_")
            .replace(" ", "_")
            .replace("(", "")
            .replace("-", "_")
            .replace(")", "")
            .replace("%", "_Pct_")
            .replace("1/3", "One_Third")
            .replace("/", "_per")
            .replace("+", "_Plus_")
            .replace(":", "_")
            .replace("__", "_")
            for x in oldcols
        ]
        new = [x.strip("_") for x in new]
        v.columns = new
        v = v.T.drop_duplicates().T
        v = v.drop_duplicates()
        v.drop(["Standard_Matches", "Standard_Rk", "Standard Matches"], axis=1, inplace=True, errors="ignore")
        results[k] = v.loc[:, ~v.columns.duplicated()]

    # Extract individual dataframes from results dictionary
    squad = results["squad"]
    against = results["against"]
    players = results["player_stats"]
    squad_gks = results["squad_gk"]
    against_gks = results["against_gk"]
    players_gk = results["player_gk"]
    shots = results["shots"]
    squad_logs = results["squad_logs"]
    player_logs = results["player_logs"]

    # Filter to only include league matches (Matchweek games)
    player_logs_league = player_logs[(player_logs["Stage"].str.contains("Matchweek"))]
    squad_logs_league = squad_logs[squad_logs["Round"].str.contains("Matchweek")]

    # Map team names between player logs and squad logs for consistency
    player_squad_logs_mapping = find_closest_matches(player_logs_league["Home_Team"].unique().tolist(), squad_logs_league["Squad"].unique().tolist())
    player_logs_league["Home_Team"] = player_logs_league["Home_Team"].map(player_squad_logs_mapping)
    player_logs_league["Away_Team"] = player_logs_league["Away_Team"].map(player_squad_logs_mapping)

    # Add standardized defense metric columns for consistency
    player_logs_league["Defense_Blocks"] = player_logs_league["Summary_Performance_Blocks"]
    player_logs_league["Defense_Tackles_Tkl"] = player_logs_league["Summary_Performance_Tkl"]
    player_logs_league["Defense_Int"] = player_logs_league["Summary_Performance_Int"]

    # Create mapping from players to their teams for squad identification
    player_names = player_logs_league["Summary_Player"].unique().tolist()
    stats_team_mapping = {}
    for player in player_names:
        stats_team_mapping[player] = players[players["Standard_Player"] == player]["Standard_Squad"].values

    player_logs_league["Squad"] = player_logs_league.apply(lambda x: _find_team(x, stats_team_mapping), axis=1)

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

    # Apply team name mapping to standardize across datasets
    player_logs_league["Home_Team"] = player_logs_league["Home_Team"].map(logs_to_stats)
    player_logs_league["Away_Team"] = player_logs_league["Away_Team"].map(logs_to_stats)

    # Create unique match identifiers for merging player and squad data
    player_logs_league["Match_String"] = player_logs_league.apply(lambda row: "".join(sorted([row["Home_Team"], row["Away_Team"]])), axis=1)
    squad_logs_league["Match_String"] = squad_logs_league.apply(lambda row: "".join(sorted([row["Squad"], row["Opponent"]])), axis=1)

    # Merge player logs with squad possession data using match and squad identifiers
    merged_df = player_logs_league.merge(squad_logs_league[["Squad", "Match_String", "Poss"]], on=["Match_String", "Squad"], how="left")

    # Remove duplicate player entries per match and log team counts for verification
    merged_df = merged_df.drop_duplicates(subset=["Stage", "Summary_Player", "Squad"])

    # for logging purposes
    for team in merged_df["Squad"].unique().tolist():
        print(f"Found {len(merged_df[merged_df['Squad'] == team])} matches for {team}")

    print(f"Length of merged df {len(merged_df)} and length of player logs {len(player_logs_league)}")
    # assert len(merged_df) == len(player_logs_league), f"Length of merged df {len(merged_df)} does not match length of player logs {len(player_logs_league)}"
    # Length of merged df 56077 does not match length of player logs 56079

    # Calculate possession-adjusted metrics for defensive and passing statistics
    to_adjust_metrics = [x for x in merged_df.columns if ("Defense" in x) or (x.startswith("Passing"))]
    to_adjust_metrics = [x for x in to_adjust_metrics if "pct" not in x.lower()]
    for metric in to_adjust_metrics:
        merged_df[f"Padj_{metric.replace('Defense', 'Defensive')}"] = merged_df.apply(lambda row: _possession_adjust(row, metric), axis=1)

    # Aggregate possession-adjusted metrics by player across all matches
    padj_df = (
        merged_df[["Summary_Player", "Summary_Player_ID"] + [f"Padj_{x}" for x in [y.replace("Defense", "Defensive") for y in to_adjust_metrics]]]
        .groupby(["Summary_Player", "Summary_Player_ID"])
        .sum()
        .reset_index()
    )
    player_df = players.merge(padj_df, left_on=["Standard_Player", "Standard_Player_ID"], right_on=["Summary_Player", "Summary_Player_ID"], how="left")

    # Normalize all counting stats to per-90-minute rates for fair comparison
    to_normalize = [x for x in player_df.columns if ("90" not in x) and ("pct" not in x.lower()) and ("playing_time" not in x.lower())]
    minutes = "Standard_Playing_Time_90s"
    player_df = player_df.apply(pd.to_numeric, errors="ignore")
    for col in to_normalize:
        try:
            player_df[col + "_Per_90"] = player_df[col] / player_df[minutes]
        except Exception as e:
            print(col, e)

    # Add accurate position data from external mapping spreadsheet
    sheet_id = "1GjjS9IRp6FVzVX5QyfmttMk8eYBtIzuZ_YIM0VWg8OY"
    mapping_df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv", on_bad_lines="skip")
    mapping_df["fbref_id"] = mapping_df["UrlFBref"].apply(lambda x: x.split("players/")[1].split("/")[0])
    player_df = player_df.merge(mapping_df, how="left", left_on="Standard_Player_ID", right_on="fbref_id")
    player_df = player_df.rename(columns={"TmPos": "Position"})
    player_df.drop(["UrlFBref", "fbref_id", "UrlTmarkt"], axis=1, inplace=True)

    # Return processed and cleaned dataset with enhanced player statistics
    processed_result: StatsScraperResult = {
        "squad": squad,
        "squad_gk": squad_gks,
        "against": against,
        "against_gk": against_gks,
        "player_stats": player_df,
        "player_gk": players_gk,
        "matches": results["matches"],
        "player_logs": player_logs,
        "shots": shots,
        "squad_logs": squad_logs,
    }
    return processed_result


def scrape_league(year: int, league: str, fb: FBRefWrapper) -> ScrapeLeagueResult:
    # {category: (squad_stats, opponent_stats, player_stats)}
    data: Dict[str, Tuple[pd.DataFrame, pd.DataFrame]] = fb.scrape_all_stats(year, league)
    results = {}
    squad_stats = [(key, stat_data[0]) for key, stat_data in data.items()]
    opponent_stats = [(key, stat_data[1]) for key, stat_data in data.items()]
    player_stats = [(key, stat_data[2]) for key, stat_data in data.items()]
    # There is an issue with scraping the column names for the squads so we use the opponent stats to fix it
    for (_, squad_stat), (_, opponent_stat) in zip(squad_stats, opponent_stats):
        squad_stat.columns = opponent_stat.columns

    squad_df, squad_gk_df = _process_stats(squad_stats)
    opponent_df, opponent_gk_df = _process_stats(opponent_stats)
    player_df, player_gk_df = _process_stats(player_stats)

    for _df in [squad_df, squad_gk_df, opponent_df, opponent_gk_df, player_df, player_gk_df]:
        _df["League"] = league

    results = {
        "squad": squad_df,
        "squad_gk": squad_gk_df,
        "against": opponent_df,
        "against_gk": opponent_gk_df,
        "player_stats": player_df,
        "player_gk": player_gk_df,
    }

    matches = fb.scrape_matches(year, league)

    shots_dfs = []
    player_logs = []
    for _, row in matches.iterrows():
        home_player_stats = _parse_match_stats_for_players(row["Home Player Stats"])
        away_player_stats = _parse_match_stats_for_players(row["Away Player Stats"])
        match_id = row["Link"].split("/")[-2].strip()
        home_player_stats["Match ID"] = match_id
        away_player_stats["Match ID"] = match_id
        shots = format_column_names(row["Shots"]["Both"], "")
        shots["Minute"] = shots["Minute"].astype(str)
        player_logs.append(pd.concat([home_player_stats, away_player_stats], ignore_index=True))
        shots_dfs.append(shots)
    matches.drop(columns=["Home Player Stats", "Away Player Stats", "Shots"], inplace=True)

    results["matches"] = matches
    results["player_logs"] = pd.concat(player_logs, ignore_index=True)
    results["shots"] = pd.concat(shots_dfs, ignore_index=True)
    return results


def get_match_logs(team_id: str, year: int, team: str, fb: FBRefWrapper) -> pd.DataFrame:
    prefixes = ["schedule", "shooting", "keeper", "passing", "passing_types", "gca", "defense", "possession", "misc"]
    starting_url = f"https://fbref.com/en/squads/{team_id}/{year}/matchlogs/all_comps/"

    dfs = []
    for prefix in prefixes:
        newurl = starting_url + prefix
        r = fb._get(newurl)
        # we can just handle the team since the opponent will be handled in the next iteration
        for_df = pd.read_html(StringIO(r.text))[0]
        # for_df = format_column_names(for_df, prefix)
        cols = for_df.columns.tolist()
        parsed = []
        for col in cols:
            if type(col) == tuple:
                if ("For " in col[0]) or ("Unnamed" in col[0]):
                    parsed.append(col[1])
                else:
                    parsed.append(col[0] + " " + col[1])
            else:
                parsed.append(col)
        for_df.columns = [x.strip().replace(" ", "_").title() for x in parsed]
        dfs.append(for_df)

    df = pd.concat(dfs, axis=1)
    df = df.drop([x for x in df.columns.tolist() if ("Notes" in x) | ("Match_Report" in x)], axis=1)
    df = df.T.drop_duplicates().T
    df.insert(1, "Squad", team)
    df = df.replace("Champions Lg", "Champions League").replace("Europa Lg", "Europa League")
    df = df.rename(columns={"Gf": "GF", "Ga": "GA"})

    # Penalties are in the GF/GA columns of the form FT (PEN)
    penfor = df["GF"].apply(lambda x: str(x).split()[-1].replace("(", "").replace(")", "") if (len(str(x).split()) > 1) else 0)
    penagainst = df["GA"].apply(lambda x: str(x).split()[-1].replace("(", "").replace(")", "") if (len(str(x).split()) > 1) else 0)
    df["GF"] = df["GF"].apply(lambda x: str(x).split("(")[0])
    df["GA"] = df["GA"].apply(lambda x: str(x).split("(")[0])
    df.insert(10, "penfor", penfor)
    df.insert(11, "penagainst", penagainst)
    return df.apply(pd.to_numeric, errors="ignore").drop_duplicates()


def scrape_year(year: int, league: str) -> StatsScraperResult:
    # with FBRefWrapper() as fb:
    fb = FBRefWrapper()
    league_results = scrape_league(year, league, fb)
    team_ids = dict(zip(league_results["matches"]["Home Team ID"].tolist(), league_results["matches"]["Home Team"].tolist()))
    match_logs = []
    for team_id in team_ids:
        match_logs.append(get_match_logs(team_id, year, team_ids[team_id], fb))
    match_logs_clean = [df.reset_index(drop=True) for df in match_logs]
    squad_logs = pd.concat(match_logs_clean, ignore_index=True)

    raw_results: StatsScraperResult = {"squad_logs": squad_logs, **league_results}
    processed_results = process_results(raw_results)
    # fb.quit() is automatically called by the context manager
    return processed_results


def process_league_year(year_string: str, league: str, table_mapping: dict, write_type: WriteType) -> None:
    """Process a single league-year combination and write results to BigQuery."""
    year_suffix = year_string.split("-")[1]  # Get ending year (e.g., "2024" from "2023-2024")
    print(f"Starting {year_string} {league}...")

    try:
        results = scrape_year(year_string, league)

        for k, v in results.items():
            if k in table_mapping:
                dataset_name, table_prefix = table_mapping[k]
                if k == "shots":
                    # E.g. Shots tables: Shots_2024
                    table_name = f"{table_prefix}_{year_suffix}"
                elif k == "squad_logs":
                    # E.g. Squad match logs: Squad_Match_Logs_2024_EPL
                    table_name = f"{table_prefix}_{year_suffix}_{league}"
                else:
                    # E.g. Stats tables: Players_2024_EPL, Against_2024_EPL, etc.
                    table_name = f"{table_prefix}_{year_suffix}_{league}"

                write_to_bq(v, table_name, dataset_name, write_type)

        print(f"Completed {year_string} {league}")

    except Exception as e:
        print(f"Error processing {year_string} {league}: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, help="Start year", default=datetime.datetime.now().year - 1)
    parser.add_argument("--end", type=int, help="End year", default=datetime.datetime.now().year)
    parser.add_argument(
        "--leagues",
        nargs="+",
        help="Leagues included are for eg ['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga', 'Eredivisie', 'Primeira Liga']",
        default=[
            "EPL",
            "EFL Championship",
            "La Liga",
            "Serie A",
            "Ligue 1",
            "Bundesliga",
        ],
    )
    parser.add_argument("--write_type", type=WriteType, help="Write Type", default=WriteType.WRITE_TRUNCATE)
    args = parser.parse_args()

    years = range(args.start, args.end + 1)
    year_strings = [f"{year}-{year+1}" for year in years]

    # (Dataset, Table)
    table_mapping = {
        "shots": ("Shots", "Shots"),
        "squad_logs": ("Squad_Match_Logs", "Squad_Match_Logs"),
        "player_logs": ("Stats", "Player_Logs"),
        "squad": ("Stats", "Squad"),
        "squad_gk": ("Stats", "Squad_GK"),
        "against": ("Stats", "Against"),
        "against_gk": ("Stats", "Against_GK"),
        "player_stats": ("Stats", "Players"),
        "player_gk": ("Stats", "Players_GK"),
        "matches": ("Stats", "Matches"),
    }

    # Process each year sequentially, but leagues within each year concurrently
    for year_string in year_strings:
        # Not too many due to FBRef rate limiting
        # with ThreadPoolExecutor(max_workers=1) as executor:
        #     future_to_league = {executor.submit(process_league_year, year_string, league, table_mapping, args.write_type): league for league in args.leagues}

        #     for future in as_completed(future_to_league):
        #         league = future_to_league[future]
        #         # try:
        #         #     future.result()
        #         # except Exception as e:
        #         #     print(f"Error with {year_string} {league}: {e}")
        #         future.result()
        for league in args.leagues:
            process_league_year(year_string, league, table_mapping, args.write_type)
