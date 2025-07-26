from typing import TypedDict, List
import pandas as pd


class ScrapeLeagueResult(TypedDict):
    squad: pd.DataFrame
    squad_gk: pd.DataFrame
    against: pd.DataFrame
    against_gk: pd.DataFrame
    player_stats: pd.DataFrame
    player_gk: pd.DataFrame
    matches: pd.DataFrame
    player_logs: pd.DataFrame
    shots: pd.DataFrame


class StatsScraperResult(ScrapeLeagueResult):
    squad_logs: pd.DataFrame


class RawEventData(TypedDict):
    matchCentreData: dict
    matchCentreEventTypeJson: dict
    matchId: int
    formationIdNameMappings: dict
