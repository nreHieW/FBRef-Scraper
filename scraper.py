import argparse
import os
import time
import datetime
import json
import difflib
import pandas as pd
from fbref import FBRef
from google.cloud import bigquery

pd.options.mode.chained_assignment = None

class TeamPlayerScraper:
    def __init__(self, start:int, end:int, leagues: list = ['EPL', 'La Liga', 'Serie A','Ligue 1','Bundesliga'], write_type:str = "WRITE_TRUNCATE") -> None:
        self.start = start
        self.end = end
        self.years = list(range(start, end + 1))
        self.leagues = leagues
        self.write_type = write_type

    def scrape(self) -> None:
        self.scraper = FBRef()
        for year in self.years:
            players = pd.DataFrame()
            players_gks = pd.DataFrame()
            squad = pd.DataFrame()
            squad_gks = pd.DataFrame()
            against = pd.DataFrame()
            against_gks = pd.DataFrame()
            shots = pd.DataFrame()
            player_logs = pd.DataFrame()
            squad_logs = pd.DataFrame()
            for league in self.leagues:
                try:
                    data = self.scraper.scrape_all_stats(year, league=league)
                except Exception as e:
                    print(e)
                    time.sleep(180)
                    data = self.scraper.scrape_all_stats(year, league=league)
                squad_indiv,squad_gk_indiv = self._parse_stats(data,0)
                squad_indiv['League'] = league
                squad_gk_indiv['League'] = league
                against_indiv,against_gks_indiv = self._parse_stats(data,1)
                against_indiv['League'] = league
                against_gks_indiv['League'] = league
                player_stats, player_gk_indiv = self._parse_stats(data,2)
                player_stats['League'] = league
                player_gk_indiv['League'] = league

                squad = pd.concat([squad, squad_indiv],ignore_index=True)
                squad_gks = pd.concat([squad_gks, squad_gk_indiv],ignore_index=True)
                against_gks = pd.concat([against_gks, against_gks_indiv],ignore_index=True)
                against = pd.concat([against, against_indiv],ignore_index=True)
                players = pd.concat([players, player_stats],ignore_index=True)
                players_gks = pd.concat([players_gks, player_gk_indiv],ignore_index=True)

                matches = self.scraper.scrape_matches(year, league).dropna()
                print(f'Found {len(matches)} completed matches', end='\r', flush=True)
                for row in matches.iterrows():
                    home_players = self._parse_match_stats(row[1]['Home Player Stats'],row[1].to_frame())
                    away_players = self._parse_match_stats(row[1]['Away Player Stats'],row[1].to_frame())
                    player_logs = pd.concat([player_logs, home_players,away_players],ignore_index=True)
                    indiv_shots = self._parse_shots(row[1]['Shots'].loc['Both'],row[1].to_frame())
                    shots = pd.concat([shots, indiv_shots],ignore_index=True)

            ids = dict(zip(squad['Standard Squad'].tolist(),squad['Standard Team_ID'].tolist()))
            print('Total Team IDs: ',len(ids))
            tmp = self._get_match_level(ids,year)
            squad_logs = pd.concat([squad_logs,tmp],ignore_index=True)
            try:
                shots['Minute'] = shots['Minute'].astype(str)
            except Exception as e:
                print(shots.columns)
                print(e)
                
            #save as csv
            if not os.path.exists('data'):
                    os.makedirs('data')

            squad = self._parse_columns(squad)
            against = self._parse_columns(against)
            players = self._parse_columns(players)
            squad_gks = self._parse_columns(squad_gks)
            against_gks = self._parse_columns(against_gks)
            players_gks = self._parse_columns(players_gks)
            shots = self._parse_columns(shots)
            squad_logs = self._parse_columns(squad_logs)
            player_logs = self._parse_columns(player_logs)

            player_logs_league = player_logs[player_logs["Stage"].str.contains("Matchweek")]
            squad_logs_league = squad_logs[squad_logs["Round"].str.contains("Matchweek")]

            player_squad_logs_mapping = self._find_closest_matches(player_logs_league["Home_Team"].unique().tolist(), squad_logs_league["Squad"].unique().tolist())
            player_logs_league["Home_Team"] = player_logs_league["Home_Team"].map(player_squad_logs_mapping)
            player_logs_league["Away_Team"] = player_logs_league["Away_Team"].map(player_squad_logs_mapping)

            player_logs_league["Defense_Blocks"] = player_logs_league['Summary_Performance_Blocks']
            player_logs_league["Defense_Tackles_Tkl"] = player_logs_league['Summary_Performance_Tkl']
            player_logs_league["Defense_Int"] = player_logs_league["Summary_Performance_Int"]

            player_logs_league = player_logs_league.drop_duplicates(subset=["Stage"])
            squad_logs_league = squad_logs_league.drop_duplicates(subset=["Stage"])

            player_names = player_logs_league["Summary_Player"].unique().tolist()
            stats_team_mapping = {}
            for player in player_names:
                stats_team_mapping[player] = players[players["Standard_Player"] == player]["Standard_Squad"].values

            player_logs_league["Squad"] = player_logs_league.apply(lambda x: self._find_team(x, stats_team_mapping), axis = 1)

            # Create mapping from logs to stats -> replace home and away teams with stats teams
            logs_to_stats = {}
            logs_teams = player_logs_league["Home_Team"].unique().tolist()
            stats_teams = players["Standard_Squad"].unique().tolist()
            for team in logs_teams:
                most_similar = self._find_most_similar_string(team, stats_teams)
                logs_to_stats[team] = most_similar
                stats_teams.remove(most_similar)

            assert len(logs_to_stats) == len(logs_teams)
            assert sorted(logs_to_stats.keys()) == sorted(squad_logs_league["Squad"].unique().tolist())
            print(f"Created mapping of {len(logs_to_stats)} teams from logs to stats")
            
            player_logs_league["Home_Team"] = player_logs_league['Home_Team'].map(logs_to_stats)
            player_logs_league["Away_Team"] = player_logs_league['Away_Team'].map(logs_to_stats)

            player_logs_league["Match_String"] = player_logs_league.apply(lambda row: "".join(sorted([row["Home_Team"], row["Away_Team"]])), axis = 1)
            squad_logs_league["Match_String"] = squad_logs_league.apply(lambda row: "".join(sorted([row["Squad"], row["Opponent"]])), axis = 1)

            merged_df = player_logs_league.merge(squad_logs_league[["Squad","Match_String", "Poss"]], on = ["Match_String", "Squad"], how = "left")

            assert len(merged_df) == len(player_logs_league), f"Length of merged df {len(merged_df)} does not match length of player logs {len(player_logs_league)}"

            # for each team, drop duplicates based on stage 
            merged_df = merged_df.drop_duplicates(subset=["Stage", "Squad"])

            # for logging purposes
            for team in merged_df["Squad"].unique().tolist():
                print(f"Found {len(merged_df[merged_df['Squad'] == team])} matches for {team}")

            
            to_adjust_metrics = [x for x in merged_df.columns if ("Defense" in x) or (x.startswith("Passing"))]
            to_adjust_metrics = [x for x in to_adjust_metrics if 'pct' not in x.lower()]
            for metric in to_adjust_metrics:
                merged_df[f"Padj_{metric.replace('Defense', 'Defensive')}"] = merged_df.apply(lambda row: self._posession_adjust(row, metric), axis = 1)

            padj_df = merged_df[["Summary_Player"] + [f"Padj_{x}" for x in [y.replace("Defense", "Defensive") for y in to_adjust_metrics]]].groupby(["Summary_Player"]).sum().reset_index()
            player_df = players.merge(padj_df, left_on = ["Standard_Player"], right_on=["Summary_Player"],how = "left")

            to_normalize = [x for x in player_df.columns if ("90" not in x ) and ("pct" not in x.lower()) and ("playing_time" not in x.lower()) ]
            minutes = 'Standard_Playing_Time_90s'
            player_df = player_df.apply(pd.to_numeric, errors='ignore')
            for col in to_normalize:
                try:
                    player_df[col + "_Per_90"] = player_df[col] / player_df[minutes] 
                except Exception as e:
                    print(col, e)

            # To get accurate positioning

            sheet_id = "1GjjS9IRp6FVzVX5QyfmttMk8eYBtIzuZ_YIM0VWg8OY"
            mapping_df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv", on_bad_lines='skip')
            mapping_df['fbref_id'] = mapping_df['UrlFBref'].apply(lambda x: x.split("players/")[1].split("/")[0])
            player_df = player_df.merge(mapping_df, how="left", left_on="Standard_Player_ID", right_on="fbref_id")
            player_df = player_df.rename(columns={"TmPos": "Position"})
            player_df.drop(["UrlFBref", "fbref_id", "UrlTmarkt"], axis=1, inplace=True)


            if self.write_type == 'WRITE_TRUNCATE':
                squad.to_csv(f'data/{year}_squad.csv',index=False)
                against.to_csv(f'data/{year}_against.csv',index=False)
                player_df.to_csv(f'data/{year}_players.csv',index=False)
                squad_gks.to_csv(f'data/{year}_squad_gks.csv',index=False)
                against_gks.to_csv(f'data/{year}_against_gks.csv',index=False)
                players_gks.to_csv(f'data/{year}_players_gks.csv',index=False)
                shots.to_csv(f'data/{year}_shots.csv',index=False)
                squad_logs.to_csv(f'data/{year}_squad_logs.csv',index=False)
                player_logs.to_csv(f'data/{year}_player_logs.csv',index=False)

            #save to bigquery
            self._write_to_bq(squad, f'Squad_{year}','Stats',self.write_type)
            self._write_to_bq(against, f'Against_{year}','Stats',self.write_type)
            self._write_to_bq(player_df, f'Players_{year}','Stats',self.write_type)
            self._write_to_bq(squad_gks, f'Squad_GK_{year}','Stats',self.write_type)
            self._write_to_bq(against_gks, f'Against_GK_{year}','Stats',self.write_type)
            self._write_to_bq(players_gks, f'Players_GK_{year}','Stats',self.write_type)
            self._write_to_bq(shots, f'Shots_{year}','Stats', self.write_type)
            self._write_to_bq(squad_logs, f'Squad_Logs_{year}','Stats',self.write_type)
            self._write_to_bq(player_logs, f'Player_Logs_{year}','Stats',self.write_type)
            print(f'Finished {year}')

        self.scraper.close()
    
            
    def _parse_stats(self, d, i):
        '''
        0 is standard 
        1 is against
        2 is players
        '''
        def merge_dfs(dfs):
            df = pd.DataFrame()
            first = True
            for x in dfs:
                if first:
                    df = x
                    first = False
                else:
                    left_cols = df.columns.tolist()[0]
                    right_cols = x.columns.tolist()[0]
                    df[left_cols] = df[left_cols].fillna(0)
                    df = pd.merge(df,x,how='left',left_on=left_cols,right_on=right_cols)
            df = df.loc[:, ~df.columns.duplicated()]
            df = df.T.drop_duplicates().T
            return df
        def fix_columns(curr,key):
            cols = curr.columns.tolist()
            parsed = []
            for col in cols:
                if ('Unnamed' in col[0]):
                    parsed.append(col[1])
                elif (col[0] == col[1]):
                    parsed.append(col[0])
                else:
                    parsed.append(col[0] + " " +col[1])
            curr.columns = [key.title() + ' ' + x.strip().replace(' ','_') for x in parsed]
            return curr
        dfs = []
        gk_dfs = []
        for key in d:
            if key not in ['goalkeeping', 'advanced goalkeeping']:
                dfs.append(fix_columns(d[key][i].copy(),key))
            else:
                gk_dfs.append(fix_columns(d[key][i].copy(),key))
        df = merge_dfs(dfs)
        gk_df = merge_dfs(gk_dfs)
        return df,gk_df
    
    def _parse_match_stats(self, one,main):
        raw = one.iloc[1:].to_dict(orient = 'index')
        dfs = []
        for key in raw:
            curr = raw.get(key)[0]
            cols = curr.columns.tolist()
            parsed = []
            for col in cols:
                if ('Unnamed' in col[0]):
                    parsed.append(col[1])
                elif (col[0] == col[1]):
                    parsed.append(col[0])
                else:
                    parsed.append(col[0] + " " +col[1])
            curr.columns = [key.title() + ' ' + x.strip().replace(' ','_') for x in parsed]
            dfs.append(curr)
        parsed = pd.concat(dfs,axis=1).iloc[:-1]
        parsed =  pd.concat(dfs,axis=1).iloc[:-1]
        for x in ['Date','Stage','Home Team','Away Team','Home Goals','Away Goals']:
            parsed[x.strip().replace(' ','_')] = main.loc[x].item()
        parsed['Date'] = pd.to_datetime(parsed['Date'])
        return parsed

    def _parse_shots(self, one, main):
        cols = one.columns.tolist()
        parsed = []
        for col in cols:
            if ('Unnamed' in col[0]):
                parsed.append(col[1])
            elif (col[0] == col[1]):
                parsed.append(col[0])
            else:
                parsed.append(col[0] + " " +col[1])
        one.columns = [x.strip().replace(' ','_') for x in parsed]
        for x in ['Date','Stage','Home Team','Away Team','Home Goals','Away Goals']:
            one[x.strip().replace(' ','_')] = main.loc[x].item()
        one['Date'] = pd.to_datetime(one['Date'])
        return one

    def _get_match_logs(self, id,year,team):
        print('Scraping',team,'for',year, end='\r', flush=True)
        prefixes = ['shooting','keeper','passing','passing_types','gca','defense','possession','misc']
        starting_url = f'https://fbref.com/en/squads/{id}/{year-1}-{year}/matchlogs/all_comps/'
        dfs = []
        for prefix in prefixes:
            newurl = starting_url+prefix
            tmp = pd.read_html(self.scraper.requests_get(newurl).content)[0]
            cols = tmp.columns.tolist()
            parsed = []
            for col in cols:
                if (len(col[0].split()) > 1):
                    parsed.append(prefix + ' ' +col[1])
                elif (col[0] == col[1]):
                    parsed.append(prefix + ' ' +col[0])
                else:
                    parsed.append(prefix + ' ' + col[0] + " " +col[1])
            tmp.columns = [x.strip().replace(' ','_').title() for x in parsed]
            dfs.append(tmp)
        df = pd.concat(dfs,axis=1)
        df = df.drop([x for x in df.columns.tolist() if ('Notes' in x) | ('Match_Report' in x)],axis=1)
        new_url =starting_url+'schedule'
        tmp = pd.read_html(new_url)[0].drop(['Match Report','Notes'],axis=1,errors='ignore')
        df = tmp.merge(df,how='right',left_on='Date',right_on='Shooting_Date')
        df.insert(1,'Squad',team)
        df = df.replace('Champions Lg','Champions League').replace('Europa Lg','Europa League')
        df = df.drop(df.tail(1).index)
        return df

    def _get_match_level(self, teams, season):#get individual match level data
        def fix_penalty(df):
            penfor = df['GF'].apply(lambda x:str(x).split()[-1].replace('(','').replace(')','') if (len(str(x).split()) >1) else 0)
            penagainst = df['GA'].apply(lambda x:str(x).split()[-1].replace('(','').replace(')','') if (len(str(x).split()) >1) else 0)
            df['GF'] = df['GF'].apply(lambda x:str(x).split('(')[0])
            df['GA'] = df['GA'].apply(lambda x:str(x).split('(')[0])
            df.insert(10,'penfor',penfor)
            df.insert(11,'penagainst',penagainst)
            return df
        dfs = []
        for team in teams:
            tmp  = self._get_match_logs(teams.get(team),season,team)
            dfs.append(tmp)
    
        df = pd.concat(dfs,ignore_index=True)
        df = df.dropna(thresh=35).fillna(0)
        df = df.T.drop_duplicates().T
        df = fix_penalty(df)
        r = df.drop('Date',axis=1).apply(pd.to_numeric,errors='ignore')
        r['Date'] = df['Date']
        return r.drop_duplicates()

    def _parse_columns(self, df):
        oldcols = df.columns.tolist()
        new = [x.replace('#','_No_').replace(' ','_').replace('(','').replace('-','_').replace(')','').replace('%','_Pct_').replace('1/3','One_Third').replace('/',"_per").replace('+','_Plus_').replace(':','_').replace('__','_') for x in oldcols]
        new = [x.strip('_') for x in new]
        df.columns = new
        df = df.T.drop_duplicates().T
        df = df.drop_duplicates()
        df.drop(['Standard_Matches','Standard_Rk','Standard Matches'],axis=1,inplace=True,errors='ignore')
        return df.loc[:, ~df.columns.duplicated()]
    
    def _write_to_bq(self, df, name, dataset, write_type = 'APPEND'): #writes to bigquery, types supported are APPEND or WRITE_TRUNCATE 
        project_id = os.environ.get("GCP_PROJECT_NAME")
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset)
        table = dataset.table(name)
        write_type = bigquery.WriteDisposition.WRITE_TRUNCATE

        if write_type =='APPEND':
            existing_df = client.query(f"SELECT * FROM {project_id}.{dataset.dataset_id}.{table.table_id}").to_dataframe()

            if sorted(existing_df.columns.tolist()) != sorted(df.columns.tolist()):
                print('Columns do not match')
                return
            df = pd.concat([existing_df, df]).reset_index(drop=True).drop_duplicates()
            if len(df) == len(existing_df):
                print('No new data to upload')
                return
            elif df.shape[1] != existing_df.shape[1]:
                print('Columns do not match after concatenation')
                return

        job_config = bigquery.LoadJobConfig(write_disposition = write_type)
        try:
            json_data = df.to_json(orient = 'records')
            json_object = json.loads(json_data)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            job.result()
        except:
            job = client.load_table_from_dataframe(df,table,job_config = job_config)
            job.result()
        print('Uploaded',name, 'successfully of length', len(df))
        table = client.get_table(table)  # Make an API request.
        print(
            "New Length of {} rows and {} columns".format(
                table.num_rows, len(table.schema)
            )
        )
        client.close()
        return
    
    def _find_closest_matches(self, list1, list2):
        matches = {}
        for i in range(len(list1)):
            if list1[i] == list2[i]:
                matches[list1[i]] = list2[i]
        unmatched_list1 = [item for item in list1 if item not in matches]
        unmatched_list2 = [item for item in list2 if item not in matches.values()]
        threshold = 1.0 

        while unmatched_list1:
            closest_match = {}
            for item1 in unmatched_list1:
                for item2 in unmatched_list2:
                    similarity = difflib.SequenceMatcher(None, item1, item2).ratio()
                    if similarity >= threshold and (item1 not in closest_match or similarity > closest_match[item1][0]):
                        closest_match[item1] = (similarity, item2)
            for item1, (_, item2) in closest_match.items():
                matches[item1] = item2
                unmatched_list1.remove(item1)
                unmatched_list2.remove(item2)

            threshold -= 0.1 
        return matches
    
    def _find_team(self, row, mapping):
        log_player = row["Summary_Player"]
        home, away = row["Home_Team"], row["Away_Team"]
        stats_team = mapping[log_player]
        if home in stats_team:
            return home
        else:
            return away
        
    def _posession_adjust(self, row, metric):
        opp_posession = 100 - row["Poss"]
        assert opp_posession >= 0, f"{row['Poss']} is not a valid posession value for metric {metric}"
        return row[metric] / opp_posession * 50
    
    def _find_most_similar_string(self, string, list_of_strings):
        matches = difflib.get_close_matches(string, list_of_strings)
        if len(matches) > 0:
            return matches[0]
        else:
            return None

    

    

if __name__ == '__main__':

    #to scrape 2022-2023 set to 2023
    parser = argparse.ArgumentParser(description='Team Player Scraper')
    parser.add_argument('--start', type=int, help='Start year', default=datetime.date.today().year)
    parser.add_argument('--end', type=int, help='End year', default=datetime.date.today().year)
    parser.add_argument('--leagues', nargs='+', help="Leagues included are for eg ['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga', 'Eredivisie', 'Primeira Liga']", 
                        default=['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga'])
    parser.add_argument('--write_type', type=str, help='Write Type', default='WRITE_TRUNCATE')
    args = parser.parse_args()
    scraper = TeamPlayerScraper(args.start, args.end, args.leagues, args.write_type)
    scraper.scrape()

    # Example Usage
