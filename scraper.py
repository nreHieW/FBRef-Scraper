import argparse
import json
import datetime
import pandas as pd
import ScraperFC as sfc

class TeamPlayerScraper:
    def __init__(self, start:int, end:int, leagues: list = ['EPL', 'La Liga', 'Serie A','Ligue 1','Bundesliga']) -> None:
        self.start = start
        self.end = end
        self.years = list(range(start, end + 1))
        self.leagues = leagues

    def scrape(self) -> None:
        self.scraper = sfc.FBRef()
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
                self._parse_columns(squad).to_csv(f'./data/Squad_{year}.csv',index=False)
                self._parse_columns(against).to_csv(f'./data/Against_{year}.csv',index=False)
                self._parse_columns(players).to_csv(f'./data/Players_{year}.csv',index=False)
                self._parse_columns(squad_gks).to_csv(f'./data/Squad_GK_{year}.csv',index=False)
                self._parse_columns(against_gks).to_csv(f'./data/Against_GK_{year}.csv',index=False)
                self._parse_columns(players_gks).to_csv(f'./data/Players_GK_{year}.csv',index=False)
                self._parse_columns(shots).to_csv(f'./data/Shots_{year}.csv',index=False)
                self._parse_columns(squad_logs).to_csv(f'./data/Squad_Match_Logs_{year}.csv',index=False)
                self._parse_columns(player_logs).to_csv(f'./data/Player_Match_Logs_{year}.csv',index=False)

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
        new = []
        oldcols = df.columns.tolist()
        new = [x.replace('#','_No_').replace(' ','_').replace('(','').replace('-','_').replace(')','').replace('%','_Pct_').replace('1/3','One_Third').replace('/',"_per").replace('+','_Plus_').replace(':','_').replace('__','_') for x in oldcols]
        df.columns = new
        df = df.T.drop_duplicates().T
        df = df.drop_duplicates()
        df.drop(['Standard_Matches','Standard_Rk','Standard Matches'],axis=1,inplace=True,errors='ignore')
        return df.loc[:, ~df.columns.duplicated()]
    

if __name__ == '__main__':

    #to scrape 2022-2023 set to 2023
    parser = argparse.ArgumentParser(description='Team Player Scraper')
    parser.add_argument('--start', type=int, help='Start year', default=datetime.date.today().year)
    parser.add_argument('--end', type=int, help='End year', default=datetime.date.today().year + 1)
    parser.add_argument('--leagues', nargs='+', help="Leagues included are for eg ['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga', 'Eredivisie', 'Primeira Liga']", 
                        default=['EPL', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga'])
    args = parser.parse_args()
    scraper = TeamPlayerScraper(args.start, args.end, args.leagues)
    scraper.scrape()