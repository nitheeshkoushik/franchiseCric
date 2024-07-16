import requests 
import configparser
import os
from google.cloud import secretmanager, bigquery
from datetime import datetime
import pandas as pd

class currentETL:


    def getCurrentLeagues(self, apiSecret = None):

        url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/league"

        headers = {
            "x-rapidapi-key": apiSecret,
            "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers)
        result = response.json()['seriesMapProto']
        current_date = datetime.now()
        formatted_date = current_date.strftime('%B %Y').upper()

        for r in result:
            if r['date'] == formatted_date:
                
                return r['series']
    

    def getStandings(self):

        apiSecret = self.getAPISecret()
        currentSeries = self.getCurrentLeagues(apiSecret)
        headers = {
            "x-rapidapi-key": apiSecret, 
            "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
        }
        final = []

        for series in currentSeries:
            url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/series/{series['id']}/points-table"
            response = requests.get(url, headers=headers)
            try:
                result = response.json()
                result['pointsTable'][0]['league'] = series['name']
                final.append(result)
                for teams in result['pointsTable'][0]['pointsTableInfo']: 
                    del teams['teamMatches']
            except:
                continue

        return final


    def transform(self):
        result = self.getStandings()

        standings = []
        for r in result:
            league = r['pointsTable'][0]['league']
            for teams in  r['pointsTable'][0]['pointsTableInfo']:
                teams['league'] = league

            standings.extend(r['pointsTable'][0]['pointsTableInfo'])

            
        df = pd.DataFrame(standings)
        df = df[["teamFullName", "matchesPlayed", "matchesWon","matchesLost","points","nrr", "league"]]
        df = df.fillna(0)
        return df


        
if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read("pipeline.config")
    gcp_cred = parser.get("gcp_cred_location", "location")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_cred
    current = currentETL()

    df = current.transform()
    dataset = parser.get("gcp_bigQuery", "dataset")
    current_df = parser.get("gcp_bigQuery", "current_df")
    # loadData(df, dataset, current_df)