from google.cloud import secretmanager, bigquery
import pandas as pd 
import configparser
import os
import requests

class standingET:

    
    def extract(self):
        rapidAPIKey = self.getAPISecret()
        url = "https://livescore6.p.rapidapi.com/leagues/v2/get-table"
        leagues = {"india" : "ipl", 
                "england": "vitality-blast", 
                "australia" : "big-bash-league", 
                "pakistan" : "super-league",
                "bangladesh" : "bangladesh-premier-league",
                "south-africa": "sa20-league",
                "sri-lanka": "lanka-premier-league",
                "new-zealand": "super-smash",
                "usa": "major-league-cricket"}




        headers = {
            "X-RapidAPI-Key": rapidAPIKey,
            "X-RapidAPI-Host": "livescore6.p.rapidapi.com"
        }
        result = []
        for league in leagues:
            querystring = {"Category":"cricket", "Ccd": league, "Scd": leagues[league]}
            response = requests.get(url, headers=headers, params=querystring)
            result.append(response.json())

        return result

    def transform(self):
        result = self.extract()
        master = pd.DataFrame()
        for league in result:
            lists = league["LeagueTable"]['L'][0]['Tables'][0]['team']
            df = pd.DataFrame(lists)
            df = df[['rnk', 'Tnm','win', 'lst', 'pts', 'drw', 'nrr']]
            df['League'] = league['Snm']
            master = pd.concat([master, df],ignore_index= True)
        return master
    


if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read("pipeline.config")
    gcp_cred = parser.get("gcp_cred_location", "location")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_cred
    standing = standingET()

    df = standing.transform()
    dataset = parser.get("gcp_bigQuery", "dataset")
    standings_df = parser.get("gcp_bigQuery", "standings_df")
    # loadData(df, dataset, standings_df)