import requests 
import configparser
import os
from google.cloud import secretmanager, bigquery
from datetime import datetime
import pandas as pd

class currentETL:

    def getAPISecret(self, project_id = "franchisecric", secret_id = "rapidAPI", version_id = 1): 
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")



    def getCurrentLeagues(self, apiSecret = None):

        url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/league"

        headers = {
            "x-rapidapi-key": '407eeb0cd0mshf7a0bbb38613a5ep120c0ejsnf4c4c1943706',
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
            "x-rapidapi-key": '407eeb0cd0mshf7a0bbb38613a5ep120c0ejsnf4c4c1943706', 
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



def loadData(df, dataset, table):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )

    job.result()
    return None
        
if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read("pipeline.config")
    gcp_cred = parser.get("gcp_cred_location", "location")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_cred
    current = currentETL()

    df = current.transform()
    dataset = parser.get("gcp_bigQuery", "dataset")
    current_df = parser.get("gcp_bigQuery", "current_df")
    loadData(df, dataset, current_df)