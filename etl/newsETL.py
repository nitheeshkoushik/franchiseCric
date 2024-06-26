from google.cloud import secretmanager, bigquery
from datetime import datetime, timedelta
import pandas as pd 
import requests
import configparser
import os


class newsET:

    def getAPISecret(self, project_id = "franchisecric", secret_id = "newsAPI", version_id = 1): 
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    
    def extract(self, league):
        todayStr = datetime.today().strftime("%Y-%m-%d")
        apiKey = self.getAPISecret()

        url = ('https://newsapi.org/v2/everything?'
        f'q={league}&'
        f'to={todayStr}'
        'language=en&'
        'sortBy=relevancy&'
        f'apiKey={apiKey}')

        response = requests.get(url)
        articles = response.json()["articles"]
        return articles 
    
    def transform(self, league):
        articles = self.extract(league)
        df = pd.json_normalize(articles, sep='_')
        df = df[["source_name", "title", "urlToImage", "url", "publishedAt"]]
        df['publishedAt'] = pd.to_datetime(df['publishedAt'])
        df.sort_values(by="publishedAt", ascending = False)
        df = df[~df["urlToImage"].isna()]
        df["League"] = league
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
    news = newsET()
    leagues = ["Indian Premier League", 
               "T20 Blast UK", "Big Bash T20 Australia", 
               "Pakistan Super League", "Bangladesh Premier League", "CSA T20 Challenge",
               "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League"]
    master = pd.DataFrame()
    for league in leagues: 
        df = news.transform(league)
        master = pd.concat([master, df[:5]],ignore_index= True)
    dataset = parser.get("gcp_bigQuery", "dataset")
    news_df = parser.get("gcp_bigQuery", "news_df")
    loadData(master, dataset, news_df)
