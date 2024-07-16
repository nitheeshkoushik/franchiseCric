from google.cloud import secretmanager, bigquery
from datetime import datetime, timedelta
import pandas as pd 
import requests
import configparser
import os


class newsET:
    
    def __init__(self, apiSecret) -> None:
        self.apiSecret = apiSecret

    
    def extract(self):
        todayStr = datetime.today().strftime("%Y-%m-%d")
        leagues = ["Indian Premier League",
               "T20 Blast UK", "Big Bash T20 Australia", 
               "Pakistan Super League", "Bangladesh Premier League",  "SA20 South Africa",
               "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", "Major League Cricket"]
        allLeagueArticles = []
        for league in leagues:
            url = ('https://newsapi.org/v2/everything?'
            f'q={league}&'
            f'to={todayStr}&'
            'language=en&'
            f'apiKey={self.apiSecret}')

            response = requests.get(url)
            articles = response.json()["articles"]
            for article in articles:
                article['League'] = league
            allLeagueArticles.extend(articles)
        return allLeagueArticles
    
    def transform(self):
        response = self.extract()
        masterDF = pd.DataFrame()
        for r in response:
            if r != []:
                df = pd.json_normalize(r, sep='_')
                df = df[["source_name", "title", "urlToImage", "url", "publishedAt", 'League']]
                df['publishedAt'] = pd.to_datetime(df['publishedAt'])
                df.sort_values(by="publishedAt", ascending = False)
                df = df[~df["urlToImage"].isna()]
                masterDF = pd.concat([masterDF, df])
        return masterDF 
    



if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read("pipeline.config")
    gcp_cred = parser.get("gcp_cred_location", "location")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_cred
    news = newsET()
    leagues = ["Indian Premier League", 
               "T20 Blast UK", "Big Bash T20 Australia", 
               "Pakistan Super League", "Bangladesh Premier League",  "SA20 South Africa",
               "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", "Major League Cricket"]
    master = pd.DataFrame()
    for league in leagues: 
        df = news.transform(league)
        if df is not None:
            master = pd.concat([master, df[:5]],ignore_index= True)
    dataset = parser.get("gcp_bigQuery", "dataset")
    news_df = parser.get("gcp_bigQuery", "news_df")
    # loadData(master, dataset, news_df)
