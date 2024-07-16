from google.cloud import secretmanager, bigquery
import googleapiclient.discovery
import pandas as pd 
import configparser
import os


class youtubeET:
    def __init__(self, apiKey):
        self.apiKey = apiKey


    def extract(self):
        leagues = ["Indian Premier League", 
                "T20 Blast UK", "Big Bash T20 Australia", 
                "Pakistan Super League", "Bangladesh Premier League", "CSA T20 Challenge",
                "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", "Major League Cricket"]
        
        youtube = googleapiclient.discovery.build(
                "youtube", "v3", developerKey=self.apiKey
            )
        result = []
        for league in leagues:
            query = league + ' Podcasts'
            search_response = (
            youtube.search()
            .list(
                part = "snippet",
                maxResults=10,  
                q=query
            )
            .execute())
            league_response = search_response['items']
            result.append(league_response)
        return result
    

    def transform(self): 
        response = self.extract(self.apiKey)
        masterDF = pd.DataFrame()
        for r in response:
            df = pd.json_normalize(r)
            df = df[["id.videoId", "snippet.title", "snippet.thumbnails.medium.url"]]
            df.rename({'id.videoId' : 'videoID',
                    'snippet.title': 'title',
                    'snippet.thumbnails.medium.url': 'url'}, axis = 1, inplace = True)
            df["league"] = league 
            df = df[~df["videoID"].isna()]
            masterDF = pd.concat([masterDF, df])
        return masterDF 



if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read("pipeline.config")
    gcp_cred = parser.get("gcp_cred_location", "location")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_cred
    ytET = youtubeET()

    leagues = ["Indian Premier League", 
                "T20 Blast UK", "Big Bash T20 Australia", 
                "Pakistan Super League", "Bangladesh Premier League", "CSA T20 Challenge",
                "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", "Major League Cricket"]
    master = pd.DataFrame()
    for league in leagues: 
        df = ytET.transform(league)
        master = pd.concat([master, df[:5]],ignore_index= True)
    dataset = parser.get("gcp_bigQuery", "dataset")
    youtube_df = parser.get("gcp_bigQuery", "youtube_df")
    # loadData(master, dataset, youtube_df)