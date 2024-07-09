from google.cloud import secretmanager, bigquery
import googleapiclient.discovery
import pandas as pd 
import configparser
import os


class youtubeET:

    def getAPISecret(self, project_id = "franchisecric", secret_id = "youtubeAPI", version_id = 1): 
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")


    def extract(self, league):
        apiKey = self.getAPISecret()
        query = league + ' Podcasts'
        youtube = googleapiclient.discovery.build(
            "youtube", "v3", developerKey=apiKey
        )
        search_response = (
        youtube.search()
        .list(
            part = "snippet",
            maxResults=10,  
            q=query
        )
        .execute())
        return search_response['items']
    

    def transform(self, league): 
        response = self.extract(league)
        df = pd.json_normalize(response)
        df = df[["id.videoId", "snippet.title", "snippet.thumbnails.medium.url"]]
        df.rename({'id.videoId' : 'videoID',
                'snippet.title': 'title',
                'snippet.thumbnails.medium.url': 'url'}, axis = 1, inplace = True)
        df["league"] = league 
        df = df[~df["videoID"].isna()]
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
    loadData(master, dataset, youtube_df)