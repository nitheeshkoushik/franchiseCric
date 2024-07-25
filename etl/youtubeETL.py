import googleapiclient.discovery
import pandas as pd 

class youtubeET:
    """
    The youtubeET class is designed to extract and transform YouTube video data related to various cricket leagues
    using the YouTube Data API. It provides methods to fetch video data for specific leagues and transform the data
    into a structured format suitable for analysis.

    Attributes:
        apiKey (str): The API key for authenticating requests to the YouTube Data API.
    """

    def __init__(self, apiKey):
        self.apiKey = apiKey

    def extract(self):
        """
        Extract YouTube video data for predefined cricket leagues from the YouTube Data API.

        Returns:
            list: A list of video data with league information added.
        """
        leagues = ["Indian Premier League",
                   "T20 Blast UK", "Big Bash T20 Australia", 
                   "Pakistan Super League", "Bangladesh Premier League", "CSA T20 Challenge",
                   "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", 
                   "Major League Cricket"]
        
        youtube = googleapiclient.discovery.build(
            "youtube", "v3", developerKey=self.apiKey
        )
        
        result = []
        for league in leagues:
            query = league
            search_response = (
                youtube.search()
                .list(
                    part="snippet",
                    maxResults=10,  
                    q=query
                )
                .execute()
            )
            league_response = search_response['items']
            league_response[0]['league'] = league
            result.append(league_response)

        return result

    def transform(self): 
        """
        Transform the extracted YouTube video data into a structured DataFrame.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the transformed video data.
        """
        response = self.extract()
        masterDF = pd.DataFrame()
        
        for r in response:
            df = pd.json_normalize(r)
            df = df[["id.videoId", "snippet.title", "snippet.thumbnails.medium.url"]]
            df.rename({'id.videoId': 'videoID',
                       'snippet.title': 'title',
                       'snippet.thumbnails.medium.url': 'url'}, axis=1, inplace=True)
            df["league"] = r[0]['league'] 
            df = df[~df["videoID"].isna()]
            masterDF = pd.concat([masterDF, df])

        return masterDF
