from datetime import datetime
import pandas as pd 
import requests

class newsET:
    """
    The newsET class is designed to extract and transform news articles related to various cricket leagues
    using the NewsAPI. It provides methods to fetch news articles for specific leagues and transform the data
    into a structured format suitable for analysis.

    Attributes:
        apiSecret (str): The API key for authenticating requests to the NewsAPI.
    """
    
    def __init__(self, apiSecret) -> None:
        self.apiSecret = apiSecret

    def extract(self):
        """
        Extract news articles for predefined cricket leagues from the NewsAPI.

        Returns:
            list: A list of news articles with league information added.
        """
        todayStr = datetime.today().strftime("%Y-%m-%d")
        leagues = ["Indian Premier League", "T20 Blast UK", "Big Bash T20 Australia", 
                   "Pakistan Super League", "Bangladesh Premier League", "SA20 South Africa",
                   "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", 
                   "Major League Cricket"]
        allLeagueArticles = []

        for league in leagues:
            url = ('https://newsapi.org/v2/everything?'
                   f'q={league}&'
                   f'to={todayStr}&'
                   'sortBy=relevancy&'
                   'language=en&'
                   f'apiKey={self.apiSecret}')

            response = requests.get(url)
            articles = response.json()["articles"]
            for article in articles:
                article['League'] = league
            allLeagueArticles.extend(articles)

        return allLeagueArticles
    
    def transform(self):
        """
        Transform the extracted news articles into a structured DataFrame.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the transformed news articles.
        """
        response = self.extract()
        masterDF = pd.DataFrame()

        for r in response:
            if r != []:
                df = pd.json_normalize(r, sep='_')
                df = df[["source_name", "title", "urlToImage", "url", "publishedAt", 'League']]
                df['publishedAt'] = pd.to_datetime(df['publishedAt'])
                df.sort_values(by="publishedAt", ascending=False, inplace=True)
                df = df[~df["urlToImage"].isna()]
                if not df.empty:
                    masterDF = pd.concat([masterDF, df[:5]])

        return masterDF
