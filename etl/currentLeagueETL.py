import requests 
from datetime import datetime
import pandas as pd

class currentETL:
    """
    The currentETL class is designed to extract, transform, and load (ETL) cricket league data 
    using the Cricbuzz API. It provides methods to fetch current leagues, get their standings, 
    and transform the data into a structured format suitable for analysis.

    Attributes:
        apiSecret (str): The API key for authenticating requests to the Cricbuzz API.
    """

    def __init__(self, apiSecret):
        self.apiSecret = apiSecret

    def getCurrentLeagues(self):
        """
        Fetch the current cricket leagues from the Cricbuzz API based on the current date.

        Returns:
            list: A list of current league series if the current date matches, otherwise None.
        """
        url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/league"

        headers = {
            "x-rapidapi-key": self.apiSecret,
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
        """
        Fetch the standings of the current cricket leagues from the Cricbuzz API.

        Returns:
            list: A list of standings data for the current leagues.
        """
        currentSeries = self.getCurrentLeagues()
        headers = {
            "x-rapidapi-key": self.apiSecret, 
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
                
                # Remove the 'teamMatches' key from the points table info
                for teams in result['pointsTable'][0]['pointsTableInfo']: 
                    del teams['teamMatches']
            except:
                continue

        return final


    def transform(self):
        """
        Transform the standings data into a structured DataFrame suitable for analysis.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the transformed standings data.
        """
        result = self.getStandings()

        standings = []
        for r in result:
            league = r['pointsTable'][0]['league']
            for teams in r['pointsTable'][0]['pointsTableInfo']:
                teams['league'] = league

            standings.extend(r['pointsTable'][0]['pointsTableInfo'])

        df = pd.DataFrame(standings)
        df = df[["teamFullName", "matchesPlayed", "matchesWon", "matchesLost", "points", "nrr", "league"]]
        df = df.fillna(0)
        return df
