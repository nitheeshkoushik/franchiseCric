import pandas as pd 
import requests

class standingET:
    """
    The standingET class is designed to extract and transform cricket league standings data
    using the Livescore API. It provides methods to fetch standings for various cricket leagues
    and transform the data into a structured format suitable for analysis.

    Attributes:
        apiSecret (str): The API key for authenticating requests to the Livescore API.
    """

    def __init__(self, apiSecret):
        self.apiSecret = apiSecret

    def extract(self):
        """
        Extract standings data for predefined cricket leagues from the Livescore API.

        Returns:
            list: A list of standings data for the current leagues.
        """
        rapidAPIKey = self.apiSecret
        url = "https://livescore6.p.rapidapi.com/leagues/v2/get-table"
        leagues = {
            "india": "ipl", 
            "england": "vitality-blast", 
            "australia": "big-bash-league", 
            "pakistan": "super-league",
            "bangladesh": "bangladesh-premier-league",
            "south-africa": "sa20-league",
            "sri-lanka": "lanka-premier-league",
            "new-zealand": "super-smash",
            "usa": "major-league-cricket"
        }

        headers = {
            "X-RapidAPI-Key": rapidAPIKey,
            "X-RapidAPI-Host": "livescore6.p.rapidapi.com"
        }

        result = []
        for league in leagues:
            querystring = {"Category": "cricket", "Ccd": league, "Scd": leagues[league]}
            response = requests.get(url, headers=headers, params=querystring)
            result.append(response.json())

        return result

    def transform(self):
        """
        Transform the extracted standings data into a structured DataFrame.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the transformed standings data.
        """
        result = self.extract()
        master = pd.DataFrame()

        for league in result:
            lists = league["LeagueTable"]['L'][0]['Tables'][0]['team']
            df = pd.DataFrame(lists)
            df = df[['rnk', 'Tnm', 'win', 'lst', 'pts', 'drw', 'nrr']]
            df['League'] = league['Snm']
            master = pd.concat([master, df], ignore_index=True)

        return master
