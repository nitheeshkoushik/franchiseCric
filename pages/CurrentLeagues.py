import streamlit as st 
from PIL import Image
from io import BytesIO
import requests
from google.cloud import bigquery



client = bigquery.Client()

def readData():
    query = f"""
    SELECT *
    FROM `franchisecric.franchiseCricDS.currentStandingsDF`
    """
    df = client.query_and_wait(query).to_dataframe()
    return df

def pageStructure(df):
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("Leagues happening this month")
    filter_options = st.selectbox("Select League", df["league"].unique())
    filtered_df = df[df["league"] == filter_options].reset_index()
    return filtered_df

def pointsTable(filtered_df):

    newDF = filtered_df.sort_values(by = ['points', 'nrr'], ascending = False)
    newDF = newDF[['teamFullName', 'matchesPlayed', 'matchesWon', 'matchesLost', 'points', 'nrr']]
    newDF = newDF.rename({'teamFullName': 'Team', 
                            'matchesPlayed': 'Played', 
                            'matchesWon': 'Won', 
                            'matchesLost': 'Lost', 
                            'points': 'Points', 
                            'nrr': 'Run Rate'}, axis = 1)
    newDF.index = newDF.index + 1

    st.table(newDF)

if __name__ == "__main__":
    df = readData()
    filtered_df = pageStructure(df)
    pointsTable(filtered_df)