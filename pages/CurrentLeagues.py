import streamlit as st 
from PIL import Image
from io import BytesIO
import requests
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

def readData():
    """
    Query data from BigQuery and return it as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing current standings data.
    """
    query = """
    SELECT *
    FROM `franchisecric.franchiseCricDS.currentStandingsDF`
    """
    df = client.query(query).to_dataframe()
    return df

def pageStructure(df):
    """
    Set up the Streamlit page layout and filter options.

    Args:
        df (pd.DataFrame): The DataFrame containing the standings data.

    Returns:
        pd.DataFrame: The filtered DataFrame based on user selection.
    """
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("Leagues happening this month")
    filter_options = st.selectbox("Select League", df["league"].unique())
    filtered_df = df[df["league"] == filter_options].reset_index()
    return filtered_df

def pointsTable(filtered_df):
    """
    Display a table of the filtered standings data, sorted by points and net run rate.

    Args:
        filtered_df (pd.DataFrame): The DataFrame containing the filtered standings data.
    """
    newDF = filtered_df.sort_values(by=['points', 'nrr'], ascending=[False, False]).reset_index()
    newDF = newDF[['teamFullName', 'matchesPlayed', 'matchesWon', 'matchesLost', 'points', 'nrr']]
    newDF = newDF.rename({'teamFullName': 'Team', 
                          'matchesPlayed': 'Played', 
                          'matchesWon': 'Won', 
                          'matchesLost': 'Lost', 
                          'points': 'Points', 
                          'nrr': 'Run Rate'}, axis=1)
    newDF.index = newDF.index + 1

    st.table(newDF)

if __name__ == "__main__":
    df = readData()
    filtered_df = pageStructure(df)
    pointsTable(filtered_df)
