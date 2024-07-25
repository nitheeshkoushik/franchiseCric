import streamlit as st 
from PIL import Image
from io import BytesIO
import requests
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

def readData():
    """
    Query standings data from BigQuery and return it as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing standings data.
    """
    query = """
    SELECT *
    FROM `franchisecric.franchiseCricDS.standingsDF`
    """
    df = client.query(query).to_dataframe()
    return df

def pageStructure(df):
    """
    Set up the Streamlit page layout and filter options.

    Args:
        df (pd.DataFrame): The DataFrame containing standings data.

    Returns:
        pd.DataFrame: The filtered DataFrame based on user selection.
    """
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("Popular Leagues' Standings")
    filter_options = st.selectbox("Select League", df["League"].unique())
    filtered_df = df[df["League"] == filter_options].reset_index()
    return filtered_df

def pointsTable(filtered_df):
    """
    Display the standings table sorted by points and net run rate.

    Args:
        filtered_df (pd.DataFrame): The DataFrame containing filtered standings data.
    """
    # Sort the DataFrame by points and net run rate in descending order
    newDF = filtered_df.sort_values(by=['pts', 'nrr'], ascending=[False, False]).reset_index()
    # Select and rename columns for better readability
    newDF = newDF[['Tnm', 'win', 'lst', 'pts', 'nrr']]
    newDF = newDF.rename({'Tnm': 'Team', 
                          'win': 'Won', 
                          'lst': 'Lost', 
                          'pts': 'Points', 
                          'nrr': 'Run Rate'}, axis=1)
    # Adjust index to start from 1
    newDF.index = newDF.index + 1

    st.table(newDF)

if __name__ == "__main__":
    df = readData()
    filtered_df = pageStructure(df)
    pointsTable(filtered_df)
