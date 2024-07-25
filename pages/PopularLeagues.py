import streamlit as st 
from PIL import Image
from io import BytesIO
import requests
from google.cloud import bigquery



client = bigquery.Client()

def readData():
    query = f"""
    SELECT *
    FROM `franchisecric.franchiseCricDS.standingsDF`
    """
    df = client.query_and_wait(query).to_dataframe()
    return df

def pageStructure(df):
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("Popular Leagues' Standings")
    filter_options = st.selectbox("Select League", df["League"].unique())
    filtered_df = df[df["League"] == filter_options].reset_index()
    return filtered_df

def pointsTable(filtered_df):

    newDF = filtered_df.sort_values(by = ['pts', 'nrr'], ascending = [False, False]).reset_index()
    newDF = newDF[['Tnm','win','lst','pts','nrr']]
    newDF = newDF.rename({'Tnm': 'Team', 
                          'matchesPlayed': 'Played', 
                          'win': 'Won', 
                          'lst': 'Lost', 
                          'pts': 'Points', 
                          'nrr': 'Run Rate'}, axis = 1)
    newDF.index = newDF.index + 1

    st.table(newDF)

if __name__ == "__main__":
    df = readData()
    filtered_df = pageStructure(df)
    pointsTable(filtered_df)