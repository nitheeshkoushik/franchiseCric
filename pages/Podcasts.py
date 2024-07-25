import streamlit as st 
import pandas as pd
import requests
from PIL import Image
from io import BytesIO
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

def readData():
    """
    Query YouTube video data from BigQuery and return it as a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing YouTube video data.
    """
    query = """
    SELECT *
    FROM `franchisecric.franchiseCricDS.youtubeDF`
    """
    df = client.query(query).to_dataframe()
    return df

def pageStructure(df):
    """
    Set up the Streamlit page layout and filter options.

    Args:
        df (pd.DataFrame): The DataFrame containing YouTube video data.

    Returns:
        pd.DataFrame: The filtered DataFrame based on user selection.
    """
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("Podcasts and Videos")
    filter_options = st.selectbox("Select League", df["league"].unique())
    filtered_df = df[df["league"] == filter_options]
    return filtered_df

def strucureAricles(filtered_df):
    """
    Process and resize images, titles, URLs, and video links from the filtered DataFrame.

    Args:
        filtered_df (pd.DataFrame): The DataFrame containing filtered YouTube video data.

    Returns:
        tuple: A tuple containing lists of resized images, titles, URLs, and video links.
    """
    resized_images = []
    titles = []
    urls = []
    videos = []
    for index, row in filtered_df.iterrows():
        if len(resized_images) < 5:
            try:
                image = row["url"]
                response = requests.get(image)
                image = Image.open(BytesIO(response.content))
                resized_images.append(image.resize((280, 180)))
                titles.append(row["title"])
                urls.append(row["url"])
                videos.append(f'https://www.youtube.com/watch?v={row["videoID"]}')
            except:
                continue
    return resized_images, titles, urls, videos

def writeArticles(resized_images, titles, urls, videos):
    """
    Display video articles with images, titles, and links in Streamlit columns.

    Args:
        resized_images (list): List of resized video thumbnails.
        titles (list): List of video titles.
        urls (list): List of video URLs (not used in this function).
        videos (list): List of full video links.
    """
    col1, col2, col3, col4 = st.columns(4)

    try:
        with col1:
            st.image(resized_images[0], width=360, use_column_width=True)
            st.subheader(titles[0])
            st.markdown(f"[Full Video]({videos[0]})")
            
        with col2:
            st.image(resized_images[1], width=360, use_column_width=True)
            st.subheader(titles[1])
            st.markdown(f"[Full Video]({videos[1]})")

        with col3:
            st.image(resized_images[2], width=360, use_column_width=True)
            st.subheader(titles[2])
            st.markdown(f"[Full Video]({videos[2]})")

        with col4:
            st.image(resized_images[3], width=360, use_column_width=True)
            st.subheader(titles[3])
            st.markdown(f"[Full Video]({videos[3]})")
    except:
        pass  # Silently handle any exceptions

if __name__ == "__main__":
    df = readData()
    filtered_df = pageStructure(df)
    resized_images, titles, urls, videos = strucureAricles(filtered_df)
    writeArticles(resized_images, titles, urls, videos)
