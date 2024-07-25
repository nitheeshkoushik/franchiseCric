import streamlit as st 
import pandas as pd
import requests
from PIL import Image
from io import BytesIO
from google.cloud import bigquery


client = bigquery.Client()

def readData():

    query = f"""
    SELECT *
    FROM `franchisecric.franchiseCricDS.youtubeDF`
    """
    df = client.query_and_wait(query).to_dataframe()
    return df

def pageStructure(df):
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("Podcasts and Videos")
    filter_options = st.selectbox("Select League", df["league"].unique())
    filtered_df = df[df["league"] == filter_options]
    return filtered_df


def strucureAricles(filtered_df):
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
    col1, col2, col3, col4 = st.columns(4)

    try:
        with col1:
            st.image(resized_images[0],width=360,use_column_width=True)
            st.subheader(titles[0])
            # st.markdown(f'{sources[0]}')
            st.markdown(f"[Full Video]({videos[0]})")
            
        with col2:
            st.image(resized_images[1],width=360,use_column_width=True)
            st.subheader(titles[1])
            # st.markdown(f'{sources[1]}')
            st.markdown(f"[Full Video]({videos[1]})")


        with col3:
            st.image(resized_images[2],width=360,use_column_width=True)
            st.subheader(titles[2])
            st.markdown(f"[Full Video]({videos[2]})")

        with col4:
            st.image(resized_images[3],width=360,use_column_width=True)
            st.subheader(titles[3])
            st.markdown(f"[Full Video]({videos[3]})")
    except:
        pass 

if __name__ == "__main__":
    df = readData()
    filtered_df = pageStructure(df)
    resized_images, titles, urls, videos = strucureAricles(filtered_df)
    writeArticles( resized_images, titles, urls, videos)