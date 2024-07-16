from utils.apiSecret import GetAPISecret
from utils.loader import Loader
from utils.getConfig import GetConfig

from et.currentLeagueETL import currentETL
from et.newsETL import newsET
from et.standingsETL import standingET
from et.youtubeETL import youtubeET

import os

def main():
    parser = GetConfig()

    gcp_cred = parser.getConfig("gcp_cred_location", "location")

    dataset = parser.getConfig("gcp_bigQuery", "dataset")
    standings_df_loc = parser.getConfig("gcp_bigQuery", "standings_df")
    youtube_df_loc = parser.getConfig("gcp_bigQuery", "youtube_df")
    news_df_loc = parser.getConfig("gcp_bigQuery", "news_df")
    current_df_loc = parser.getConfig("gcp_bigQuery", "current_df")


    rapidSecLoc = parser.getConfig("gcp_secrets_loc", "rapid_api")
    youtubeSecLoc = parser.getConfig("gcp_secrets_loc", "youtube_api")
    newsSecLoc = parser.getConfig("gcp_secrets_loc", "news_api")


    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_cred

    youtubeAPI = GetAPISecret(youtubeSecLoc)
    youtubeAPISecret = youtubeAPI.getAPISecret()

    youtube_et = youtubeET(youtubeAPISecret)
    youtube_df = youtube_et.transform()

    newsAPI = GetAPISecret(newsSecLoc)
    newsAPISecret = newsAPI.getAPISecret()

    news_et = newsET(newsAPISecret)
    news_df = news_et.transform()

    rapidAPI = GetAPISecret(rapidSecLoc)
    rapidAPISec = rapidAPI.getAPISecret()

    popularLeague = standingET(rapidAPISec)
    popularLeague_df = popularLeague.transform()

    currentLeague = currentETL(rapidAPISec)
    currentLeague_df = currentLeague.transform()

    loader = Loader()

    loader.loadData(youtube_df, dataset, youtube_df_loc)
    loader.loadData(news_df, dataset, news_df_loc)
    loader.loadData(popularLeague_df, dataset, standings_df_loc)
    loader.loadData(currentLeague_df, dataset, current_df_loc)



if __name__ == "__main__":
    main()





