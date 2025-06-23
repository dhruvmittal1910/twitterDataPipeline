# access twitter api here and do some basic etl here
import pandas as pd
import requests
import json
from datetime import datetime
import s3fs

def run_twitter_etl():
        # Define your Twitter API credentials
    apikey="fb7318f7402b47e0bbf86697545b5f9c"

    url = "https://api.twitterapi.io/twitter"

    headers = {"X-API-Key": apikey}

    params = {"userName": "elonmusk"}

    tweets=requests.request("GET",url+"/user/last_tweets",headers=headers, params=params)
    tweets_data=tweets.json()
    # print(tweets_data.keys())
    tweets_list=[]


    for tweet in tweets_data:
        print(tweet)
        refined_tweet={
            "user":tweet.user,
            "text":tweet.text,
            "created_at":tweet.created_at,
            "favorite_count":tweet.favorite_count,
            "retweet_count":tweet.retweet_count
        }

        tweets_list.append(refined_tweet)

    # converting to dataframe
    df=pd.DataFrame(tweets_list)

    df.to_csv("s3://dhruv-airflow-twitter-bucket/tweets.csv")
    # save this df to s3 bucket


