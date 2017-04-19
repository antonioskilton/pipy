from sqlalchemy import create_engine,  MetaData, Table, insert
import pandas as pd

engine = create_engine("mysql+mysqldb://dbBambooDev:BigBamboo99@127.0.0.1:3306/twitter")

connection = engine.connect()

stmt = 'SELECT id, source FROM twitter.member_tweets WHERE screen_name = "realDonaldTrump" AND membership = "trump-and-aides" AND id > (SELECT MAX(id) FROM twitter.trump_source_context) ORDER BY id'

results = connection.execute(stmt).fetchall()

df = pd.DataFrame(results).rename(columns = {0: "id", 1: "source"})

metadata = MetaData()

trump_source_context = Table('trump_source_context', metadata, autoload = True, autoload_with = engine)

# Twitter
import tweepy

consumer_key = "fVI4Qlicl4VQoIa36Kr4fcPSo"
consumer_secret = "RMQCJY9ghYuu2QvQpfpUfOZBtd952kU1tyrO1twpJXP9ispcSL"

access_token = "815193936541007873-grxktQRp7Kyq91Bq0KCzW4EvmNYgowJ"
access_token_secret = "pss2OalXUXULpiyHJGxb5FzApwqEGUFF6SCZNsfKVJdZ3"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

for row in df.iterrows():
    tweet = str(row[1][1]) + ' #trumpdevice' + ' https://twitter.com/realDonaldTrump/status/' + str(row[1][0])
    print tweet

    # once we have sent tweet, store id of trump's original tweet so we don't tweet about it again
    stmt2 = insert(trump_source_context).values(id = row[1][0])
    print stmt2
    result_proxy = connection.execute(stmt2)
    api.update_status(status = tweet)
