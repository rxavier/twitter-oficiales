import tweepy
import pandas as pd
import json
import plotnine as p9
import datetime as dt

with open("Keys.json", "r") as f:
    keys = json.load(f)

auth = tweepy.OAuthHandler(keys["consumer_token"], keys["consumer_token_secret"])
auth.set_access_token(keys["access_token"], keys["access_token_secret"])
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

USERS = ["SCPresidenciauy", "oppuruguay", "MRREE_Uruguay", "MGAPUruguay", "MDN_Uruguay", "Mvotma_Uruguay", "midesuy",
         "MIEM_Uruguay", "MI_UNICOM", "mef_Uruguay", "Uruguay_Natural", "MEC_Uruguay", "MSPUruguay", "MTSSuy", "mtopuy",
         "ASSEcomunica", "ANEP_Uruguay", "Udelaruy"]


def get_data(user_list, num_items=1000, data=None, save=True, forward=True):

    if data is None:
        data_aux = []
        ids = [None] * len(user_list)
    elif forward is False:
        data_aux = data.values.tolist()
        ids = data.groupby("User").min()["ID"].reindex(user_list).to_list()
    else:
        data_aux = data.values.tolist()
        ids = data.groupby("User").max()["ID"].reindex(user_list).to_list()

    for user, ref_id in zip(user_list, ids):

        if data is None:
            element_cursor = tweepy.Cursor(api.user_timeline, screen_name=user,
                                           tweet_mode="extended").items(num_items)
        elif forward is False:
            element_cursor = tweepy.Cursor(api.user_timeline, screen_name=user,
                                           tweet_mode="extended", max_id=(ref_id - 1)).items(num_items)
        else:
            element_cursor = tweepy.Cursor(api.user_timeline, screen_name=user,
                                           tweet_mode="extended", since_id=ref_id).items(num_items)

        for element in element_cursor:

            if element.full_text.startswith("RT @"):
                text = element.retweeted_status.full_text
                tweet_type = "Retweet"
            elif element.in_reply_to_status_id is not None:
                text = element.full_text
                tweet_type = "Reply"
            else:
                text = element.full_text
                tweet_type = "Tweet"

            data_aux.append([user, text, element.created_at, element.id,
                             element.favorite_count, element.retweet_count, tweet_type])

    output = pd.DataFrame(data_aux)
    output.columns = ["User", "Tweet", "Date", "ID", "Favorites", "Retweets", "Type"]
    output.sort_values(["User", "Date"], inplace=True)

    if save is True:
        output.to_csv("data.csv", sep=" ", index=False)

    return output


def mungle_plot(data_df, users=USERS, aggregation="7D", start="2018-12-31", end=None):

    data = data_df.sort_values(["User", "Date"])
    data.drop_duplicates(subset=["User", "Tweet"], inplace=True)
    data = data.astype({"User": "object", "Tweet": "str", "Date": "datetime64[ns]",
                        "Favorites": "int", "Type": "category"})
    data["Date"] = data["Date"]-dt.timedelta(hours=3)

    filtered_data = data.loc[(data.Type == "Tweet")].loc[data.User.isin(users)]
    data_sums = (filtered_data.groupby("User").
                 apply(lambda x: x.set_index("Date").resample("1D").sum().
                       reindex(pd.date_range(dt.datetime(2018, 12, 30), data.max()["Date"], freq="D"))).
                 drop("ID", axis=1).
                 rename_axis(["User", "Date"]).reset_index())
    data_count = (filtered_data.groupby("User").
                  apply(lambda x: x.set_index("Date").resample("1D").count().
                        reindex(pd.date_range(dt.datetime(2018, 12, 30), data.max()["Date"], freq="D"))).
                  drop(["User", "Favorites", "Retweets", "ID", "Type"], axis=1).
                  rename_axis(["User", "Date"]).reset_index())

    full_data = data_count.merge(data_sums, how="left", on=["User", "Date"])
    resampled_data = (full_data.set_index("Date").groupby("User").
                      resample(aggregation, label="right", closed="right").sum())

    agg_data = resampled_data.groupby("Date").agg("sum").reset_index()

    if end is None:
        end = agg_data.Date.max()

    agg_data = agg_data.loc[(agg_data.Date >= start) & (agg_data.Date <= end)]

    plot = (p9.ggplot(agg_data, p9.aes("Date", "Tweet")) +
            p9.geom_line() + p9.theme(axis_text_x=p9.element_text(rotation=90, hjust=1)))

    return plot, agg_data
