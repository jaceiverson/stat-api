from getstat import STAT

import datetime as dt
import json

import pandas as pd


def save(file: dict, filename: str):
    with open(filename, "w") as f:
        json.dump(file, f)


def get_sites(s: STAT) -> dict:
    """gets a dictionary of ID:SiteName"""
    return {site["Id"]: site["Title"] for site in s.get_sites()}


def tag_df(s: STAT, id: str) -> pd.DataFrame:
    """gets a table of the tags"""
    df = pd.DataFrame(s.get_tags(id))
    df["Keywords"] = df["Keywords"].apply(pd.Series)["Id"]
    return df


def keyword_df(kw: list) -> pd.DataFrame:
    """given a list of kws, parses the dictionary style data and creates tabular rows"""

    # create the df
    kw_df = pd.DataFrame(kw)

    # Expand KeywordRanking column (dictionary values)
    rank = kw_df["KeywordRanking"].apply(pd.Series)
    google = rank["Google"].apply(pd.Series)
    google.columns = [f"Google_{x}" for x in google.columns]
    kw_df["SERP Date"] = pd.to_datetime(rank["date"])
    kw_df[google.columns] = google
    kw_df[["Google_BaseRank", "Google_Rank"]] = kw_df[
        ["Google_BaseRank", "Google_Rank"]
    ].astype(float)

    # Expand KeywordStats column (dicitonary values)
    stats = kw_df["KeywordStats"].apply(pd.Series)
    stats_columns = [
        "AdvertiserCompetition",
        "GlobalSearchVolume",
        "RegionalSearchVolume",
        "CPC",
    ]
    kw_df[stats_columns] = stats[stats_columns].astype(float)

    # Expand the local search trends from KeywordStats column above (dictionary values)
    local_search_trends = stats["LocalSearchTrendsByMonth"].apply(pd.Series)
    local_search_trends.columns = [
        f"trend_{x.lower()}" for x in local_search_trends.columns
    ]
    kw_df[local_search_trends.columns] = local_search_trends.astype(int)

    # Expand KeywordTags into one column per tag
    tags = kw_df["KeywordTags"].str.split(",", expand=True)
    tags.columns = [f"Tag_{x}" for x in tags.columns]
    kw_df[tags.columns] = tags

    return kw_df


def serp_df(
    s: STAT, keyword: str, date: dt.date = dt.date.today() - dt.timedelta(days=1)
) -> pd.DataFrame:
    """calls the serp API and formats as a pd.DataFrame"""
    serp = s.serp(keyword, date)
    df = pd.DataFrame(serp)
    serp_features = df["ResultTypes"].apply(pd.Series)
    serp_features = serp_features["ResultType"].apply(pd.Series)
    serp_features.columns = [f"serp_feature_{x}" for x in serp_features.columns]
    df[serp_features.columns] = serp_features
    return df
