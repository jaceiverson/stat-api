from wsgiref.handlers import format_date_time
from getstat import STAT
from util import get_sites, keyword_df, serp_df, tag_df

import datetime as dt

import pandas as pd
from googlewrapper import GoogleBigQuery as GBQ

from rich.console import Console

LOG_FILE = open("stat-import.log", "a")
CONSOLE = Console(file=LOG_FILE, log_time_format="%Y-%m-%d %H:%M:%S")


def gbq_import(df: pd.DataFrame, table: str, behavior: str = "append") -> None:
    """send the KW SERP features to GBQ"""
    gbq = GBQ("gbq.json")
    gbq.set_dataset("serp_features")
    gbq.set_table(table)
    gbq.send(df, behavior=behavior)


def clean_tag_table(df: pd.DataFrame, property_name: str) -> pd.DataFrame:
    """clean the tag results to get the tags we want"""
    df = filter_serp_tags(df)
    df["Property"] = property_name
    return df


def filter_serp_tags(df: pd.DataFrame) -> pd.DataFrame:
    """filters the tags to the SERP Features we would like"""
    tags = (
        "answerbox (all)",
        "answerbox (owned)",
        "faq (all)",
        "faq (owned)",
        "indented (all)",
        "indented (owned)",
        "videos (all)",
        "videos (owned)",
    )
    return df.loc[df["Tag"].isin(tags)].copy()


def filter_topic_categories_tags(df: pd.DataFrame) -> pd.DataFrame:
    """filters the tags to the SERP Features we would like"""
    tags = (
        "buying a house",
        "credit",
        "equity & home value",
        "home improvement & maintenance",
        "home warranty",
        "housing market",
        "interior design",
        "location",
        "mortgage",
        "refinance",
        "personal finance",
        "personal loans (transactional)",
        "real estate agents (transactional)",
        "renting",
        "selling a house",
        "types of dwellings",
        "types of mortgages",
    )
    return df.loc[df["Tag"].isin(tags)].copy()


def filter_output_serps(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cols = [
        "Keyword",
        "KeywordDevice",
        "Domain",
        "KeywordTags",
        "SERP Date",
        "Google_Rank",
        "Google_BaseRank",
        "Google_Url",
        "AdvertiserCompetition",
        "GlobalSearchVolume",
        "RegionalSearchVolume",
        "CPC",
        "trend_mar",
        "trend_feb",
        "trend_jan",
        "trend_dec",
        "trend_nov",
        "trend_oct",
        "trend_sep",
        "trend_aug",
        "trend_jul",
        "trend_jun",
        "trend_may",
        "trend_apr",
    ]
    df = df[cols].copy()
    df.columns = [
        "Keyword",
        "KeywordDevice",
        "Domain",
        "KeywordTags",
        "SERP_Date",
        "Google_Rank",
        "Google_BaseRank",
        "Google_Url",
        "AdvertiserCompetition",
        "GlobalSearchVolume",
        "RegionalSearchVolume",
        "CPC",
        "trend_mar",
        "trend_feb",
        "trend_jan",
        "trend_dec",
        "trend_nov",
        "trend_oct",
        "trend_sep",
        "trend_aug",
        "trend_jul",
        "trend_jun",
        "trend_may",
        "trend_apr",
    ]
    df["SERP_Date"] = pd.to_datetime(df["SERP_Date"])
    ranks = df[
        [
            "Keyword",
            "KeywordDevice",
            "Domain",
            "KeywordTags",
            "SERP_Date",
            "Google_Rank",
            "Google_BaseRank",
            "Google_Url",
        ]
    ].copy()
    stats = df[
        [
            "Keyword",
            "AdvertiserCompetition",
            "GlobalSearchVolume",
            "RegionalSearchVolume",
            "CPC",
            "trend_mar",
            "trend_feb",
            "trend_jan",
            "trend_dec",
            "trend_nov",
            "trend_oct",
            "trend_sep",
            "trend_aug",
            "trend_jul",
            "trend_jun",
            "trend_may",
            "trend_apr",
        ]
    ].copy()
    stats = stats.loc[stats.duplicated() == False].copy()
    return ranks, stats


def serp_data(
    s: STAT, df: pd.DataFrame, d: dt.date = dt.date.today() - dt.timedelta(days=1)
) -> dict:
    """
    given a df from .tag_df() find the unique keywords
    as well as the SERP associated with the date passed in.
    Defaults to yesterday
    """
    # find the list of keywords we need to get SERP data for
    unique_keywords = set(df["Keywords"].explode())
    # create the dictionary
    master_keyword_serp = {}
    # loop through each keyword
    for keyword in list(unique_keywords):
        # if the keyword is a string (ignores NAN/None) continue
        if isinstance(keyword, str):
            # get the SERP Dataframe and add it to the dictionary
            temp = serp_df(s, keyword, d)
            master_keyword_serp[keyword] = temp

    return master_keyword_serp


def kws_and_tags(api_key) -> tuple:
    s = STAT(api_key)
    # set the reults to the maximum of 5000 (up from default of 1000)
    s._set_results(5000)
    # pull all the sites we have access to
    all_sites = get_sites(s)

    # get all tags and keywords for a site
    tag = pd.DataFrame()
    kw = pd.DataFrame()
    for site_id, site_name in all_sites.items():
        # TAGS
        tag = tag.append(tag_df(s, site_id))
        tag["KeywordCount"] = tag["Keywords"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        # KEYWORDS
        keywords = s.keywords(site_id)
        # format the kw table
        kw_df = keyword_df(keywords)
        kw_df["Domain"] = site_name
        # add it back into main df
        kw = kw.append(kw_df)

    return tag, kw


def moneytips():
    mt_api = "970447a91ccf73de3c1dbba0710276dc799a421b"


def serp() -> None:
    """main function for pulling the SERP data daily"""
    rocket_api = "970447a91ccf73de3c1dbba0710276dc799a421b"
    tag, kw = kws_and_tags(rocket_api)
    kw1 = kw.loc[kw["Domain"] != "MoneyTips"].copy()
    tag = filter_serp_tags(tag)

    # once we have the data, we need to filter for oclumns we want
    ranks, stats = filter_output_serps(kw)

    # SAVE -- CSV
    ts = dt.date.today() - dt.timedelta(days=1)
    # Save to tag CSV with current date and time attached
    # tag.to_csv(f"./outputs/tags-{ts.isoformat()}.csv")
    # Save to kw CSV with current date and time attached
    # kw.to_csv(f"./outputs/kws-{ts.isoformat()}.csv")
    # CONSOLE.log("Saved tags and kws Locally")

    # SAVE -- GBQ
    gbq_import(ranks, "ranks")
    CONSOLE.log("Saved ranking data to GBQ")
    # each 25th of the month we will update the stats table
    # by the 21st (pulled on the 22nd) the previous month is included
    # we will pull on the 25th just to make sure.
    if ts.day == 25:
        gbq_import(stats, "trends")
        CONSOLE.log("Saved stats data to GBQ")


def topics() -> None:
    rocket_api = "970447a91ccf73de3c1dbba0710276dc799a421b"
    s = STAT(rocket_api)
    s._set_results(5000)
    all_sites = get_sites(s)

    # get all tags and keywords for a site
    tag = pd.DataFrame()
    kw = pd.DataFrame()
    for site_id, site_name in all_sites.items():
        # tags
        temp = filter_topic_categories_tags(tag_df(s, site_id))
        temp["Property"] = site_name
        tag = tag.append(temp)
        tag["KeywordCount"] = tag["Keywords"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        # keywords
        keywords = s.keywords(site_id)
        # FORMAT THE KW TABLE
        kw_df = keyword_df(keywords)
        kw_df["Domain"] = site_name
        # add it back into main df
        kw = kw.append(kw_df)

    # SAVE
    ts = dt.datetime.now().isoformat()
    # Save to tag CSV with current date and time attached
    tag.to_csv(f"./outputs/topic-tags-{ts}.csv")
    # Save to kw CSV with current date and time attached
    kw.to_csv(f"./outputs/topic-kws-{ts}.csv")


def debug() -> None:
    r = 5000
    rocket_api = "970447a91ccf73de3c1dbba0710276dc799a421b"
    site = "18597"
    s = STAT(rocket_api)
    s._set_results(10)
    start_date = dt.date(2022, 5, 10)
    end_date = start_date
    url = s._define_url(
        "/keywords/list",
        f"&site_id={site}&from_date={start_date.isoformat()}&to_date={end_date.isoformat()}",
    )
    kw = s._make_request(url, raw=True)


def all_urls():
    from googlewrapper import GoogleSheets

    rhq = "https://docs.google.com/spreadsheets/d/1mNVyzZTHGdercahdqFTCguLuN9tGeKcwVxWvmTgxjMc/edit#gid=1467003182"
    rm = "https://docs.google.com/spreadsheets/d/1yOW0gG0vDFlm39stw7CZfuy2dIaNQqgDxcQKvnwu8rU/edit#gid=1467003182"
    ql = "https://docs.google.com/spreadsheets/d/1o_GifR2_GsBDWMv60NkgpaC7GVdcWcQOeURfuL2zR_w/edit#gid=1467003182"
    rh = "https://docs.google.com/spreadsheets/d/1MxaLOOicRdpvIfwukGl7-C3hF_xazjduXxKMTOSLaBk/edit#gid=1467003182"

    s = GoogleSheets(rm)
    s.set_tab("STAT Data | All SERP Features")

    df = s.get_df(index=0)
    all_urls = pd.DataFrame(df["URL"].str.split(",").explode("URL"))

    # https://www.geeksforgeeks.org/join-pandas-dataframes-matching-by-substring/
    all_urls.columns = ["SingleUrl"]
    all_urls["joiner"] = 1
    df["joiner"] = 1

    main = df.merge(all_urls, on="joiner").drop("joiner", axis=1)

    all_urls.drop("joiner", axis=1, inplace=True)

    main["match"] = main.apply(lambda x: x.URL.find(x.SingleUrl), axis=1).ge(0)

    final = main[main["match"]].copy()
    final["dups"] = final.duplicated()
    final = final.loc[final["dups"] == False].copy()
    final = final.loc[final["SingleUrl"] != ""].copy()

    s.add_tab("Each URL", final)


if __name__ == "__main__":
    if False:
        CONSOLE.log(f"BEGIN process at {dt.datetime.now()}")
        serp()
        CONSOLE.log(f"END process at {dt.datetime.now()}")
    LOG_FILE.close()
