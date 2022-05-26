from util import get_sites, tag_df, keyword_df
from getstat import STAT

import datetime as dt

import pandas as pd


def main():
    # add key here
    my_api_key = ""
    s = STAT(my_api_key)
    # set the max number of results per response (5000)
    s._set_results(5000)

    # get each of your sites you have access to
    all_sites = get_sites(s)

    # get all tags and keywords for a site
    tag = pd.DataFrame()
    kw = pd.DataFrame()
    for site_id, site_name in all_sites.items():
        # tags
        tag = tag.append(tag_df(s, site_id))
        tag["KeywordCount"] = tag["Keywords"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        # keywords
        keywords = s.keywords(site_id)
        # format kws in df
        kw_df = keyword_df(keywords)
        kw_df["Domain"] = site_name
        # add it back into main df
        kw = kw.append(kw_df)

    # SAVE
    ts = dt.datetime.now().isoformat()
    # Save to tag CSV with current date and time attached
    tag.to_csv(f"./outputs/tags-{ts}.csv")
    # Save to kw CSV with current date and time attached
    kw.to_csv(f"./outputs/kws-{ts}.csv")
