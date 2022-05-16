# API Wrapper for app.getstat.com
Helps faciliate pulling information from getstat.com as well as gaining access to historical trend data on the keyword level. 

<a href=https://app.getstat.com/>STAT Browswer Portal</a>

<a href=https://help.getstat.com/knowledgebase/requests-and-responses/#apicalls>Base API Documentation</a>

# Instalation
At this time you will need to clone the repo, in the future this will be a pypi project. 

One you have downloaded the repo you can create your own .py file and create the class using the following code. 
```
from stat_api import STAT
stat_wrapper = STAT(YOUR_API_KEY)
```
# Methods
Methods of the STAT class defined here:
```py
__init__(self, api_key: str) -> None
```
STAT class accepts your API key from your app.getstat account

```py
.get_site_ranks(
    self, 
    site_id: Union[int, str], 
    start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
    end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
) -> list
```
gets a ranking distribution for an entire site by ID between the given dates (inclusive)

```py
.get_site_sov(
    self, 
    site_id: Union[int, str], 
    start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
    end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
) -> list
```
returns the Share of Voice for a given site ID over the given date range
```py
.get_sites(self) -> list
```
lists all sites you have access to
```py
.get_tag_ranks(
    self, 
    tag_id: Union[int, str], 
    start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
    end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
) -> list
```
gets a ranking distribution for an entire tag by ID
```py
.get_tag_sov(
    self, 
    tag_id: Union[int, str], 
    start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
    end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
) -> list
```
returns the Share of Voice for a given tag ID over the given date range
```py
.get_tags(self, site_id: str) -> list
```
lists all tags for a site ID
```py
.keyword_ranks(
    self, 
    keyword_id: Union[int, str], 
    start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
    end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
) -> list
```
returns ranking list for a given keyword and date range
```py
.keywords(self, site_id: Union[int, str], raw: bool = False) -> list
```
returns a list of keywords for a given site id
```py
.projects(self)
```
returns all the projects your account has access to
```py
.serp(
    self, 
    keyword_id: Union[int, str], 
    date: datetime.date = datetime.date - dt.timedelta(days=1)
) -> list
```
pulls a SERP for a given keyword ID for a given day (defaults to yesterday)
```py
.subaccounts(self)
```
returns all subaccounts on your account

# Example
See example.py for code
```py
# from stat_api.py and util.py in this module
from stat_api import STAT
from util import get_sites, tag_df, keyword_df
import datetime as dt
# will need to pip install pandas
import pandas as pd

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
    kw_df["Domain"] = site_name
    kw_df = keyword_df(keywords)
    # add it back into main df
    kw = kw.append(kw_df)

# SAVE
ts = dt.datetime.now().isoformat()
# Save to tag CSV with current date and time attached
tag.to_csv(f"./outputs/tags-{ts}.csv")
# Save to kw CSV with current date and time attached
kw.to_csv(f"./outputs/kws-{ts}.csv")

```