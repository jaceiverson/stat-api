import json
import datetime as dt
from typing import Union, Optional

import requests

from rich.console import Console


def open_close_log_file(func):
    """
    opens log file prior to running the function, and then closes it after the fact
    """

    def wrapper(*args, **kwargs):
        args[0]._open_log_file()
        func(*args, **kwargs)
        args[0]._close_log_file()

    return wrapper


class STAT:
    def __init__(self, api_key: str) -> None:
        """STAT class accepts your API key from your app.getstat account"""
        self.API_KEY = api_key
        self.start = 0
        self.results = 1000
        self.engine = "google"
        self._open_log_file()
        self.CONSOLE = Console(file=self.log_file)

    def _open_log_file(self) -> None:
        self.log_file = open("stat-url.log", "a")

    def _close_log_file(self) -> None:
        self.log_file.close()

    def _set_start(self, start: int):
        """sets the starting point of the API requests"""
        self.start = start

    def _set_results(self, results: int):
        """changes the results per page you'd like to get"""
        self.results = results

    def _set_search_engine(self, engine: str):
        """sets the desired search engine, defaults to google"""
        self.engine = engine

    def _define_url(self, sub_string: str, parameters=None):
        """helper class to create the URL we will be requesting to"""
        if parameters is None:
            parameters = ""
        return (
            f"http://app.getstat.com/api/v2/{self.API_KEY}{sub_string}"
            f"?format=json&start={self.start}&results={self.results}{parameters}"
        )

    def check_for_more_data(self, request_data: dict):
        """checks if there is need to make additional requests"""
        return request_data["Response"].get("nextpage")

    @open_close_log_file
    def _make_request(
        self, url: str, response: Optional[list] = None, raw: bool = False
    ) -> list:
        """
        makes the HTTP request to the url provided
        will validate the status code starts with 2 before sending back
        if not a valid class 200 response, the response object is sent back
        """
        self.CONSOLE.log(url)
        if response is None:
            response = []

        r = requests.get(url)
        # if we have a class 200 status code
        if str(r.status_code).startswith("2"):
            if raw:
                return json.loads(r.text)
            # save the results to the master list
            response += json.loads(r.text)["Response"]["Result"]
            # if there is another request needed to get all the data call the next_request URL
            if next_request := self.check_for_more_data(json.loads(r.text)):
                url = f"http://app.getstat.com/api/v2/{self.API_KEY}{next_request}"
                return self._make_request(url, response)
        else:
            return response

        return response

    def _sov(
        self,
        tag_or_sites: str,
        id: Union[int, str],
        start_date: dt.date,
        end_date: dt.date,
    ) -> list:
        """main function for pulling tag/site Share of Voice"""
        url = self._define_url(
            f"/{tag_or_sites}/sov",
            f"&id={id}&from_date={start_date.isoformat()}&to_date={end_date.isoformat()}",
        )
        return self._make_request(url)

    def _rank(
        self,
        tag_or_sites: str,
        id: Union[int, str],
        start_date: dt.date,
        end_date: dt.date,
    ) -> list:
        """main function for pulling tag/site ranking distributions"""
        url = self._define_url(
            "/{tag_or_sites}/ranking_distributions",
            f"&id={id}&from_date={start_date.isoformat()}&to_date={end_date.isoformat()}",
        )
        return self._make_request(url)

    def get_sites(self) -> list:
        """lists all sites you have access to"""
        url = self._define_url(sub_string="/sites/all")
        return self._make_request(url)

    def get_tags(self, site_id: str) -> list:
        """lists all tags for a site ID"""
        url = self._define_url("/tags/list", f"&site_id={site_id}")
        return self._make_request(url)

    def get_site_sov(
        self,
        site_id: Union[int, str],
        start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
        end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
    ) -> list:
        """returns the Share of Voice for a given site ID over the given date range"""
        return self._sov("sites", site_id, start_date, end_date)

    def get_tag_sov(
        self,
        tag_id: Union[int, str],
        start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
        end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
    ) -> list:
        """returns the Share of Voice for a given tag ID over the given date range"""
        return self._sov("tag", tag_id, start_date, end_date)

    def get_site_ranks(
        self,
        site_id: Union[int, str],
        start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
        end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
    ) -> list:
        """gets a ranking distribution for an entire site by ID"""
        return self._rank("sites", site_id, start_date, end_date)

    def get_tag_ranks(
        self,
        tag_id: Union[int, str],
        start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
        end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
    ) -> list:
        """gets a ranking distribution for an entire tag by ID"""
        return self._rank("tag", tag_id, start_date, end_date)

    def serp(
        self,
        keyword_id: Union[int, str],
        date: dt.date = dt.date.today() - dt.timedelta(days=1),
        raw: bool = False,
    ) -> list:
        """pulls a SERP for a given keyword ID for a given day (defaults to yesterday)"""
        url = self._define_url(
            "/serps/show",
            f"&keyword_id={keyword_id}&engine={self.engine}&date={date.isoformat()}",
        )
        return self._make_request(url, raw=raw)

    def keyword_ranks(
        self,
        keyword_id: Union[int, str],
        start_date: dt.date = dt.date.today() - dt.timedelta(days=31),
        end_date: dt.date = dt.date.today() - dt.timedelta(days=1),
    ) -> list:
        """returns ranking list for a given keyword and date range"""
        url = self._define_url(
            "/rankings/list",
            f"keyword_id={keyword_id}&from_date={start_date.isoformat()}&to_date={end_date.isoformat()}",
        )
        return self._make_request(url)

    def keywords(
        self,
        site_id: Union[int, str],
        raw: bool = False,
    ) -> list:
        """returns a list of keywords for a given site id"""
        url = self._define_url("/keywords/list", f"&site_id={site_id}")
        return self._make_request(url, raw=raw)

    def projects(self):
        """returns all the projects your account has access to"""
        url = self._define_url("/projects/list")
        return self._make_request(url, raw=True)

    def subaccounts(self):
        """returns all subaccounts on your account"""
        url = self._define_url("/subaccounts/list")
        return self._make_request(url, raw=True)
