import argparse
import requests
import json
import time
from typing import Optional

BASE_URL = "https://www.reddit.com"

HEADERS = {
    "User-Agent": "cyberlang reaserach",
}

RATELIMIT_RESET = 0
"""Seconds until the quota resets, so that we can query again"""


def retry(fn):
    def wrapper(self, *args, **kwargs):
        resp = fn(self, *args, **kwargs)

        while resp.status_code != 200 and self.retries < self.max_retries:
            if resp.status_code == 409:  # too many requests
                # TODO sleep based on ratelimit header
                # time.sleep(6000)
                print("Waiting")

            resp = fn(self, *args, **kwargs)

            self.retries += 1

        if self.retries == self.max_retries:
            json.dump(resp.json(), self.raw_file, indent=True)

            for post in resp.json()["data"]["children"]:
                print(time.ctime(post["data"]["created"]))
                self.time_file.write(time.ctime(post["data"]["created"]) + "\n")

            print("retries exceeded leaving")
            exit()
        else:
            self.retries = 0

        return resp

    return wrapper


class scraper_2000:
    def __init__(
        self, subreddit: str, query: str, max: int, max_retries: int, end: str = None
    ) -> None:
        self.subreddit = subreddit
        self.query = query
        self.max = max
        self.end = end
        self.max_retries = max_retries
        self.count = 0
        self.retries = 0

        self.raw_file = open(f"./{subreddit}-{query}-out.json", "+w")
        self.time_file = open(f"./{subreddit}-{query}-time.txt", "+w")

    @retry
    def subreddit_search(
        self,
        q: str,
        sub_reddit: str,
        after: str = None,
        sort: str = "new",
        limit: int = 10,
        restrict_sr: bool = True,
    ):
        url = BASE_URL + "/r/" + sub_reddit + "/search.json"
        params = {"q": q, "sort": sort, "limit": limit, "restrict_sr": restrict_sr}

        if after != None:
            params["after"] = after

        resp = requests.get(url, params=params, headers=HEADERS)
        if resp.status_code == 200:
            self.count = self.count + int(resp.json()["data"]["dist"])

        return resp

    def resp_err(self, resp: requests.Response):
        print(
            f"ERROR: url {resp.url}, status {resp.status_code}, content {resp.content}"
        )
        exit()

    def run(self):
        if self.end is not None:
            curr_after = self.end
        else:
            # First get a starting point the most recent post
            resp = self.subreddit_search(self.query, self.subreddit, limit=1)

            if resp.status_code != 200:
                self.resp_err()

            json.dump(resp.json(), self.raw_file, indent=True)
            item = resp.json()["data"]["children"][0]

            print(time.ctime(item["data"]["created"]))
            self.time_file.write(time.ctime(item["data"]["created"]) + "\n")

            curr_after = item["data"]["name"]

        while self.count < self.max:
            resp = self.subreddit_search(
                self.query, self.subreddit, after=curr_after, limit=100
            )

            json.dump(resp.json(), self.raw_file, indent=True)

            for post in resp.json()["data"]["children"]:
                print(time.ctime(post["data"]["created"]))
                self.time_file.write(time.ctime(post["data"]["created"]) + "\n")

            item = resp.json()["data"]["children"][0]
            curr_after = item["data"]["name"]

        self.raw_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--subreddit",
        "-s",
        metavar="SUBREDDIT_NAME",
        help="Subreddit to get data from",
        default="unix",
    )
    parser.add_argument("--query", "-q", help="Search term", default="unix")
    parser.add_argument("--max", "-m", help="Max ???", default=500, type=int)
    parser.add_argument("--retries", "-r", help="Max retries", default=5, type=int)
    parser.add_argument(
        "--end",
        "-e",
        metavar="ID",
        help="ID of the last post, to search from backwards",
        default=None,
    )
    args = parser.parse_args()
    scrapy = scraper_2000(
        args.subreddit, args.query, max=args.max, max_retries=args.retries, end=args.end
    )
    scrapy.run()
