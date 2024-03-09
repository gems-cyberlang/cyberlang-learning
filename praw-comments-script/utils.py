from typing import Optional

import configparser
import praw


def init_reddit() -> praw.Reddit:
    # Copied from CommentCollector in main.py made by Oliver
    config = configparser.ConfigParser()
    config.read("config.ini")

    reddit_config = config["REDDIT"]

    return praw.Reddit(
        client_id=reddit_config["app_uid"],
        client_secret=reddit_config["app_secret"],
        username=reddit_config["user_name"],
        password=reddit_config["user_pass"],
        user_agent=reddit_config["user_agent"],
    )


def search(
    reddit_or_sr: praw.Reddit | praw.models.Subreddit,
    query: str,
    *,
    sort: str = "relevance",
    limit: int = 100,
    before: Optional[str] = None,
    after: Optional[str] = None,
    params: dict[str, any] = {},
    **generator_kwargs,
):
    """Search for some text

    ## Parameters
    - `reddit_or_sr`: To search across all of Reddit, pass a `praw.Reddit` instance.
        Otherwise, pass a `Subreddit` instance.
    - `limit`: How many posts to get (maximum 100)
    - `params`: Arguments to pass directly to the API (e.g. "category")
    - `generator_kwargs`: Extra args to pass to the generator
    """

    params = params.copy()

    if before is not None:
        params["before"] = before
    if after is not None:
        params["after"] = after

    if 1 <= limit <= 100:
        params["limit"] = limit
    else:
        print(f"Invalid limit: {limit}")

    if isinstance(reddit_or_sr, praw.Reddit):
        # The specific subreddit chosen doesn't matter because /search searches all
        # of Reddit anyway
        subreddit = reddit_or_sr.subreddit("AskReddit")
        params["restrict_sr"] = False
    else:
        subreddit = reddit_or_sr
        params["restrict_sr"] = True

    print("here!!!!!!!!!!!!!!!!")
    print(params)
    return subreddit.search(
        query, sort=sort, syntax="lucene", time_filter="all", params=params, **generator_kwargs
    )
