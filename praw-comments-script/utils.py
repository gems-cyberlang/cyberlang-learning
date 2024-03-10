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
    subreddit: praw.models.Subreddit,
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
    - `subreddit`: To search across all of Reddit, use r/all
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

    return subreddit.search(
        query, sort=sort, syntax="lucene", time_filter="all", params=params, **generator_kwargs
    )

COMMENT_COLS = ["name", "subreddit", "body"]

def comment_relevant_fields(comment: praw.models.Comment):
    """
    Extract the relevant fields of a comment, to be saved in a CSV
    """
    return [
        comment.name,
        comment.subreddit_name_prefixed.removeprefix("r/"),
        comment.body.replace("\n", " ").replace("\r", " "),
    ]
