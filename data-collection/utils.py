from typing import Optional

import configparser
import praw
import re
import sys


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
        query,
        sort=sort,
        syntax="lucene",
        time_filter="all",
        params=params,
        **generator_kwargs,
    )


def multiline_to_csv(s: str) -> str:
    """Make a multiline string appropriate for CSVs"""
    return s.replace("\n", " ").replace("\r", " ")


COMMENT_COLS = ["name", "subreddit", "time", "body"]


def comment_relevant_fields(comment: praw.models.Comment):
    """
    Extract the relevant fields of a comment, to be saved in a CSV
    """
    return [
        comment.name.removeprefix("t1_"),
        comment.subreddit_name_prefixed.removeprefix("r/"),
        int(comment.created_utc),
        multiline_to_csv(comment.body),
    ]


POST_COLS = ["id", "time", "subreddit", "author", "title", "body", "num_comments"]


def post_relevant_fields(post: praw.models.Submission):
    """
    Extract the relevant fields of a post, to be saved in a CSV
    """
    sr_match = re.search("r/([^/]+)", post.permalink)
    if sr_match is None:
        sr_name = post.permalink
        print(f"Could not extract subreddit for post {post.id}", file=sys.stderr)
    else:
        sr_name = post.permalink[sr_match.start(1) : sr_match.end(1)]
    return [
        post.id,
        int(post.created_utc),
        sr_name,
        post.author.name,
        post.title,
        multiline_to_csv(post.selftext),
        post.num_comments,
    ]
