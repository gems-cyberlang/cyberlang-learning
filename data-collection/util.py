import configparser
from glob import glob
import os
import pandas as pd
import praw
import praw.models
import re
import sys
from typing import Any, Optional

COMMENTS_FILE_NAME = "comments.csv"
MISSED_FILE_NAME = "missed-ids.txt"

_curr_dir = os.path.dirname(__file__)

out_dir = os.path.join(_curr_dir, "out")
"""The directory in which all the runs are"""


def get_runs() -> dict[int, str]:
    """Get the path to each run, mapped from the run number"""

    return {
        int(run_dir.split("_", maxsplit=1)[1]): os.path.join(out_dir, run_dir)
        for run_dir in glob("run_*", root_dir=out_dir)
    }


def load_comments(path: str) -> pd.DataFrame:
    """
    Load a CSV with comments

    If the given path is a file, load that file. If it's a folder, load "{path}/comments.csv"
    """
    if os.path.isdir(path):
        path = os.path.join(path, COMMENTS_FILE_NAME)
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_csv(path)
    df["comment_id"] = df["comment_id"].apply(lambda id: int(id, 36))
    df = df.sort_values(["comment_id"]).reset_index(drop=True)
    return df


def load_misses(path: str) -> list[int]:
    """
    Load a file with a list of missed IDs

    If the given path is a file, load that file. If it's a folder, load "{path}/missed-ids.txt"
    """
    if os.path.isdir(path):
        path = os.path.join(path, MISSED_FILE_NAME)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r") as f:
        return [int(id, 36) for id in f.readlines()]

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
    params: dict[str, Any] = {},
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

