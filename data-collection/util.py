import configparser
from datetime import datetime
from glob import glob
import numpy as np
import os
import pandas as pd
import praw
import praw.models
import re
import sys
import sqlite3
from typing import Any, Optional

from config import TimeRange

_curr_dir = os.path.dirname(__file__)

SQLITE_DB_FILE = os.path.join(_curr_dir, "data.db")
COMMENTS_TABLE = "comments"
MISSES_TABLE = "misses"

AUTOMOD_ID = int("6l4z3", 36)
"""ID of automod user as integer"""


def to_b36(id: int) -> str:
    """Get the base 36 repr of an ID to pass to Reddit or store"""
    return np.base_repr(id, 36).lower()


def create_db_conn(db_file: Optional[str] = None) -> sqlite3.Connection:
    """
    Connect to the given database and create tables if they don't exist

    TODO use Postgres instead

    This creates two tables, `comments` and `misses`, if they don't exist already.
    In both tables, `id` is set as the primary key so that they can't have duplicate IDs.

    # Arguments
    * `db_file`: The file in which the SQLite database lives (data.db if not given)
    """
    if not db_file:
        db_file = SQLITE_DB_FILE

    conn = sqlite3.connect(db_file)
    other_cols = ",".join(COMMENT_COLS[1:])
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {COMMENTS_TABLE}({ID} INTEGER PRIMARY KEY, {other_cols})"
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS {MISSES_TABLE}({ID} INTEGER PRIMARY KEY)")
    return conn


def load_data() -> tuple[pd.DataFrame, pd.Series]:
    with create_db_conn() as conn:
        df = pd.read_sql(f"SELECT * FROM {COMMENTS_TABLE} ORDER BY {ID}", conn)
        df[BODY] = df[BODY].apply(str)
        df[TIME] = df[TIME].map(lambda ts: datetime.fromtimestamp(ts))

        misses = pd.read_sql(f"SELECT * FROM {MISSES_TABLE} ORDER BY {ID}", conn)
        return df, misses[ID]


def time_range_stats(
    comments: pd.DataFrame, misses: pd.Series, time_ranges: list[TimeRange]
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "start_date": [time_range.start_date for time_range in time_ranges],
            "end_date": [time_range.end_date for time_range in time_ranges],
            "start_id": [time_range.start_id for time_range in time_ranges],
            "end_id": [time_range.end_id for time_range in time_ranges],
            "hits": [
                len(
                    comments[
                        (time_range.start_id <= comments[ID])
                        & (comments[ID] <= time_range.end_id)
                    ]
                )
                for time_range in time_ranges
            ],
            "misses": [
                len(
                    misses[
                        (time_range.start_id <= misses) & (misses <= time_range.end_id)
                    ]
                )
                for time_range in time_ranges
            ],
        }
    )


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
    """
    Make a multiline string appropriate for CSVs

    Replace newlines with "\\n". Existing literal "\\n"s are escaped with an extra slash
    """
    return (
        s.replace(r"\n", r"\\n")
        .replace("\r\n", r"\n")
        .replace("\n", r"\n")
        .replace("\r", r"\n")
    )


# Column names
ID = "id"
"""Column for comment ID (integer)"""
TIME = "time"
SR_NAME = "sr_name"
"""Column for subreddit name ('r/foo')"""
AUTHOR_ID = "author_id"
"""Column for the ID (integer) of the author of the comment"""
PARENT_ID = "parent_id"
"""Column for the ID (integer) of the parent of the comment"""
POST_ID = "post_id"
"""Column for the post the comment was submitted to"""
UPVOTES = "upvotes"
"""Column for number of upvotes"""
DOWNVOTES = "downvotes"
"""Column for number of downvotes"""
BODY = "body"
COMMENT_COLS = [
    ID,
    TIME,
    SR_NAME,
    AUTHOR_ID,
    PARENT_ID,
    POST_ID,
    UPVOTES,
    DOWNVOTES,
    BODY,
]


POST_COLS = [ID, TIME, SR_NAME, "author", "title", BODY, "num_comments"]


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
