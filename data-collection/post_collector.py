from dataclasses import dataclass
import json
import logging
from random import randint
import random
from typing import Optional
import os
import praw
import praw.models
import praw.exceptions
import prawcore
import selectors
import signal
import socket
import sqlite3
import time

from config import Config, TimeRange
import util
from util import (
    AUTHOR_ID,
    AUTOMOD_ID,
    BODY,
    COMMENT_COLS,
    COMMENTS_TABLE,
    MISSES_TABLE,
    POST_COLS,
    SQLITE_DB_FILE,
    ID,
)

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
REQUEST_PER_CALL = 100
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

LOG_FILE_NAME = "run.log"
PROGRAM_DATA_FILE_NAME = "program_data.json"

POSTS_TABLE = "posts"


def get_formatted_time():
    return time.strftime("%Y-%m-%d-%H-%M-%S")


@dataclass
class TimeRangeWithHits:
    time_range: TimeRange
    hits: int
    misses: int

    def total(self) -> int:
        """Total number of comments in this time range"""
        return self.time_range.end_id - self.time_range.start_id

    def needed(self) -> int:
        return max(0, self.time_range.min_comments - self.hits)


def b36_if_truthy(s: Optional[str]) -> Optional[int]:
    """Convert from base 36 if the string is non-empty and not None"""
    return int(s, 36) if s else None


class gems_runner:
    def __init__(
        self,
        query: str,
        client_id: str,
        reddit_secret: str,
        output_dir: str,
        log_level: int,
        praw_log_level: int,
        port: int,
    ) -> None:
        self.query = query
        self.output_dir = output_dir
        self.client_id = client_id
        self.reddit_secret = reddit_secret
        self.timestamp = get_formatted_time()

        # Open files and directores loading data from there
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)  # make output directory if it dose not exits

        curr_run = self.curr_run_num()

        self.run_dir = os.path.join(output_dir, f"run_{curr_run}")
        """Where all the data for this run goes"""
        os.mkdir(self.run_dir)

        # Start logging
        self.logger = self._init_logging("gems_runner", log_level, praw_log_level)
        self.logger.info("Logging started")

        self.db_conn = sqlite3.connect("posts.db")  # util.create_db_conn()
        cur = self.db_conn.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS {POSTS_TABLE}({','.join(POST_COLS)})")
        cur.close()

        # Get the last post we found for this query
        cur = self.db_conn.execute(
            f"SELECT {ID} FROM {POSTS_TABLE} WHERE {BODY} LIKE '%{self.query}%' ORDER BY {ID}"
        )
        res = cur.fetchone()
        if res:
            (id_int,) = res
            self.last = f"t3_{util.to_b36(id_int)}"
        else:
            self.last = None

        cur = self.db_conn.execute(
            f"SELECT COUNT(*) FROM {POSTS_TABLE} WHERE {BODY} LIKE '%{self.query}%'"
        )
        (self.total,) = cur.fetchone()

        self.req_num = 0
        """Just to keep track of how many requests we've made so far"""

        # Start reddit
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.reddit_secret,
            user_agent=USER_AGENT,
        )

        self.logger.info("init complete")

    def curr_run_num(self):
        """Get the current run number."""

        prev_runs = util.get_runs()
        prev_run_nums = sorted(list(prev_runs.keys()))
        if len(prev_run_nums) == 0:
            # This is the first run
            return 0
        else:
            assert len(prev_run_nums) == max(prev_run_nums) + 1, "Missing run detected"
            return len(prev_run_nums)

    def create_err(self, err_msg: str, logger: logging.Logger):
        """logs error and kills program

        Args:
            err_msg (str):
            logger (logging.Logger):
        """
        logger.error("ERROR: " + err_msg)
        exit(1)

    def _init_logging(
        self,
        logger_name: str,
        log_level: int,
        praw_log_level: int,
    ) -> logging.Logger:
        """Initailize logging for whole system

        Args:
            logger_name (str): Will be printed in logs
            log_level: What log level to use for our own logs
            praw_log_level: What log level to use for PRAW output to stderr

        Returns:
            logging.Logger: new logger
        """
        # Set up logging file
        # Global Log config
        logging.basicConfig(
            level=logging.DEBUG,
            filename=os.path.join(self.run_dir, LOG_FILE_NAME),
            format=LOG_FORMAT,
        )

        # Praw logging goes to stderr
        praw_stderr_handler = logging.StreamHandler()
        praw_stderr_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        praw_stderr_handler.setLevel(praw_log_level)
        for other_logger_name in ("praw", "prawcore", "urllib3.connectionpool"):
            logger = logging.getLogger(other_logger_name)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(praw_stderr_handler)  # main handler also goes to file

        # Set up term logging and verbosity
        our_stderr_handler = logging.StreamHandler()
        our_stderr_handler.setLevel(log_level)
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(our_stderr_handler)

        return logger

    def get_post_fields(self, post: praw.models.Submission):
        """
        Extract the relevant fields of a post, to be saved in a CSV
        """
        slash_ind = post.permalink.index("/", 3)
        if post.permalink.startswith("/r/") and slash_ind > -1:
            sr_name = post.permalink[:slash_ind].removeprefix("/r/")
        else:
            sr_name = post.permalink
            self.logger.warning(
                f"Could not extract subreddit for post {post.id} ({post.permalink})"
            )
        return [
            int(post.id, 36),
            int(post.created_utc),
            sr_name,
            post.author.name.removeprefix("t2_"),
            post.title,
            post.selftext,
            post.num_comments,
        ]

    def search_posts(self):
        self.req_num += 1
        self.logger.debug(
            f"Attempting group {self.req_num} of size {REQUEST_PER_CALL} (last: {self.last})"
        )

        try:
            posts = list(
                self.reddit.subreddit("all").search(
                    self.query,
                    sort="new",
                    limit=REQUEST_PER_CALL,
                    params={"after": f"t3_{self.last}"},
                )
            )
        except prawcore.exceptions.ServerError as e:
            self.logger.error(f"Prawcore error, skipping batch {self.req_num}: {e}")
            return
        except praw.exceptions.PRAWException as e:
            self.logger.error(f"Praw error in batch {self.req_num}: {e}")
            return

        self.logger.info(f"Got {','.join(post.id for post in posts)}")

        for post in posts:
            try:
                fields = self.get_post_fields(post)
                id = post.id
                id_int = int(id.removeprefix("t3_"), 36)
                for col_name, field in zip(POST_COLS, fields):
                    if col_name != AUTHOR_ID and col_name != BODY:
                        assert (
                            field != "" and field != None
                        ), f"{col_name} was empty/none in post {id_int}"

                cur = self.db_conn.execute(
                    f"SELECT {ID} FROM {POSTS_TABLE} WHERE {ID} = ?", (id_int,)
                )
                if cur.fetchone():
                    # We already have this post
                    cur.close()
                    continue
                cur.close()

                self.db_conn.execute(
                    f"INSERT INTO {POSTS_TABLE} VALUES({','.join(['?'] * len(fields))})",
                    fields,
                )
                self.db_conn.commit()
                self.total += 1
            except:
                raise Exception(f"Got exception while processing {post}")

        self.logger.debug(
            f"Completed group {self.req_num}, got {len(posts)} posts (total {self.total})"
        )

        if len(posts) > 0:
            self.last = posts[-1].id
            self.search_posts()

    def close(self):
        """Closes all open files."""
        self.db_conn.close()
