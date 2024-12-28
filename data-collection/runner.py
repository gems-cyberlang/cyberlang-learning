from dataclasses import dataclass
import logging
import random
from typing import Optional
import os
import praw
import praw.models
import praw.exceptions
import prawcore
import time

from config import Config, TimeRange
import util
from util import (
    AUTHOR_ID,
    AUTOMOD_ID,
    COMMENT_COLS,
    COMMENTS_TABLE,
    MISSES_TABLE,
    ID,
)

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
REQUEST_PER_CALL = 100
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

_curr_dir = os.path.dirname(__file__)
LOG_DIR = os.path.join(_curr_dir, "logs")


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
        config: Config,
        client_id: str,
        reddit_secret: str,
        db_file: str,
        log_level: int,
        praw_log_level: int,
    ) -> None:
        self.client_id = client_id
        self.reddit_secret = reddit_secret
        self.db_file = db_file
        self.time_ranges = [
            TimeRangeWithHits(time_range, hits=0, misses=0)
            for time_range in config.time_ranges
        ]

        # Start logging
        self.logger = self._init_logging("gems_runner", log_level, praw_log_level)
        self.logger.info("Logging started")

    def __enter__(self):
        self.db_conn = util.create_db_conn(self.db_file)
        self.update_time_ranges()

        self.req_num = 0
        """Just to keep track of how many requests we've made so far"""

        # Start reddit
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.reddit_secret,
            user_agent=USER_AGENT,
        )

        self.logger.info("init complete")

        return self

    def __exit__(self, type, value, traceback):
        self.db_conn.close()

    def update_time_ranges(self):
        """Get the number of hits and misses from previous runs in each time period"""

        def get_count(table: str, time_range: TimeRange) -> int:
            cur = self.db_conn.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {time_range.start_id} <= {ID} AND {ID} <= {time_range.end_id}"
            )
            return cur.fetchone()[0]

        for bin in self.time_ranges:
            bin.hits = get_count(COMMENTS_TABLE, bin.time_range)
            bin.misses = get_count(MISSES_TABLE, bin.time_range)

        self.logger.debug("Updated time ranges")

    def add_result(self, id: int, is_hit: bool):
        """Record a requested ID that was either a hit or a miss"""
        for bin in self.time_ranges:
            if bin.time_range.start_id <= id <= bin.time_range.end_id:
                if is_hit:
                    bin.hits += 1
                else:
                    bin.misses += 1
                return
        hit_or_miss = "hit" if is_hit else "miss"
        self.logger.error(
            f"ID {util.to_b36(id)} ({hit_or_miss}) didn't fit into any bins"
        )

    def _init_logging(
        self,
        logger_name: str,
        log_level: int,
        praw_log_level: int,
    ) -> logging.Logger:
        """Initialize logging for whole system

        Args:
            logger_name (str): Will be printed in logs
            log_level: What log level to use for our own logs
            praw_log_level: What log level to use for PRAW output to stderr

        Returns:
            logging.Logger: new logger
        """
        if not os.path.isdir(LOG_DIR):
            os.mkdir(LOG_DIR)
        # Global log config
        logging.basicConfig(
            level=logging.DEBUG,
            filename=os.path.join(LOG_DIR, time.strftime("%Y-%m-%d-%H-%M-%S") + ".log"),
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

    def request_batch(self, id_ints: list[int]):
        """
        Requests the given IDs in a single API call.

        Make sure that there are no more than `REQUEST_PER_CALL` (100) of them
        """
        self.req_num += 1
        self.logger.debug(f"Attempting group {self.req_num} of size {REQUEST_PER_CALL}")
        ids = [util.to_b36(int(id)) for id in id_ints]

        try:
            ret = self.reddit.request(
                method="GET",
                path="api/info/",
                params={"id": ",".join("t1_" + id for id in ids)},
            )
        except prawcore.exceptions.ServerError as e:
            self.logger.error(f"Prawcore error, skipping batch {self.req_num}: {e}")
            return
        except praw.exceptions.PRAWException as e:
            self.logger.error(f"Praw error in batch {self.req_num}: {e}")
            return

        comments = ret["data"]["children"]

        misses = set(id_ints)
        for comment in comments:
            try:
                data = comment["data"]
                id_int = data["id"]
                body = data["body"]
                author_id = b36_if_truthy(
                    data.get("author_fullname", "").removeprefix("t2_")
                )
                if author_id == AUTOMOD_ID:
                    self.logger.debug(f"Comment {id_int} is by automod, skipping")
                    continue
                if len(body) == 0:
                    # Consider an empty comment a miss
                    self.logger.info(f"Comment {id_int} was empty")
                    continue
                if body == "[removed]" or body == "[deleted]":
                    # Consider a deleted comment a miss
                    self.logger.info(f"Comment {id_int} was {body}")
                    continue

                id_int = int(id_int, 36)
                fields = [
                    id_int,
                    int(data["created_utc"]),
                    data["subreddit"],
                    author_id,
                    b36_if_truthy(data["parent_id"].removeprefix("t1_")),
                    b36_if_truthy(data["link_id"].removeprefix("t3_")),
                    data["ups"],
                    data["downs"],
                    body,
                ]
                for col_name, field in zip(COMMENT_COLS, fields):
                    if col_name != AUTHOR_ID:
                        assert (
                            field != "" and field != None
                        ), f"{col_name} was empty/none in comment {id_int}"

                self.add_result(id_int, True)
                # Because we set "id" to be the primary key, inserting duplicate comments
                # isn't allowed.
                # So we use INSERT OR IGNORE, but that ignores ALL conflicts, so it's a bit
                # dangerous.
                self.db_conn.execute(
                    f"INSERT OR IGNORE INTO {COMMENTS_TABLE} VALUES({','.join(['?'] * len(fields))})",
                    fields,
                )
                self.db_conn.commit()
                misses.remove(id_int)
            except Exception as e:
                self.logger.error(f"Got exception while processing {comment}")
                raise

        for id_int in misses:
            self.add_result(id_int, False)
            self.db_conn.execute(
                f"INSERT OR IGNORE INTO {MISSES_TABLE} VALUES(?)", (id_int,)
            )
            self.db_conn.commit()

        self.logger.debug(f"Completed group {self.req_num} of size {REQUEST_PER_CALL}")

    def run_step(self) -> bool:
        if all(bin.needed() == 0 for bin in self.time_ranges):
            self.logger.info(
                "Done! Got minimum number of comments for every time range"
            )
            self.logger.info(", ".join(map(str, self.time_ranges)))
            return False

        # Pick a bin and request 100 comments from it
        next_bin = random.choice([bin for bin in self.time_ranges if bin.needed() > 0])
        next_ids = [
            random.randint(next_bin.time_range.start_id, next_bin.time_range.end_id)
            for _ in range(REQUEST_PER_CALL)
        ]
        self.logger.debug(
            f"Requesting {len(next_ids)}: {','.join(map(util.to_b36, next_ids))}"
        )
        self.request_batch(next_ids)
        return True
