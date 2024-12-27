from dataclasses import dataclass
import json
import logging
import random
from typing import Optional
import os
import praw
import praw.models
import praw.exceptions
import prawcore
import selectors
import socket
import time

from config import Config, TimeRange
import util
from util import (
    AUTHOR_ID,
    AUTOMOD_ID,
    COMMENT_COLS,
    COMMENTS_TABLE,
    MISSES_TABLE,
    SQLITE_DB_FILE,
    ID,
)

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
REQUEST_PER_CALL = 100
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

LOG_FILE_NAME = "run.log"
PROGRAM_DATA_FILE_NAME = "program_data.json"


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
        config: Config,
        client_id: str,
        reddit_secret: str,
        output_dir: str,
        log_level: int,
        praw_log_level: int,
        port: int,
    ) -> None:
        self.output_dir = output_dir
        self.client_id = client_id
        self.reddit_secret = reddit_secret
        self.time_ranges = [
            TimeRangeWithHits(time_range, hits=0, misses=0)
            for time_range in config.time_ranges
        ]
        self.timestamp = get_formatted_time()
        self.sel = selectors.DefaultSelector()

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

        self.db_conn = util.create_db_conn()
        cur = self.db_conn.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS comments({','.join(COMMENT_COLS)})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS misses(id)")
        cur.close()

        self.update_time_ranges()

        self.req_num = 0
        """Just to keep track of how many requests we've made so far"""

        # Start reddit
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.reddit_secret,
            user_agent=USER_AGENT,
        )

        server_sock = socket.socket()
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("localhost", port))
        server_sock.listen(100)
        server_sock.setblocking(False)
        self.sel.register(server_sock, selectors.EVENT_READ, False)

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
            print(f"hits: {bin.hits}, misses: {bin.misses}")

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

    def accept(self, sock: socket.socket):
        """Accept a new connection"""
        conn, addr = sock.accept()
        self.logger.info(f"Accepted connection from {addr}")
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, True)

    def read(self, conn: socket.socket):
        """Receive a message from the dashboard. We're not actually using this yet,
        but we might want to allow stopping the server from the dashboard at some point
        """
        try:
            data = conn.recv(1)
        except:
            self.remove_conn(conn)
            return
        if not data:
            self.remove_conn(conn)
            return

        self.logger.warn(f"Got {data!r} from {conn}, I don't know what to do with it")

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
                self.db_conn.execute(
                    f"INSERT INTO {COMMENTS_TABLE} VALUES({','.join(['?'] * len(fields))})",
                    fields,
                )
                self.db_conn.commit()
                misses.remove(id_int)
            except:
                raise Exception(f"Got exception while processing {comment}")

        for id_int in misses:
            self.add_result(id_int, False)
            self.db_conn.execute(f"INSERT INTO {MISSES_TABLE} VALUES(?)", (id_int,))
            self.db_conn.commit()

        self.logger.debug(f"Completed group {self.req_num} of size {REQUEST_PER_CALL}")

    def get_next_ids(self) -> list[int]:
        # Pick a bin and request 100 comments from it
        # This doesn't account for there being no more comments to request,
        # but that's fine, given how many comments Reddit has
        bin = random.choice([bin for bin in self.time_ranges if bin.needed() > 0])

        ids = []
        while len(ids) < REQUEST_PER_CALL:
            id = random.randint(bin.time_range.start_id, bin.time_range.end_id)
            cur = self.db_conn.execute(
                f"SELECT {ID} FROM {COMMENTS_TABLE} WHERE {ID} = ?", (id,)
            )
            if cur.fetchone():
                # We already have this comment
                cur.close()
                continue
            cur.close()
            cur = self.db_conn.execute(
                f"SELECT {ID} FROM {MISSES_TABLE} WHERE {ID} = ?", (id,)
            )
            if cur.fetchone():
                # We already tried (and failed) to get this comment
                cur.close()
                continue
            cur.close()

            ids.append(id)

        return ids

    def run_step(self) -> bool:
        if all(bin.needed() == 0 for bin in self.time_ranges):
            self.logger.info(
                "Done! Got minimum number of comments for every time range"
            )
            self.logger.info(", ".join(map(str, self.time_ranges)))
            return False
        next_ids = self.get_next_ids()
        self.logger.debug(
            f"Requesting {len(next_ids)}: {','.join(map(util.to_b36, next_ids))}"
        )

        self.request_batch(next_ids)

        # Send updates to dashboard client(s)
        header = b"Date,Min,Hits,Misses,Total"
        rows = [
            f"{bin.time_range.start_date},{bin.time_range.min_comments},{bin.hits},{bin.misses},{bin.total()}".encode()
            for bin in self.time_ranges
        ]
        msg = header + b"\n" + b"\n".join(rows)
        for fd in list(self.sel.get_map()):
            key = self.sel.get_key(fd)
            if key.data:  # Is a client socket, not the server socket
                conn: socket.socket = key.fileobj  # type: ignore
                try:
                    conn.setblocking(False)
                    conn.send(msg)
                except:
                    self.remove_conn(conn)

        events = self.sel.select(0)
        for key, _mask in events:
            is_client = key.data
            if is_client:
                # This isn't necessary yet, but at some point, we might want to
                # receive messages from the dashboard
                self.read(key.fileobj)  # type: ignore
            else:
                self.accept(key.fileobj)  # type: ignore
        return True

    def remove_conn(self, conn: socket.socket):
        self.logger.info(f"Closing {conn}")
        self.sel.unregister(conn)
        conn.close()

    def close(self):
        """Closes all open files."""
        self.db_conn.close()

        program_data = {
            "timestamp": self.timestamp,
            "time_ranges": {
                str(time_range.time_range.start_date): {
                    "hits": time_range.hits,
                    "misses": time_range.misses,
                }
                for time_range in self.time_ranges
            },
        }
        with open(
            os.path.join(self.run_dir, PROGRAM_DATA_FILE_NAME), "w"
        ) as program_data_f:
            json.dump(program_data, program_data_f, indent=2)

        for fd in list(self.sel.get_map()):
            key = self.sel.get_key(fd)
            self.remove_conn(key.fileobj)  # type: ignore
