import csv
import datetime
import json
import logging
from typing import Optional
import os
import praw
import praw.models
import praw.exceptions
import selectors
import shutil
import signal
import socket
import sqlite3
import time

from bins import BinBinBin
from config import CONFIG_FILE_NAME, Config
import util
from util import (
    AUTHOR_ID,
    AUTOMOD_ID,
    COMMENT_COLS,
    COMMENTS_TABLE,
    MISSES_TABLE,
    SQLITE_DB_NAME,
    ID,
)

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
REQUEST_PER_CALL = 100
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

LOG_FILE_NAME = "run.log"
PROGRAM_DATA_FILE_NAME = "program_data.json"


def get_formatted_time():
    return time.strftime("%Y-%m-%d-%H-%M-%S")


class ProtectedBlock:
    """
    A context manager to protect a block from being interrupted by Ctrl+C.

    Copied from https://stackoverflow.com/a/21919644.
    """

    def __enter__(self):
        self.signal_received = None
        self.old_handler = signal.signal(signal.SIGINT, self.handler)

    def handler(self, sig, frame):
        self.signal_received = (sig, frame)

    def __exit__(self, _type, _value, _traceback):
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received is not None:
            # We were interrupted at some point, time to die now
            if callable(self.old_handler):
                self.old_handler(*self.signal_received)
            else:
                # TODO should we even bother handling this?
                exit(1)


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
        self.time_ranges = BinBinBin(config.time_ranges)
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

        self.conn = sqlite3.connect(os.path.join(output_dir, SQLITE_DB_NAME))
        self.cur = self.conn.cursor()
        self.cur.execute(
            f"CREATE TABLE IF NOT EXISTS comments({','.join(COMMENT_COLS)})"
        )
        self.cur.execute(f"CREATE TABLE IF NOT EXISTS misses(id)")

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

    def request_batch(self, id_ints: list[int]):
        """
        Requests the given IDs in a single API call.

        Make sure that there are no more than `REQUEST_PER_CALL` (100) of them
        """
        i = None  # TODO idk what to assign to this
        self.logger.debug(f"Attempting group {i} of size {REQUEST_PER_CALL}")
        ids = [util.to_b36(int(id)) for id in id_ints]

        try:
            ret = self.reddit.request(
                method="GET",
                path="api/info/",
                params={"id": ",".join("t1_" + id for id in ids)},
            )
        except praw.exceptions.PRAWException as e:
            self.logger.error(f"Praw error: {e}")
            self.logger.error(
                f"Praw through a exeception in batch {i} of size {REQUEST_PER_CALL}"
            )
            return

        comments = ret["data"]["children"]

        misses = set(id_ints)
        for comment in comments:
            try:
                data = comment["data"]
                id = data["id"]
                body = data["body"]
                author_id = data.get("author_fullname") or ""
                author_id = author_id.removeprefix("t2_")
                if author_id == AUTOMOD_ID:
                    self.logger.debug(f"Comment {id} is by automod, skipping")
                    continue
                if len(body) == 0:
                    # Consider an empty comment a miss
                    self.logger.info(f"Comment {id} was empty")
                    continue
                if body == "[removed]" or body == "[deleted]":
                    # Consider a deleted comment a miss
                    self.logger.info(f"Comment {id} was {body}")
                    continue

                fields = [
                    id,
                    int(data["created_utc"]),
                    data["subreddit"],
                    author_id,
                    data["parent_id"],
                    data["link_id"],
                    data["ups"],
                    data["downs"],
                    body,
                ]
                for col_name, field in zip(COMMENT_COLS, fields):
                    assert field is not None, col_name
                    if col_name != AUTHOR_ID:
                        assert field != "", col_name

                id_int = int(id, 36)
                self.time_ranges.notify_requested(id_int, True)
                self.cur.execute(
                    f"INSERT INTO {COMMENTS_TABLE} VALUES({','.join(['?'] * len(fields))})",
                    fields,
                )
                self.conn.commit()
                misses.remove(id_int)
            except:
                raise Exception(f"Got exception while processing {comment}")

        for id in misses:
            self.time_ranges.notify_requested(id, False)
            self.cur.execute(
                f"INSERT INTO {MISSES_TABLE} VALUES(?)", (util.to_b36(id),)
            )
            self.conn.commit()

        self.logger.debug(f"Completed group {i} of size {REQUEST_PER_CALL}")

    def run_step(self) -> bool:
        # TODO figure out how to use tqdm with this
        if self.time_ranges.needed == 0:
            self.logger.info(
                "Done! Got minimum number of comments for every time range"
            )
            self.logger.info(self.time_ranges.bins)
            return False
        next_ids = self.time_ranges.next_ids(REQUEST_PER_CALL)
        self.logger.debug(
            f"Requesting {len(next_ids)}: {','.join(map(util.to_b36, next_ids))}"
        )

        with ProtectedBlock():
            self.request_batch(next_ids)

        # Send updates to dashboard client(s)
        header = b"Date,Min,Hits,Misses"
        rows = [
            f"{bin.start_date},{bin.min},{bin.hits},{bin.misses}".encode()
            for bin in self.time_ranges.bins
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
        self.cur.close()
        self.conn.close()

        program_data = {
            "timestamp": self.timestamp,
            "time_ranges": {
                str(time_range.start_date): {
                    "hits": time_range.hits,
                    "misses": time_range.misses,
                }
                for time_range in self.time_ranges.bins
            },
        }
        with open(
            os.path.join(self.run_dir, PROGRAM_DATA_FILE_NAME), "w"
        ) as program_data_f:
            json.dump(program_data, program_data_f, indent=2)

        for fd in list(self.sel.get_map()):
            key = self.sel.get_key(fd)
            self.remove_conn(key.fileobj)  # type: ignore
