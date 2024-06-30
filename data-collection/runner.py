import csv
from glob import glob
import json
import logging
import numpy as np
import os
import pandas as pd
import praw
import praw.models
import praw.exceptions
import signal
import threading
import time

from bins import BinBinBin, TimeRange

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
ROWS = ["time", "comment_id", "body", "permalink", "score", "subreddit", "subreddit_id"]
REQUEST_PER_CALL = 100
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

COMMENTS_FILE_NAME = "comments.csv"
MISSED_FILE_NAME = "missed-ids.txt"
LOG_FILE_NAME = "run.log"
PROGRAM_DATA_FILE_NAME = "program_data.json"


def get_formatted_time():
    return time.strftime("%Y-%m-%d-%H-%M-%S")


def to_b36(id: int) -> str:
    """Get the base 36 repr of an ID to pass to Reddit or store"""
    return np.base_repr(id, 36).lower()


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
        time_ranges: list[TimeRange],
        client_id: str,
        reddit_secret: str,
        output_dir: str,
        log_level: int,
        praw_log_level: int,
    ) -> None:
        self.output_dir = output_dir
        self.client_id = client_id
        self.reddit_secret = reddit_secret
        self.time_ranges = BinBinBin(time_ranges)
        self.timestamp = get_formatted_time()

        # Open files and directores loading data from there
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)  # make output directory if it dose not exits

        # Load data on previous runs
        prev_run_dirs = glob("run_*", root_dir=self.output_dir)
        prev_run_nums = sorted(
            int(name.split("_", maxsplit=1)[1]) for name in prev_run_dirs
        )
        if len(prev_run_nums) == 0:
            # This is the first run
            curr_run = 0
        else:
            assert len(prev_run_nums) == max(prev_run_nums) + 1, "Missing run detected"
            curr_run = len(prev_run_nums)
            for run_num in prev_run_nums:
                run_path = os.path.join(output_dir, f"run_{run_num}")
                comments = pd.read_csv(os.path.join(run_path, COMMENTS_FILE_NAME))
                comments["comment_id"].apply(
                    lambda id: self.time_ranges.notify_requested(int(id, 36), True)
                )
                del comments
                with open(os.path.join(run_path, MISSED_FILE_NAME), "r") as misses_file:
                    for id in misses_file.readlines():
                        self.time_ranges.notify_requested(int(id, 36), False)

        self.run_dir = os.path.join(output_dir, f"run_{curr_run}")
        """Where all the data for this run goes"""
        os.mkdir(self.run_dir)

        # Start logging
        self.logger = self._init_logging("gems_runner", log_level, praw_log_level)
        self.logger.info("Logging started")

        # Come back and fix this
        # TODO wait fix what?
        comments_path = os.path.join(self.run_dir, COMMENTS_FILE_NAME)

        self.main_csv_f = open(comments_path, "w")
        self.main_csv = csv.writer(self.main_csv_f)
        self.main_csv.writerow(ROWS)

        missed_path = os.path.join(self.run_dir, MISSED_FILE_NAME)
        self.missed_comments_file = open(missed_path, "w")
        """Store IDs of comments that Reddit didn't return any info for"""

        # Start reddit
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.reddit_secret,
            user_agent=USER_AGENT,
        )

        self.logger.info("init complete")

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
        ids = [to_b36(int(id)) for id in id_ints]

        try:
            ret = self.reddit.info(fullnames=[f"t1_{id}" for id in ids])
        except praw.exceptions.PRAWException as e:
            self.logger.error(f"Praw error: {e}")
            self.logger.error(
                f"Praw through a exeception in batch {i} of size {REQUEST_PER_CALL}"
            )
            return

        misses = set(ids)
        for submission in ret:
            if isinstance(submission, praw.models.Comment):
                misses.remove(submission.id)
                self.main_csv.writerow(
                    [
                        int(submission.created_utc),
                        submission.id,
                        str(submission.body)
                        .replace("\r\n", " ")
                        .replace("\r", " ")
                        .replace("\n", " "),
                        submission.permalink,
                        submission.score,
                        submission.subreddit_id,
                    ]
                )
                self.logger.debug(f"saved {submission.id}")
                self.time_ranges.notify_requested(int(submission.id, 36), True)
            else:
                self.logger.error(
                    f"{submission.id} was not a comment it had type {type(submission)}"
                )

        for id in misses:
            self.time_ranges.notify_requested(int(id, 36), False)
            self.missed_comments_file.write(f"{id}\n")

        self.main_csv_f.flush()

        self.logger.debug(f"Completed group {i} of size {REQUEST_PER_CALL}")

    def run_step(self) -> bool:
        # TODO figure out how to use tqdm with this
        if self.time_ranges.needed() == 0:
            self.logger.info("Done! Got minimum number of comments for every time range")
            return False
        next_ids = self.time_ranges.next_ids(REQUEST_PER_CALL)
        self.logger.debug(f"Requesting {','.join(map(to_b36, next_ids))}")
        # todo figure out how to protect that block when running use Streamlit
        if threading.current_thread() is threading.main_thread():
            with ProtectedBlock():
                self.request_batch(next_ids)
        else:
            self.request_batch(next_ids)
        return True

    def close(self):
        """Closes all open files."""
        self.main_csv_f.close()
        self.missed_comments_file.close()

        program_data = {"timestamp": self.timestamp}
        with open(
            os.path.join(self.run_dir, PROGRAM_DATA_FILE_NAME), "w"
        ) as program_data_f:
            json.dump(program_data, program_data_f)
