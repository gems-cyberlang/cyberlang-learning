from abc import ABC, abstractmethod
from collections import deque
import datetime
from dotenv import load_dotenv
import numpy as np
import os
import csv
import gzip
import itertools
import time
import tqdm
import logging
import argparse
import praw
import json
import signal
import sys
from typing import Generic, Optional, TypeVar
import numpy.typing as npt
import yaml

import praw.exceptions
import praw.models

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
ROWS = ["time", "comment_id", "body", "permalink", "score", "subreddit", "subreddit_id"]
REQUEST_PER_CALL = 100
SIZE_OF_ITERATION = 1000000
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


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


class permutaion:
    def __init__(self, start: int, stop: int, overwrite: bool) -> None:
        pass


class AbstractBin(ABC):
    @property
    @abstractmethod
    def start_id(self) -> int:
        pass

    @property
    @abstractmethod
    def end_id(self) -> int:
        pass

    @abstractmethod
    def requested(self) -> int:
        """How many IDs in this time range we've requested"""

    @abstractmethod
    def unrequested(self) -> int:
        """
        How many IDs in this range have not been requested yet.

        Note: This has nothing to do with the minimum number of IDs that must be
        requested in a time range.
        """

    def needed(self) -> int:
        """How many comments we still need from this range"""
        return 0

    @abstractmethod
    def notify_requested(self, id: int, hit: bool):
        """
        Record the fact that we requested the given ID.

        If `hit` is true, we were able to read the comment
        """

    @abstractmethod
    def next_ids(self, n: int) -> list[int]:
        """
        Generate the next n IDs in this range.

        For `BinBin`s, this modifies state (though not for `PermBin`s)
        """

    def __contains__(self, id: int) -> bool:
        """Is this necessary? No. But I've always wanted to overload `in`, so please let me have this"""
        return self.start_id <= id < self.end_id


class PermBin(AbstractBin):
    """For keeping track of work done in a range of IDs within a `TimeRange`. Each
    `TimeRange` is made up of a bunch of `PermBin`s"""

    def __init__(self, start: int, end: int):
        """
        # Arguments
        * `start` - First ID in this range (inclusive)
        * `end` - End of this range (exclusive)
        * `hits` - How

        """
        self._start_id = start
        self._end_id = end
        self.hits = 0
        """Number of IDs that we requested and actually got"""
        self.misses = 0
        """Number of IDs that we requested but turned out to be inaccessible"""

    def requested(self) -> int:
        """How many comments have been requested in this bin so far"""
        return self.hits + self.misses

    def unrequested(self) -> int:
        total_available = self.end_id - self.start_id
        return total_available - self.requested()

    def next_ids(self, n: int) -> list[int]:
        perm = np.random.default_rng(seed=[self.start_id, self.end_id]).permutation(
            np.arange(start=self.start_id, stop=self.end_id, dtype=np.uint64)
        )
        return list(map(int, perm[self.requested() : self.requested() + n]))

    def notify_requested(self, id: int, hit: bool):
        assert id in self, f"{to_b36(id)} not in {self}"
        if hit:
            self.hits += 1
        else:
            self.misses += 1

    @property
    def start_id(self):
        return self._start_id

    @property
    def end_id(self):
        return self._end_id


T = TypeVar("T", bound=AbstractBin)


class BinBin(Generic[T], AbstractBin):
    """A bin containing other bins (of type `T`)"""

    def __init__(self, bins: list[T]):
        self.bins = bins
        self._remaining = deque(self.bins)
        self._update_remaining()

    def requested(self) -> int:
        return sum(bin.requested() for bin in self.bins)

    def unrequested(self) -> int:
        return sum(bin.unrequested() for bin in self.bins)

    def find_bin(self, id: int) -> Optional[T]:
        """Find the bin that the given ID goes into (None if it doesn't go into any of the bins)"""
        if id in self:
            for bin in self.bins:
                if id in bin:
                    return bin
            raise AssertionError(
                f"ID {id} should have fit into one of the bins in {self}"
            )
        else:
            return None

    def notify_requested(self, id: int, hit: bool):
        bin = self.find_bin(id)
        assert bin is not None, f"{to_b36(id)} not in {self}"
        bin.notify_requested(id, hit)

    def next_ids(self, n: int) -> list[int]:
        """
        Get the next n IDs.

        Every time this is called, it will rotate through the remaining bins
        """
        self._update_remaining()

        # If we have more than n bins, only need to look at the first n
        num_bins = min(n, len(self._remaining))
        front_bins = list(itertools.islice(self._remaining, num_bins))
        if num_bins > len(self._remaining):
            self._remaining.rotate(-num_bins)

        # Number of IDs to request from each remaining bin
        num_ids = [1] * len(front_bins)

        print(f"got front_bins {len(front_bins)}")

        while sum(num_ids) < n and any(
            bin.unrequested() > num_ids[i] for i, bin in enumerate(front_bins)
        ):
            for i, bin in enumerate(front_bins):
                if bin.unrequested() > num_ids[i]:
                    num_ids[i] += 1
                    if sum(num_ids) == n:
                        break

        return list(
            itertools.chain.from_iterable(
                bin.next_ids(n) for bin, n in zip(front_bins, num_ids)
            )
        )

    def _update_remaining(self):
        self._remaining = deque(bin for bin in self._remaining if bin.unrequested() > 0)


class TimeRange(BinBin):
    """For keeping track of work done in a time range"""

    def __init__(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        start_id: int,
        end_id: int,
        min_comments: int,
    ):
        """
        # Arguments
        * `start_date` and `end_date` - Only used for displaying data.
        * `start_id` - First ID in this range (inclusive)
        * `end_id` - End of this range (exclusive)
        * `min_comments` - Minimum number of comments to collect in this time range.

        """
        self.start_date = start_date
        self.end_date = end_date
        self._start_id = start_id
        self._end_id = end_id
        self.min = min_comments

        super().__init__(
            [
                PermBin(
                    bin_start,
                    min(bin_start + SIZE_OF_ITERATION, self.end_id),
                )
                for bin_start in range(self.start_id, self.end_id, SIZE_OF_ITERATION)
            ]
        )

    def needed(self) -> int:
        return max(0, self.min - self.requested())

    @property
    def start_id(self):
        return self._start_id

    @property
    def end_id(self):
        return self._end_id


U = TypeVar("U", bound=BinBin)


class BinBinBin(BinBin[U]):
    """Couldn't think of a better name"""

    def __init__(self, bins: list[U]):
        super().__init__(bins)
        assert all(bins[i].end_id == bins[i + 1].start_id for i in range(len(bins) - 1))
        self._start_id = bins[0].start_id
        self._end_id = bins[len(bins) - 1].end_id

    @property
    def start_id(self):
        return self._start_id

    @property
    def end_id(self):
        return self._end_id


class gems_runner:
    def __init__(
        self,
        time_ranges: list[TimeRange],
        client_id: str,
        reddit_secret: str,
        output_dir: str,
        log_file: str,
        log_level: int,
        praw_log_level: int,
        overwrite: bool = False,
    ) -> None:
        # Open files and directores loading data from there
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)  # make output directory if it dose not exits

        # Start logging
        self.logger = self._init_logging(
            "gems_runner", log_file, log_level, praw_log_level
        )
        self.logger.info("Logging started")

        self.output_dir = output_dir
        self.client_id = client_id
        self.reddit_secret = reddit_secret
        self.overwrite = overwrite
        self.time_ranges = BinBinBin(time_ranges)

        mode = "w+" if overwrite else "a+"

        # Come back and fix this
        # TODO wait fix what?
        comments_path = os.path.join(output_dir, "comments.csv")
        write_rows = overwrite or not os.path.isfile(comments_path)

        self.main_csv_f = open(comments_path, mode)
        self.main_csv = csv.writer(self.main_csv_f)

        if write_rows:
            self.main_csv.writerow(ROWS)

        self.program_data_f = open(os.path.join(output_dir, "program_data.json"), mode)

        missed_path = os.path.join(output_dir, "missed-comments.txt.gz")
        if os.path.exists(missed_path) and not overwrite:
            missed_comments_file = gzip.open(missed_path, "rt")
            for id in missed_comments_file.readlines():
                self.time_ranges.notify_requested(int(id, 36), False)
        self.missed_comments_file = gzip.open(missed_path, "wt" if overwrite else "at")
        """Store IDs of comments that Reddit didn't return any info for"""

        if overwrite or not os.path.isfile(os.path.join(output_dir, "perm.")):
            self.perm = None
        else:
            self.perm = np.loadtxt(os.path.join(output_dir, "perm.txt"))

        if overwrite or not os.path.isfile(os.path.join(output_dir, "perm.txt")):
            self.count = 0
        else:
            try:
                program_data = json.load(self.program_data_f)
                self.count = int(program_data.count)
            except:
                self.logger.error("Loading json file failed check formating")
                exit(1)

        # Start reddit
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.reddit_secret,
            user_agent=USER_AGENT,
        )

        self.logger.info("init complete")

    def get_perm_file_name(self, start: int, end: int):
        return os.path.join(self.output_dir, f"perm-{to_b36(start)}-{to_b36(end)}.txt")

    def get_pos_file_name(self, start: int, end: int):
        return os.path.join(self.output_dir, f"pos-{to_b36(start)}-{to_b36(end)}.txt")

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
        log_file: str,
        log_level: int,
        praw_log_level: int,
    ) -> logging.Logger:
        """Initailize logging for whole system

        Args:
            logger_name (str): Will be printed in logs
            log_file: Name of file to log to
            log_level: What log level to use for our own logs
            praw_log_level: What log level to use for PRAW output to stderr

        Returns:
            logging.Logger: new logger
        """
        # Set up logging file
        # Global Log config
        logging.basicConfig(level=logging.DEBUG, filename=log_file, format=LOG_FORMAT)

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

    def get_perm(self, start: int, stop: int, max: int):
        """Will get a permutaion if its exists if it dose not will open and save a new perm.
        Note perm revocery is only possible if the SIZE_OF_ITER and other constants are exactly
        the same as the prior run.

        Args:
            start (int): start index of perm
            stop (int): end index of perm
            max (int): the max number of requests to get form the perm

        Returns:
            (perm: np.array dtype=np.unint64, pos np.array dtype=np.uint64): the perm and pos
        """
        perm_file_name = self.get_perm_file_name(start, stop)
        pos_file_name = self.get_pos_file_name(start, stop)

        perm_file_exsits = os.path.isfile(perm_file_name)
        pos_file_exsits = os.path.isfile(pos_file_name)

        if self.overwrite or not perm_file_exsits or not pos_file_exsits:
            perm = np.random.permutation(
                np.arange(start=start, stop=stop, dtype=np.uint64)
            )
            pos = np.array([0], dtype=np.uint64)

            np.savetxt(perm_file_name, perm)
            np.savetxt(pos_file_name, pos)
        else:
            perm = np.loadtxt(perm_file_name, dtype=np.uint64)
            pos = np.loadtxt(pos_file_name, dtype=np.uint64)

        return perm, pos, pos_file_name

    # def update_pos(self, pos: npt.ArrayLike):
    #     pos_file_name = self.get_pos_file_name()
    #     np.savetext(pos_file_name)

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
            if type(submission) == praw.models.Comment:
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
                self.count += 1
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

    def run(self):
        # TODO figure out how to use tqdm with this

        while True:
            next_ids = self.time_ranges.next_ids(REQUEST_PER_CALL)
            print("got ids!")
            with ProtectedBlock():
                self.logger.debug(f"Requesting {','.join(map(to_b36, next_ids))}")
                self.request_batch(next_ids)

    def close(self):
        """Closes all open files."""
        self.main_csv_f.close()
        program_data = {"count": self.count}
        json.dump(program_data, self.program_data_f)
        self.program_data_f.close()
        self.missed_comments_file.close()
        if self.perm is not None:
            np.savetxt(os.path.join(self.output_dir, "perms.txt"), self.perm)


if __name__ == "__main__":
    curr_dir = os.path.dirname(__file__)

    # Arg parse
    parser = argparse.ArgumentParser(
        description="The Gems Reddit Data collector 9000 turdo"
    )

    parser.add_argument(
        "--config-file", "-c", type=str, default=os.path.join(curr_dir, "config.yaml")
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="ouput directory",
        required=False,
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="log file to use",
        required=False,
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=os.path.join(curr_dir, ".env"),
        help="the env file to use",
        required=False,
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="will print all logging to screen"
    )
    parser.add_argument("--silent", action="store_true", help="will log only errors")
    parser.add_argument(
        "--overwrite",
        "-o",
        action="store_true",
        help="if exsting files should be overwritten",
    )
    parser.add_argument(
        "--recover",
        "-r",
        action="store_true",
        help="if we should recover from a existing file",
    )
    parser.add_argument(
        "--praw-log",
        "-p",
        help="Log level for PRAW output",
        choices=["info", "debug", "warn", "error"],
        default="warn",
    )

    args = parser.parse_args()

    config = yaml.safe_load(open(args.config_file))
    time_step = datetime.timedelta(weeks=config["timeStep"])
    time_ranges_raw: list[dict] = config["timeRanges"]
    time_ranges = []
    start_time: datetime.date = config["timeStart"]
    for i, time_range in enumerate(time_ranges_raw):
        start_id = int(time_range["start"], 36)
        if "end" in time_range:
            end_id = int(time_range["end"], 36)
        elif i + 1 < len(time_ranges_raw):
            end_id = int(time_ranges_raw[i + 1]["start"], 36)
        else:
            raise AssertionError(
                f"An end ID should've been given for time range {time_range}"
            )
        time_ranges.append(
            TimeRange(
                start_date=start_time,
                end_date=start_time + time_step,
                start_id=start_id,
                end_id=end_id,
                min_comments=time_range["min"],
            )
        )
        start_time += time_step

    # Load env
    if not load_dotenv(args.env_file):
        print(f"You need a env file at {args.env_file}")
        exit(1)

    reddit_secret = os.getenv("REDDIT_SECRET")
    client_id = os.getenv("REDDIT_ID")

    if reddit_secret is None or client_id is None:
        print("Bad env")
        exit(1)

    if args.verbose and args.silent:
        print("Both --verbose and --silent were given", file=sys.stderr)
        parser.print_help()
        exit(1)
    elif args.verbose:
        log_level = logging.DEBUG
    elif args.silent:
        log_level = logging.ERROR
    else:
        log_level = logging.INFO

    if args.praw_log == "info":
        praw_log_level = logging.INFO
    elif args.praw_log == "debug":
        praw_log_level = logging.DEBUG
    elif args.praw_log == "warn":
        praw_log_level = logging.WARN
    elif args.praw_log == "error":
        praw_log_level = logging.ERROR
    else:
        praw_log_level = logging.CRITICAL

    if args.output_dir is None:
        output_dir = os.path.join(curr_dir, f"out")
    else:
        output_dir = args.output_dir

    if args.log_file is None:
        log_file = os.path.join(output_dir, f"run_{get_formatted_time()}.log")
    else:
        log_file = args.log_file

    runner = gems_runner(
        time_ranges,
        client_id,
        reddit_secret,
        output_dir=output_dir,
        log_file=log_file,
        log_level=log_level,
        praw_log_level=praw_log_level,
    )

    try:
        runner.run()
    finally:
        runner.close()
