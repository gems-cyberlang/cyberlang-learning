from dotenv import load_dotenv
import numpy as np
import os
import csv
import time
import tqdm
import logging
import argparse
import praw
import json
from typing import Optional
import sys
import numpy.typing as npt

import praw.exceptions
import praw.models

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
ROWS = ["time", "comment_id", "body", "permalink", "score", "subreddit", "subreddit_id"]
REQUEST_PER_CALL = 100
SIZE_OF_ITERATION = 1000000
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

get_formatted_time = lambda: time.strftime("%Y-%m-%d-%H-%M-%S")


class permutaion:
    def __init__(self, start: int, stop: int, overwrite: bool) -> None:
        pass


class gems_runner:
    def __init__(
        self,
        max_collect: int,
        start_comment: int,
        end_commnet: int,
        client_id: str,
        reddit_secret: str,
        output_folder: str,
        log_file: Optional[str],
        log_level: int,
        praw_log_level: int,
        overwrite: bool = False,
    ) -> None:
        # Start logging
        self.logger = self.init_logging(
            "gems_runner", log_file, log_level, praw_log_level
        )
        self.logger.info("Logging started")

        self.output_dur = output_folder
        self.max_collect = max_collect
        self.start_comment = start_comment
        self.end_commnet = end_commnet
        self.client_id = client_id
        self.reddit_secret = reddit_secret
        self.overwrite = overwrite

        # Open files and directores loading data from there
        if not os.path.isdir(output_folder):
            os.mkdir(output_folder)  # make output directory if it dose not exits

        mode = "w+" if overwrite else "a+"

        write_rows = overwrite or not os.path.isfile(  # Come back and fix this
            os.path.join(output_folder, "comments.csv")
        )

        self.main_csv_f = open(os.path.join(output_folder, "comments.csv"), mode)
        self.main_csv = csv.writer(self.main_csv_f)

        if write_rows:
            self.main_csv.writerow(ROWS)

        self.program_data_f = open(
            os.path.join(output_folder, "program_data.json"), mode
        )

        if overwrite or not os.path.isfile(os.path.join(output_folder, "perm.")):
            self.perm = None
        else:
            self.perm = np.loadtxt(os.path.join(output_folder, "perm.txt"))

        if overwrite or not os.path.isfile(os.path.join(output_folder, "perm.txt")):
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

        # file getters
        self.get_perm_file_name = lambda start, stop: os.path.join(
            self.output_dur, f"perm-{str(start)}-{str(stop)}.perm"
        )
        self.get_pos_file_name = lambda start, stop: os.path.join(
            self.output_dur, f"pos-{str(start)}-{str(stop)}.perm"
        )

    def create_err(self, err_msg: str, logger: logging.Logger):
        """logs error and kills program

        Args:
            err_msg (str):
            logger (logging.Logger):
        """
        logger.error("ERROR: " + err_msg)
        exit(1)

    def init_logging(
        self,
        logger_name: str,
        log_file: Optional[str],
        log_level: int,
        praw_log_level: int,
    ) -> logging.Logger:
        """Initailize logging for whole system

        Args:
            logger_name (str): Will be printed in logs
            log_to_file (bool, optional): Whether to write to file default file 'log-%Y-%m-%d-%H-%M-%S.log" Defaults to True.
            log_level: What log level to use for our own logs
            praw_log_level: What log level to use for PRAW output to stderr

        Returns:
            logging.Logger: new logger
        """
        # Set up logging file
        log_file_name = (
            f"./run_{get_formatted_time()}.log" if log_file is None else log_file
        )
        # Global Log config
        logging.basicConfig(
            level=logging.DEBUG, filename=log_file_name, format=LOG_FORMAT
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

    def update_pos(self, pos: npt.ArrayLike):
        pos_file_name = self.get_pos_file_name()
        np.savetext(pos_file_name)

    def run_sub_section(self, start: int, stop: int, max: int):
        """Runs the full system for the based on inisialized values."""
        sub_count = 0
        make_request_str_from_id = lambda id: f"t1_{np.base_repr(id, 36).lower()}"

        # if(None == self.perm): # if we dont have a permutation yet make it
        self.perm = np.random.permutation(
            np.arange(start=start, stop=stop, dtype=np.uint64)
        )

        pad = np.zeros(
            [REQUEST_PER_CALL - self.perm.shape[0] % REQUEST_PER_CALL]
        )  # Create array of zeros to pad
        self.perm = np.append(self.perm, pad)
        perm_by_request_call = np.reshape(
            self.perm, [int(self.perm.shape[0] / REQUEST_PER_CALL), REQUEST_PER_CALL]
        )

        for i, id_request_goup in enumerate(perm_by_request_call):
            self.logger.debug(f"Attempting group {i} of size {REQUEST_PER_CALL}")
            id_request_str_group = [
                make_request_str_from_id(int(id)) for id in id_request_goup
            ]

            try:
                ret = self.reddit.info(fullnames=list(id_request_str_group))
            except praw.exceptions.PRAWException as e:
                self.logger.error(f"Praw error: {e}")
                self.logger.error(
                    f"Praw through a exeception in batch {i} of size {REQUEST_PER_CALL}"
                )
                continue

            for submission in ret:
                if type(submission) == praw.models.Comment:
                    self.main_csv.writerow(
                        [
                            submission.created_utc,
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
                    if self.count >= self.max_collect or sub_count >= max:
                        break
                else:
                    self.logger.debug(
                        f"{submission.id} was not a comment it had type {type(submission)}"
                    )

            self.logger.debug(f"Completed group {i} of size {REQUEST_PER_CALL}")
            if self.count >= self.max_collect or sub_count >= max:
                break

        self.logger.debug(f"End of list reached or max number of hits gotten")

    def run(self):
        for i in tqdm.trange(self.start_comment, self.end_commnet, SIZE_OF_ITERATION):
            self.run_sub_section(
                i,
                i + SIZE_OF_ITERATION,
                int(self.max_collect / (self.start_comment - self.end_commnet)),
            )

    def close(self):
        """Closes all open files."""
        self.main_csv_f.close()
        program_data = {"count": self.count}
        json.dump(program_data, self.program_data_f)
        self.program_data_f.close()
        if self.perm is not None:
            np.savetxt(os.path.join(self.output_dur, "perms.txt"), self.perm)


if __name__ == "__main__":
    # Arg parse
    parser = argparse.ArgumentParser(
        description="The Gems Reddit Data collector 9000 turdo"
    )

    parser.add_argument(
        "max_collect", type=int, help="The max number of comments to collect"
    )
    parser.add_argument("start_c", type=str, help="The comment ID to start at")
    parser.add_argument("end_c", type=str, help="The comment ID to end at")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
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
        default=f"./.env",
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

    runner = gems_runner(
        args.max_collect,
        int(args.start_c, 36),
        int(args.end_c, 36),
        client_id,
        reddit_secret,
        output_folder=args.output_dir,
        log_file=args.log_file,
        log_level=log_level,
        praw_log_level=praw_log_level,
    )

    runner.run()
    runner.close()
