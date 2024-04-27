from dotenv import load_dotenv
import numpy as np
import os
import csv
import datetime
import logging
import argparse
import praw
import json
import random

import praw.exceptions
import praw.models

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
ROWS = ["time", "comment_id", "body", "permalink", "score", "subreddit", "subreddit_id"]
REQUEST_PER_CALL = 100

get_formatted_time = lambda: datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

class gems_runner:
    def __init__(self, 
                 max_collect:int, 
                 start_comment:int, 
                 end_commnet:int, 
                 client_id:str, 
                 reddit_secret:str, 
                 output_folder:str = "./output", 
                 verbose:bool = False,
                 overwrite:bool = False,
                 ) -> None:
        # Start logging 
        self.logger = self.init_logging("gems_runner", verbose)
        self.logger.info("Logging started")

        self.max_collect = max_collect
        self.start_comment = start_comment
        self.end_commnet = end_commnet
        self.client_id = client_id 
        self.reddit_secret = reddit_secret

        # Open files and directores loading data from there
        if(not os.path.isdir(output_folder)): os.mkdir(output_folder) # make output directory if it dose not exits
        
        mode = "w+" if overwrite else "a+"

        write_rows = overwrite or not os.path.isfile(os.path.join(output_folder, "comments.csv"))

        self.main_csv_f = open(os.path.join(output_folder, "comments.csv"), mode)
        self.main_csv = csv.writer(self.main_csv_f)
        
        if write_rows: self.main_csv.writerow(ROWS)

        self.program_data_f = open(os.path.join(output_folder, "program_data.json"), mode)
        
        if overwrite or not os.path.isfile(os.path.join(output_folder, "perm.gz")):
            self.perm = None
        else:
            self.perm = np.loadtxt(os.path.join(output_folder, "perm.gz"))

        if overwrite or not os.path.isfile(os.path.join(output_folder, "perm.gz")):
            self.count = 0
        else:
            try:
                program_data = json.load(self.program_data_f)
                self.count = int(program_data.count)
            except: 
                self.logger.error("Loading json file failed check formating")
                exit()

        # Start reddit
        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.reddit_secret,
            user_agent=USER_AGENT)
   
        self.logger.info("init complete")

    def crit_err(self, err_msg:str, logger:logging.Logger):
        """ logs error and kills program

        Args:
            err_msg (str): 
            logger (logging.Logger):
        """
        logger.error("ERROR: " + err_msg)
        exit()

    def init_logging(self, logger_name:str, verbose:bool) -> logging.Logger:
        """ Initailize logging for whole system

        Args:
            logger_name (str): Will be printed in logs 
            log_to_file (bool, optional): Whether to write to file default file 'log-%Y-%m-%d-%H-%M-%S.log" Defaults to True.

        Returns:
            logging.Logger: new logger 
        """
        # Global Log config
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Set up logging file
        log_file_name = f"run_{get_formatted_time}.log"
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler("./" + log_file_name)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)

        #Praw logging goes to stderr
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        for logger_name in ("praw", "prawcore"):
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(handler)
            logger.addHandler(file_handler) # Also output to file

        # Set up term logging and verbosity
        logger = logging.getLogger(logger_name)

        if(verbose):
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        return logger

    def run(self):
        make_request_str_from_id = lambda id: f"t1_{np.base_repr(id, 36).lower()}"
        
        if(None == self.perm): # if we dont have a permutation yet make it
            self.perm = np.random.permutation(np.arange(start=self.start_comment, stop=self.end_commnet))

        pad = np.zeros([REQUEST_PER_CALL-self.perm.shape[0]%REQUEST_PER_CALL]) # Create array of zeros to pad
        self.perm = np.append(self.perm, pad)
        perm_by_request_call = np.reshape(self.perm, [REQUEST_PER_CALL, int(self.perm.shape[0]/REQUEST_PER_CALL)])
       
        for i, id_request_goup in enumerate(perm_by_request_call):
            self.logger.debug(f"Attempting group {i} of size {REQUEST_PER_CALL}")
            id_request_str_group = id_request_goup.apply_along_axis(make_request_str_from_id, 0, id_request_goup)
            
            try:
                ret = self.reddit.info(list(id_request_goup))
            except praw.exceptions.PRAWException as e:
                self.logger.error(f"Praw error: {e}")
                self.logger.error(f"Praw through a exeception in batch {i} of size {REQUEST_PER_CALL}")

            for submission in ret:
                if(type(submission) == praw.models.Comment):
                    self.main_csv.writerow([
                        submission.created_utc,
                        submission.id,
                        submission.body,
                        submission.permalink,
                        submission.score,
                        submission.subreddit_id
                    ])
                    self.logger.debug(f"saved {submission.id}")
                    self.count += 1
                    if self.count >= self.max_collect: break
                else:
                    self.logger.debug(f"{submission.id} was not a comment it had type {type(submission)}")

            self.logger.debug(f"Completed group {i} of size {REQUEST_PER_CALL}")
            if self.count >= self.max_collect: break

        self.logger.debug(f"End of list reached or max number of hits gotten"); 

    def close(self):
        self.main_csv_f.close()
        program_data = {
            "count": self.count,
        }
        np.savetxt() 
        
if(__name__ == "__main__"):
    # Arg parse
    parser = argparse.ArgumentParser(description="The Gems Reddit Data collector 9000 turdo")

    parser.add_argument("max_collect", type=int, help="The max number of comments to collect")
    parser.add_argument("start_c", type=str, help="The comment ID to start at")
    parser.add_argument("end_c", type=str, help="The comment ID to end at")
    parser.add_argument("--output_dir", type=str, default="./output", help="ouput directory", required=False)
    parser.add_argument("--log_file", type=str, default=f"./log-{get_formatted_time}.log", help="log file to use", required=False)
    parser.add_argument("--env_file", type=str, default=f"./.env", help="the env file to use", required=False)
    parser.add_argument("--verbose", "-v", action="store_true", help="will print all logging to screen")
    parser.add_argument("--overwrite", "-o", action="store_true", help="if exsting files should be overwritten")
    parser.add_argument("--recover", "-r", action="store_true", help="if we should recover from a existing file")

    args = parser.parse_args()

    # Load env
    if (not load_dotenv(args.env_file)): 
        print(f"You need a env file at {args.env_file}") 
        exit()

    reddit_secret = os.getenv("REDDIT_SECRET")
    client_id = os.getenv("REDDIT_ID")

    if(reddit_secret == None or client_id == None): 
        print("Bad env");
        exit()

    runner = gems_runner(
        args.max_collect,
        int(args.start_c, 36),
        int(args.end_c, 36),
        client_id,
        reddit_secret,
        args.output_dir,
        args.log_file,)

    runner.run()