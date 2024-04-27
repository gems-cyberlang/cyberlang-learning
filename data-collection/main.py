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

import praw.exceptions
import praw.models

USER_AGENT = "GEMSTONE CYBERLAND RESEARCH"
ROWS = ["time", "comment_id", "body", "permalink", "score", "subreddit", "subreddit_id"]
REQUEST_PER_CALL = 100
SIZE_OF_ITERATION = 1000000

get_formatted_time = lambda: time.strftime("%Y-%m-%d-%H-%M-%S")

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

        self.output_dur = output_folder
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
        log_file_name = f"run_{get_formatted_time()}.log"
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
            logger.addHandler(handler) # main handler also goes to file

        # Set up term logging and verbosity
        logger = logging.getLogger(logger_name)

        if(verbose):
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        logging.disable()
        return logger

    def run_sub_section(self, start, stop, max):
        """Runs the full system for the based on inisialized values.
        """        
        sub_count = 0
        make_request_str_from_id = lambda id: f"t1_{np.base_repr(id, 36).lower()}"
        
        # if(None == self.perm): # if we dont have a permutation yet make it
        self.perm = np.random.permutation(np.arange(start=start, stop=stop, dtype=np.uint64))

        pad = np.zeros([REQUEST_PER_CALL-self.perm.shape[0]%REQUEST_PER_CALL]) # Create array of zeros to pad
        self.perm = np.append(self.perm, pad)
        perm_by_request_call = np.reshape(self.perm, [int(self.perm.shape[0]/REQUEST_PER_CALL), REQUEST_PER_CALL])
       
        for i, id_request_goup in enumerate(perm_by_request_call):
            self.logger.debug(f"Attempting group {i} of size {REQUEST_PER_CALL}")
            id_request_str_group = [make_request_str_from_id(int(id)) for id in id_request_goup]
            
            try:
                ret = self.reddit.info(list(id_request_str_group))
            except praw.exceptions.PRAWException as e:
                self.logger.error(f"Praw error: {e}")
                self.logger.error(f"Praw through a exeception in batch {i} of size {REQUEST_PER_CALL}")

            for submission in ret:
                if(type(submission) == praw.models.Comment):
                    self.main_csv.writerow([
                        submission.created_utc,
                        submission.id,
                        str(submission.body).replace("\n", ""),
                        submission.permalink,
                        submission.score,
                        submission.subreddit_id
                    ])
                    self.logger.debug(f"saved {submission.id}")
                    self.count += 1
                    if self.count >= self.max_collect or sub_count >= max: break
                else: 
                    self.logger.debug(f"{submission.id} was not a comment it had type {type(submission)}")

            self.logger.debug(f"Completed group {i} of size {REQUEST_PER_CALL}")
            if self.count >= self.max_collect or sub_count >= max: break

        self.logger.debug(f"End of list reached or max number of hits gotten"); 

    def run(self):
        for i in tqdm.trange(self.start_comment, self.end_commnet, SIZE_OF_ITERATION):
            self.run_sub_section(i, i+SIZE_OF_ITERATION, int(self.max_collect/(self.start_comment-self.end_commnet)))

    def close(self):
        """Closes all open files.
        """        
        self.main_csv_f.close()
        program_data = {
            "count": self.count
        }
        json.dump(program_data, self.program_data_f)
        self.program_data_f.close()
        np.savetxt(os.path.join(self.output_dur, "perms.txt"), self.perm) 
        
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
    runner.close()