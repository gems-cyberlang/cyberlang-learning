from dotenv import load_dotenv
import numpy as np
import os
import datetime
import logging
import argparse
import gems_runner

get_formatted_time = lambda: datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

class gems_runner:
    def __init__(self, time_sub_segment, reddit_secret, output_folder="./output", logger_name="gems_runner") -> None:
        logger = self.init_logging(logger_name)
        logger.info("Run started")

        if(not os.path.isdir(output_folder)): os.mkdir(output_folder) # make output directory if it dose not exits
           
 
    def crit_err(self, err_msg:str, logger:logging.Logger):
        """ logs error and kills program

        Args:
            err_msg (str): 
            logger (logging.Logger):
        """
        logger.error("ERROR: " + err_msg)
        exit()

    def init_logging(self, logger_name:str, log_to_file=True) -> logging.Logger:
        """ Initailize logging for whole system

        Args:
            logger_name (str): Will be printed in logs 
            log_to_file (bool, optional): Whether to write to file default file 'log-%Y-%m-%d-%H-%M-%S.log" Defaults to True.

        Returns:
            logging.Logger: new logger 
        """
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        if(log_to_file):
            log_file_name = f"run_{get_formatted_time}.log"
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler = logging.FileHandler("./" + log_file_name)
            file_handler.setFormatter(file_formatter)
            logging.getLogger().addHandler(file_handler)

        logger = logging.getLogger(logger_name)
        return logger


    def run():
        pass

if(__name__ == "__main__"):
    # Arg parse
    parser = argparse.ArgumentParser(description="The Gems Reddit Data collector 9000 turdo")
    parser.add_argument("output_dir", type=str, default="./output", help="ouput directory")
    parser.add_argument("log_file", type=str, default=f"./log-{get_formatted_time}.log", help="log file to use")
    parser.add_argument("--verbose", "-v", action="store_true", help="will print all logging to screen")
    parser.add_argument("--overwrite", "-o", action="store_true", help="if exsting files should be overwritten")
    parser.add_argument("--recover", "-r", action="store_true", help="will recover from a prior lost run")

    # Load env
    if (not load_dotenv()): 
        print("not env found") 
        exit()

    reddit_secret = os.getenv("REDDIT_SECRET")
    if(reddit_secret == None): 
        print("reddit secret no set")
        exit()
    logger.info(f"Env loaded")
    runner = gems_runner.gems_runner()
    runner.run()




# runner.run_range()