from dotenv import load_dotenv
import os
import logging
import argparse
import sys
from alive_progress import alive_bar

from config import Config
from runner import gems_runner


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
    "--praw-log",
    "-P",
    help="Log level for PRAW output",
    choices=["info", "debug", "warn", "error"],
    default="warn",
)
parser.add_argument(
    "--port",
    "-p",
    help="Port for the server to listen on",
    default=1234,
    type=int,
)

args = parser.parse_args()

config = Config.load(args.config_file)

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

runner = gems_runner(
    config,
    client_id,
    reddit_secret,
    output_dir=output_dir,
    log_level=log_level,
    praw_log_level=praw_log_level,
    port=args.port,
)

try:
    total_needed = runner.time_ranges.needed
    with alive_bar(total=total_needed, manual=True) as pbar:
        while runner.run_step():
            pbar(1 - runner.time_ranges.needed / total_needed)
    print("Done!")
finally:
    runner.close()
