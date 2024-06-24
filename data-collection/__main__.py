import datetime
from dotenv import load_dotenv
import os
import logging
import argparse
import sys
import yaml

from bins import TimeRange
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

runner = gems_runner(
    time_ranges,
    client_id,
    reddit_secret,
    output_dir=output_dir,
    log_level=log_level,
    praw_log_level=praw_log_level,
)

try:
    runner.run()
finally:
    runner.close()
