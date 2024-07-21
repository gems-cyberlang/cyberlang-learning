from glob import glob
import os
import pandas as pd

COMMENTS_FILE_NAME = "comments.csv"
MISSED_FILE_NAME = "missed-ids.txt"

_curr_dir = os.path.dirname(__file__)

out_dir = os.path.join(_curr_dir, "out")
"""The directory in which all the runs are"""


def get_runs() -> dict[int, str]:
    """Get the path to each run, mapped from the run number"""

    return {
        int(run_dir.split("_", maxsplit=1)[1]): os.path.join(out_dir, run_dir)
        for run_dir in glob("run_*", root_dir=out_dir)
    }


def load_comments(path: str) -> pd.DataFrame:
    """
    Load a CSV with comments

    If the given path is a file, load that file. If it's a folder, load "{path}/comments.csv"
    """
    if os.path.isdir(path):
        path = os.path.join(path, COMMENTS_FILE_NAME)
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_csv(path)
    df["comment_id"] = df["comment_id"].apply(lambda id: int(id, 36))
    df = df.sort_values(["comment_id"]).reset_index(drop=True)
    return df


def load_misses(path: str) -> list[int]:
    """
    Load a file with a list of missed IDs

    If the given path is a file, load that file. If it's a folder, load "{path}/missed-ids.txt"
    """
    if os.path.isdir(path):
        path = os.path.join(path, MISSED_FILE_NAME)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r") as f:
        return [int(id, 36) for id in f.readlines()]
