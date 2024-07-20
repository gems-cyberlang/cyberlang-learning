#!/usr/bin/env python

"""
Check if the data we collected is messed up somehow

TODO make this more thorough. Right now, it just checks that all the files exist,
there aren't duplicate hit or miss IDs, and the IDs increase with the timestamps
"""

from glob import glob
import numpy as np
import os
import pandas as pd
from typing import Never


issues = []


def add_issue(msg: str):
    issues.append(msg)


def check_duplicate_comments(df: pd.DataFrame):
    """Make sure none of the comments have the same ID"""
    dupes = df[df.duplicated(subset=["comment_id"], keep=False)]
    if len(dupes) == 0:
        print("✅ Found no duplicate comment IDs")
    else:
        print("❌ Found comments with duplicate IDs:")
        print(dupes)
        add_issue("Comments with duplicate IDs")


def check_duplicate_misses(misses: list[int]):
    """Make sure none of the misses are duplicates"""
    global found_issues

    dupes = []
    for i in range(len(misses) - 1):
        if misses[i] == misses[i + 1]:
            dupes.append(np.base_repr(misses[i], 36))

    if len(dupes) > 0:
        print(f"❌ Found duplicate misses {dupes}")
        add_issue("Misses with duplicate IDs")
    else:
        print("✅ Found no duplicate misses")


def check_ids_sequential(df: pd.DataFrame):
    """Make sure that the comment IDs increase with their timestamps"""
    global found_issues

    bad_ids = False
    for i in range(len(df) - 1):
        curr = df.iloc[i]
        next = df.iloc[i + 1]
        if curr.time > next.time:
            print(f"❌ Found out-of-order ID at {i}!")
            bad_ids = True
    if bad_ids:
        add_issue("Out-of-order IDs")
    else:
        print("✅ All IDs were in order")


if __name__ == "__main__":
    curr_dir = os.path.dirname(__file__)

    out_dir = os.path.join(curr_dir, "out")
    run_dir_names = glob("run_*", root_dir=out_dir)
    run_nums = sorted(int(name.split("_", maxsplit=1)[1]) for name in run_dir_names)

    if len(run_nums) == 0:
        print("No runs")
        exit(1)

    if max(run_nums) + 1 != len(run_nums):
        msg = f"Missing run? runs={run_nums}"
        print(f"❌ {msg}")
        add_issue(msg)

    misses = []
    comment_dfs = []

    for run_num in run_nums:
        comments_file = os.path.join(out_dir, f"run_{run_num}/comments.csv")
        if os.path.exists(comments_file):
            comment_dfs.append(pd.read_csv(comments_file))
        else:
            msg = f"{comments_file} does not exist"
            print(f"❌ {msg}")
            add_issue(msg)

        misses_file = os.path.join(out_dir, f"run_{run_num}/missed-ids.txt")
        if os.path.exists(misses_file):
            with open(misses_file, "r") as f:
                misses.extend(int(id, 36) for id in f.readlines())
        else:
            msg = f"{misses_file} does not exist"
            print(f"❌ {msg}")
            add_issue(msg)

    df = pd.concat(comment_dfs, axis=0, ignore_index=True)
    df["comment_id"] = df["comment_id"].apply(lambda id: int(id, 36))
    df = df.sort_values(["comment_id"]).reset_index(drop=True)

    misses.sort()

    check_duplicate_comments(df)

    check_duplicate_misses(misses)

    check_ids_sequential(df)

    if len(issues) > 0:
        print("Found issues:")
        for issue in issues:
            print(f"- {issue}")
        exit(1)
