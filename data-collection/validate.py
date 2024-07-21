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

import util


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
    runs = util.get_runs()
    run_nums = sorted(list(runs.keys()))

    if len(run_nums) == 0:
        print("No runs")
        exit(1)

    if max(run_nums) + 1 != len(run_nums):
        msg = f"Missing run? runs={run_nums}"
        print(f"❌ {msg}")
        add_issue(msg)

    misses = []
    comment_dfs = []

    for run_dir in runs.values():
        try:
            comment_dfs.append(util.load_comments(run_dir))
        except FileNotFoundError:
            msg = f"{run_dir} has no comments file"
            print(f"❌ {msg}")
            add_issue(msg)

        try:
            misses.extend(util.load_misses(run_dir))
        except FileNotFoundError:
            msg = f"{run_dir} has no missed IDs file"
            print(f"❌ {msg}")
            add_issue(msg)

    df = pd.concat(comment_dfs, axis=0, ignore_index=True)
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
