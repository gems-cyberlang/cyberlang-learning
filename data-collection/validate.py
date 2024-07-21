#!/usr/bin/env python

"""
Check if the data we collected is messed up somehow

Can be used as either a script or as a module/library/whatever (call `validate.validate()`)

TODO make this more thorough. Right now, it just checks that all the files exist,
there aren't duplicate hit or miss IDs, and the IDs increase with the timestamps
"""

import numpy as np
import pandas as pd

import util


def duplicate_comments(df: pd.DataFrame) -> pd.DataFrame:
    """Find comments that have the same ID"""
    return df[df.duplicated(subset=["comment_id"], keep=False)]


def duplicate_misses(misses: list[int]) -> list[int]:
    """Find duplicate misses (assumes they're sorted)"""
    dupes = []
    for i in range(len(misses) - 1):
        if misses[i] == misses[i + 1]:
            dupes.append(np.base_repr(misses[i], 36))
    return dupes


def all_ids_sequential(df: pd.DataFrame) -> bool:
    """Do all comment IDs increase with their timestamps?"""
    for i in range(len(df) - 1):
        curr = df.iloc[i]
        next = df.iloc[i + 1]
        if curr.time > next.time:
            print(f"❌ Found out-of-order ID at {i}!")
            return False
    return True


def validate(df: pd.DataFrame, misses: list[int]):
    """Print out any problems detected in the data"""
    _validate_helper(df, misses, [])


def _validate_helper(df: pd.DataFrame, misses: list[int], issues: list[str]):
    """
    This helper just exists so that a list of previously found issues can be passed
    in when we're running this as a script
    """

    dupes = duplicate_comments(df)
    if len(dupes) == 0:
        print("✅ Found no duplicate comment IDs")
    else:
        print("❌ Found comments with duplicate IDs:")
        print(dupes)
        issues.append("Comments with duplicate IDs")

    dupes = duplicate_misses(misses)
    if len(dupes) > 0:
        print("❌ Found duplicate misses:")
        print(dupes)
        issues.append("Misses with duplicate IDs")
    else:
        print("✅ Found no duplicate misses")

    if all_ids_sequential(df):
        print("✅ All IDs were in order")
    else:
        issues.append("Out-of-order IDs")

    if len(issues) > 0:
        print("Found issues:")
        for issue in issues:
            print(f"- {issue}")
        exit(1)
    else:
        print("Everything good")


if __name__ == "__main__":
    runs = util.get_runs()
    run_nums = sorted(list(runs.keys()))

    if len(run_nums) == 0:
        print("No runs")
        exit(1)

    issues = []

    if max(run_nums) + 1 != len(run_nums):
        msg = f"Missing run? runs={run_nums}"
        print(f"❌ {msg}")
        issues.append(msg)

    misses = []
    comment_dfs = []

    for run_dir in runs.values():
        try:
            comment_dfs.append(util.load_comments(run_dir))
        except FileNotFoundError:
            msg = f"{run_dir} has no comments file"
            print(f"❌ {msg}")
            issues.append(msg)

        try:
            misses.extend(util.load_misses(run_dir))
        except FileNotFoundError:
            msg = f"{run_dir} has no missed IDs file"
            print(f"❌ {msg}")
            issues.append(msg)

    df = pd.concat(comment_dfs, axis=0, ignore_index=True)
    df = df.sort_values(["comment_id"]).reset_index(drop=True)

    misses.sort()

    _validate_helper(df, misses, issues)
