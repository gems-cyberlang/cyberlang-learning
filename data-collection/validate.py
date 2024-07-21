#!/usr/bin/env python

"""
Check if the data we collected is messed up somehow

Can be used as either a script or as a module/library/whatever (call `validate.validate()`)

TODO make this more thorough. Right now, it just checks that all the files exist,
there aren't duplicate hit or miss IDs, and the IDs increase with the timestamps
"""

import numpy as np
import pandas as pd
from typing import Any, Callable, Optional

import util
from util import COMMENTS_FILE_NAME, MISSED_FILE_NAME


_checks = {}
"""Maps functions that get bad rows or whatever to functions that print errors
and return a list of issues"""


def _check(
    happy_msg: str,
    error_msg: str | Callable[[Any], str],
    is_error: Optional[Callable[[Any], bool]] = None,
):
    def decorator(f):
        fn_name = f.__name__

        def make_err(res):
            msg = error_msg if isinstance(error_msg, str) else error_msg(res)
            print(f"❌ {msg} (call validate.{fn_name} for more info)")
            return msg

        def check_fn(*args, **kwargs):
            res = f(*args, **kwargs)
            if is_error is not None:
                if is_error(res):
                    return [make_err(res)]
            elif isinstance(res, pd.DataFrame):
                if len(res) > 0:
                    err = [make_err(res)]
                    print("Bad rows:")
                    print(res)
                    return err
            elif isinstance(res, list):
                if len(res) > 0:
                    err = [make_err(res)]
                    return err
            elif res is not None:
                raise ValueError(f"No idea how to handle {res}")

            print("✅", happy_msg)
            return []

        _checks[f] = check_fn

        return f

    return decorator


def run_check(f: Callable, *args, **kwargs) -> list[str]:
    return _checks[f](*args, **kwargs)


@_check(happy_msg="No missing runs", error_msg=lambda runs: f"Missing runs: {runs}")
def missing_runs(run_nums: list[int]):
    return [i for i in range(max(run_nums)) if i not in run_nums]


@_check(
    happy_msg="Found no duplicate comment IDs",
    error_msg="Found comments with duplicate IDs",
)
def duplicate_comments(df: pd.DataFrame) -> pd.DataFrame:
    """Find comments that have the same ID"""
    return df[df.duplicated(subset=[util.ID], keep=False)]


@_check(
    happy_msg="Found no duplicate misses",
    error_msg=lambda dupes: f"Found duplicate misses: {dupes}",
)
def duplicate_misses(misses: list[int]) -> list[int]:
    """Find duplicate misses (assumes they're sorted)"""
    dupes = []
    for i in range(len(misses) - 1):
        if misses[i] == misses[i + 1]:
            dupes.append(np.base_repr(misses[i], 36))
    return dupes


@_check(
    happy_msg="Found no out-of-order IDs",
    error_msg="Found out-of-order IDs",
)
def find_out_of_order_ids(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Find the first two comment where their IDs don't increase with their timestamps"""
    for i in range(len(df) - 1):
        curr = df.iloc[i]
        next = df.iloc[i + 1]
        if curr.time > next.time:
            return df.iloc[i : i + 2]
    return None


def validate(df: pd.DataFrame, misses: list[int]):
    """Print out any problems detected in the data"""
    _validate_helper(df, misses, [])


def _validate_helper(df: pd.DataFrame, misses: list[int], issues: list[str]):
    """
    This helper just exists so that a list of previously found issues can be passed
    in when we're running this as a script
    """

    issues.extend(run_check(duplicate_comments, df))

    issues.extend(run_check(duplicate_misses, misses))

    issues.extend(run_check(find_out_of_order_ids, df))

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

    issues.extend(run_check(missing_runs, run_nums))

    misses = []
    comment_dfs = []

    for run_dir in runs.values():
        try:
            comment_dfs.append(util.load_comments(run_dir))
        except FileNotFoundError:
            msg = f"No {COMMENTS_FILE_NAME} in {run_dir}"
            print(f"❌ {msg}")
            issues.append(msg)

        try:
            misses.extend(util.load_misses(run_dir))
        except FileNotFoundError:
            msg = f"No {MISSED_FILE_NAME} in {run_dir}"
            print(f"❌ {msg}")
            issues.append(msg)

    if len(comment_dfs) > 0:
        df = pd.concat(comment_dfs, axis=0, ignore_index=True)
        df = util.sort_comments(df)
    else:
        df = pd.DataFrame([], columns=util.COMMENT_COLS)

    misses.sort()

    _validate_helper(df, misses, issues)
