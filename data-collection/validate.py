#!/usr/bin/env python

"""
Check if the data we collected is messed up somehow

Can be used as either a script or as a module/library/whatever (call `validate.validate()`)

TODO make this more thorough. Right now, it just checks that all the files exist,
there aren't duplicate hit or miss IDs, and the IDs increase with the timestamps

TODO check for NaNs
"""

import os
import pandas as pd
from typing import Any, Callable, Iterable, Optional

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
def missing_runs(run_nums: Iterable[int]):
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
    error_msg=lambda dupes: f"Found {len(dupes)} duplicate misses: "
    + (
        ", ".join(map(str, dupes))
        if len(dupes) < 10
        else ", ".join(map(str, dupes[:10])) + ", ..."
    ),
)
def duplicate_misses(misses: pd.Series) -> list[str]:
    """Find duplicate misses (assumes they're sorted)"""
    return [util.to_b36(id) for id in misses[misses.duplicated(keep=False)]]


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

def unexpected_nans(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Find rows with NaNs in unexpected places"""
    pass

def validate(df: pd.DataFrame, misses: pd.Series):
    """Print out any problems detected in the data"""
    _validate_helper(df, misses, [])


def _validate_helper(df: pd.DataFrame, misses: pd.Series, issues: list[str]):
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

    comments_files = []
    misses_files = []

    for run_dir in runs.values():
        if os.path.exists(os.path.join(run_dir, COMMENTS_FILE_NAME)):
            comments_files.append(run_dir)
        else:
            msg = f"No {COMMENTS_FILE_NAME} in {run_dir}"
            print(f"❌ {msg}")
            issues.append(msg)

        if os.path.exists(os.path.join(run_dir, MISSED_FILE_NAME)):
            misses_files.append(run_dir)
        else:
            msg = f"No {MISSED_FILE_NAME} in {run_dir}"
            print(f"❌ {msg}")
            issues.append(msg)

    if len(comments_files) > 0:
        df = util.load_comments(*comments_files)
    else:
        df = pd.DataFrame([], columns=util.COMMENT_COLS)

    if len(misses_files) > 0:
        misses = util.load_misses(*misses_files)
    else:
        misses = pd.Series([])

    _validate_helper(df, misses, issues)
