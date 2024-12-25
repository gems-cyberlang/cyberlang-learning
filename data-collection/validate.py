#!/usr/bin/env python

"""
Check if the data we collected is messed up somehow

Can be used as either a script or as a module/library/whatever (call `validate.validate()`)

TODO make this more thorough. Right now, it just checks that there aren't duplicate
hit or miss IDs

TODO check for NaNs
"""

import pandas as pd
from typing import Any, Callable, Optional

import util


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


@_check(
    happy_msg="Found no duplicate comment IDs",
    error_msg="Found comments with duplicate IDs",
)
def duplicate_comments(df: pd.DataFrame) -> pd.DataFrame:
    """Find comments that have the same ID"""
    return df[df.duplicated(subset=[util.ID], keep=False)].map(util.to_b36)


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


def unexpected_nans(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Find rows with NaNs in unexpected places"""
    pass


def validate(df: pd.DataFrame, misses: pd.Series):
    """Print out any problems detected in the data"""

    issues = []

    issues.extend(run_check(duplicate_comments, df))

    issues.extend(run_check(duplicate_misses, misses))

    if len(issues) > 0:
        print("Found issues:")
        for issue in issues:
            print(f"- {issue}")
    else:
        print("Everything good")


if __name__ == "__main__":
    df, misses = util.load_data()
    validate(df, misses)
