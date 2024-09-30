#!/usr/bin/env python
"""
Find IDs for a bunch of timestamps and generate a config based on that
"""

import argparse
import datetime
import logging
from typing import Optional
import util

NOT_THIS_FUCKING_ID_AGAIN = {int("c3ctzso", 36), int("c3ctzsq", 36), int("f22fgrh", 36)}

reddit = util.init_reddit()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setLevel(logging.WARNING)
for logger_name in ("praw", "prawcore"):
    praw_logger = logging.getLogger(logger_name)
    praw_logger.setLevel(logging.DEBUG)
    praw_logger.addHandler(handler)

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

parser = argparse.ArgumentParser(
    description="Find IDs at the border of some time ranges to generate a config.yaml"
)
parser.add_argument("time_first", help="Start of first time range")
parser.add_argument("time_last", help="End of last time range")
parser.add_argument("time_step", help="Length of each time range, in months", type=int)
parser.add_argument("start_id", help="Lower bound on ID (inclusive)")
parser.add_argument("end_id", help="Upper bound on ID (inclusive)")
args = parser.parse_args()

time_first = datetime.datetime.combine(
    date=datetime.datetime.fromisoformat(args.time_first),
    time=datetime.time(0, 0, 0),
    tzinfo=datetime.timezone.utc,
)
time_last = datetime.datetime.combine(
    date=datetime.datetime.fromisoformat(args.time_last),
    time=datetime.time(0, 0, 0),
    tzinfo=datetime.timezone.utc,
)
time_step: int = args.time_step

low = int(args.start_id, 36)
high = int(args.end_id, 36)

time_ranges = []
curr = time_first
while curr < time_last:
    time_ranges.append(curr)
    if curr.month + time_step <= 12:
        curr = curr.replace(month=curr.month + time_step)
    else:
        curr = curr.replace(year=curr.year + 1, month=1)

if curr != time_last:
    logger.warning("Time ranges didn't fit exactly")
time_ranges.append(min(curr, time_last))

logger.debug(f"{time_ranges=}")

bounds: dict[
    datetime.datetime,
    tuple[
        Optional[int],
        Optional[datetime.datetime],
        Optional[int],
        Optional[datetime.datetime],
    ],
] = {range_start: (None, None, None, None) for range_start in time_ranges}
bounds[time_first] = (low, None, None, None)
bounds[time_ranges[len(time_ranges) - 1]] = (None, None, high, None)

# todo avoid re-requesting IDs that were misses

# todo there are almost certainly off-by-ones lurking around here


def bounds_str():
    res = []
    for range_start, (low, low_time, high, high_time) in bounds.items():
        res.append(
            str(range_start)
            + ", "
            + ("-".center(7) if low is None else util.to_b36(low))
            + "-"
            + ("-".center(7) if high is None else util.to_b36(high))
            + ", "
            + str(low_time)
            + "-"
            + str(high_time)
            + ("" if low is None or high is None else ", space: " + str(high - low))
        )
    return "\n".join(res)


def search(limit: int):
    if limit == 0:
        logger.info("Done, exiting")
        return

    request_ranges = []
    """Holds (low, high) tuples representing ranges of IDs to request between"""
    curr_start = None
    for range_start in time_ranges:
        low, _, high, _ = bounds[range_start]
        if low is not None:
            curr_start = low
        else:
            assert curr_start is not None, f"{range_start=!s}, bounds={bounds_str()}"

        if high is not None:
            assert (
                curr_start is not None
            ), f"{range_start=!s}, {high=}, bounds={bounds_str()}"
            if curr_start < high:
                request_ranges.append((curr_start, high))
                curr_start = high

    if len(request_ranges) == 0:
        logger.info(f"Done with {limit} iterations to go!")
        return

    logger.debug(
        str([f"{util.to_b36(low)}-{util.to_b36(high)}" for low, high in request_ranges])
    )

    num_requests = [0] * len(request_ranges)
    """Number of requests allotted per range"""
    num_left = 100
    while num_left > 0:
        added = False
        for i, (start, end) in enumerate(request_ranges):
            if num_left == 0:
                break
            if num_requests[i] + 1 < end - start:
                num_requests[i] += 1
                num_left -= 1
                added = True
        if not added:
            break

    ids = []
    for i, (n, (start, end)) in enumerate(zip(num_requests, request_ranges)):
        if start == end:
            continue
        ids.extend(range(start, end, (end - start) // (n + 1)))
    fullnames = ["t1_" + util.to_b36(id).lower() for id in ids]

    comments = list(reddit.info(fullnames=fullnames))
    for comment in comments:
        id = int(comment.id, 36)
        unix_timestamp: int = comment.created_utc
        time = datetime.datetime.fromtimestamp(unix_timestamp, tz=datetime.timezone.utc)

        if id in NOT_THIS_FUCKING_ID_AGAIN:
            continue

        for range_start in bounds.keys():
            low, low_time, high, high_time = bounds[range_start]
            if time < range_start:
                if low is None or low < id:
                    assert (
                        high is None or id <= high
                    ), f"id={util.to_b36(id)} {time=!s}, {range_start=!s} bounds={bounds_str()}"
                    low = id
                    low_time = time
            elif range_start < time:
                if high is None or id < high:
                    assert (
                        low is None or low <= id
                    ), f"id={util.to_b36(id)} {time=!s}, {range_start=!s} bounds:\n{bounds_str()}"
                    high = id
                    high_time = time
            else:
                # This comment's good enough
                low = id
                low_time = time
                high = id
                high_time = time
            bounds[range_start] = low, low_time, high, high_time

    if len(comments) == 0:
        logger.error("No comments!")
        return
    else:
        logger.info(f"New bounds:")
        logger.info(bounds_str())

    search(limit - 1)


search(25)

logger.info(bounds_str())

print(f"timeStart: {time_ranges[0].date()}")
print(f"timeStep: {time_step} # months")
print(f"timeRanges:")
for i, range_start in enumerate(time_ranges):
    low, low_time, high, high_time = bounds[range_start]
    assert low is not None and high is not None, f"{range_start=}"
    if low == high:
        id = low
        comment = f"Time: {low_time}"
    elif low + 1 == high:
        assert low_time < range_start < high_time
        id = high
        comment = f"Time: {high_time}"
    else:
        id = (low + high) // 2
        comment = f"Average of {util.to_b36(low)} ({low_time}) and {util.to_b36(high)} ({high_time})"

    if i < len(time_ranges) - 1:
        print(f"- start: {util.to_b36(id)} # {comment}")
        print(f"  min: 200 # default")
    else:
        print(f"  end: {util.to_b36(id)} # {comment}")

"""
Example output for `python find_ids.py 2008-01-01 2024-01-01 6 c020000 k000000`:

2008-01-01, c02sbfd-c02sbfe, 2007-12-31 23:59:49-2008-01-01 00:00:03, space: 1
2008-07-01, c04jg0y-c04jg0z, 2008-06-30 23:59:49-2008-07-01 00:00:02, space: 1
2009-01-01, c06vwuk-c06vwum, 2008-12-18 00:54:46-2009-01-07 13:00:28, space: 2
2009-07-01, c0apqto-c0apqto, 2009-07-01 00:00:00-2009-07-01 00:00:00, space: 0
2010-01-01, c0i1e1i-c0i1e1j, 2009-12-31 23:59:57-2010-01-01 00:00:01, space: 1
2010-07-01, c0t9vme-c0t9vme, 2010-07-01 00:00:00-2010-07-01 00:00:00, space: 0
2011-01-01, c1b0v8e-c1b0v8e, 2011-01-01 00:00:00-2011-01-01 00:00:00, space: 0
2011-07-01, c22yg5s-c22yg5s, 2011-07-01 00:00:00-2011-07-01 00:00:00, space: 0
2012-01-01, c3cw038-c3cw038, 2012-01-01 00:00:00-2012-01-01 00:00:00, space: 0
2012-07-01, c57w3y2-c57w3y2, 2012-07-01 00:00:00-2012-07-01 00:00:00, space: 0
2013-01-01, c7p3da4-c7p3da4, 2013-01-01 00:00:00-2013-01-01 00:00:00, space: 0
2013-07-01, catksx2-catksx2, 2013-07-01 00:00:00-2013-07-01 00:00:00, space: 0
2014-01-01, ceel87h-ceel87h, 2014-01-01 00:00:00-2014-01-01 00:00:00, space: 0
2014-07-01, cilgesd-cilgesd, 2014-07-01 00:00:00-2014-07-01 00:00:00, space: 0
2015-01-01, cnaz5qj-cnaz5qj, 2015-01-01 00:00:00-2015-01-01 00:00:00, space: 0
2015-07-01, csocche-csocche, 2015-07-01 00:00:00-2015-07-01 00:00:00, space: 0
2016-01-01, cyi1yvt-cyi1yvt, 2016-01-01 00:00:00-2016-01-01 00:00:00, space: 0
2016-07-01, d4uxl05-d4uxl05, 2016-07-01 00:00:00-2016-07-01 00:00:00, space: 0
2017-01-01, dbuwt32-dbuwt32, 2017-01-01 00:00:00-2017-01-01 00:00:00, space: 0
2017-07-01, djmur0k-djmur0k, 2017-07-01 00:00:00-2017-07-01 00:00:00, space: 0
2018-01-01, ds0z2ys-ds0z2ys, 2018-01-01 00:00:00-2018-01-01 00:00:00, space: 0
2018-07-01, e1l4h8q-e1l4h8q, 2018-07-01 00:00:00-2018-07-01 00:00:00, space: 0
2019-01-01, ecztaeb-ecztaeb, 2019-01-01 00:00:00-2019-01-01 00:00:00, space: 0
2019-07-01, esghzwj-esghzwj, 2019-07-01 00:00:00-2019-07-01 00:00:00, space: 0
2020-01-01, fcp98eu-fcp98eu, 2020-01-01 00:00:00-2020-01-01 00:00:00, space: 0
2020-07-01, fwjti6k-fwjti6k, 2020-07-01 00:00:00-2020-07-01 00:00:00, space: 0
2021-01-01, ghoeh1e-ghoeh1e, 2021-01-01 00:00:00-2021-01-01 00:00:00, space: 0
2021-07-01, h3n80zq-h3n80zq, 2021-07-01 00:00:00-2021-07-01 00:00:00, space: 0
2022-01-01, hqruo2y-hqruo2y, 2022-01-01 00:00:00-2022-01-01 00:00:00, space: 0
2022-07-01, ieesd59-ieesd59, 2022-07-01 00:00:00-2022-07-01 00:00:00, space: 0
2023-01-01, j2gxjqp-j2gxjqp, 2023-01-01 00:00:00-2023-01-01 00:00:00, space: 0
2023-07-01, jq7yt2u-jq7yt2u, 2023-07-01 00:00:00-2023-07-01 00:00:00, space: 0
2024-01-01, jzzzzzz-k000000, 2023-09-10 14:31:47-None, space: 1
"""
