#!/usr/bin/env python
"""
Find IDs for a bunch of timestamps and generate a config based on that
"""

import argparse
import datetime
import logging
import math
import numpy as np
from typing import Optional
import util

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
    description="The Gems Reddit Data collector 9000 turdo"
)
parser.add_argument("time_first", help="Start of first time range")
parser.add_argument("time_last", help="End of last time range")
parser.add_argument("time_step", help="Length of each time range, in months", type=int)
parser.add_argument("start_id", help="Lower bound on ID (inclusive)")
parser.add_argument("end_id", help="Upper bound on ID (inclusive)")
args = parser.parse_args()

time_first = datetime.datetime.fromisoformat(args.time_first)
time_last = datetime.datetime.fromisoformat(args.time_last)
time_step = args.time_step

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

bounds: dict[datetime.datetime, tuple[Optional[int], Optional[int]]] = {
    range_start: (None, None) for range_start in time_ranges
}
bounds[time_first] = (low, None)
bounds[time_ranges[len(time_ranges) - 1]] = (None, high)

# todo avoid re-requesting IDs that were misses

# todo there are almost certainly off-by-ones lurking around here


def bounds_str():
    res = []
    for range_start, (low, high) in bounds.items():
        res.append(
            str(range_start.date())
            + ", "
            + ("-" if low is None else util.to_b36(low))
            + ", "
            + ("-" if high is None else util.to_b36(high))
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
        low, high = bounds[range_start]
        if low is not None:
            curr_start = low
        else:
            assert curr_start is not None, f"{range_start=}, bounds={bounds_str()}"

        if high is not None:
            assert (
                curr_start is not None
            ), f"{range_start=}, {high=}, bounds={bounds_str()}"
            request_ranges.append((curr_start, high))
            curr_start = high

    logger.debug(
        str([f"{util.to_b36(low)}-{util.to_b36(high)}" for low, high in request_ranges])
    )

    num_requests = [0] * len(request_ranges)
    """Number of requests allotted per range"""
    num_left = 100
    while num_left > 0:
        for i, (start, end) in enumerate(request_ranges):
            if num_left == 0:
                break
            if num_requests[i] + 1 < end - start:
                num_requests[i] += 1
                num_left -= 1

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
        time = datetime.datetime.fromtimestamp(unix_timestamp)

        for range_start in bounds.keys():
            low, high = bounds[range_start]
            if time < range_start:
                if low is None or low < id:
                    assert (
                        high is None or id <= high
                    ), f"{id=}, {range_start=} {bounds=}"
                    low = id
            elif range_start < time:
                if high is None or id < high:
                    assert low is None or low <= id, f"{id=}, {range_start=} {bounds=}"
                    high = id
            else:
                # This comment's good enough
                low = id
                high = id
            bounds[range_start] = low, high

    if len(comments) == 0:
        logger.error("No comments!")
        return
    else:
        logger.info(f"New bounds:")
        logger.info(bounds_str())

    search(limit - 1)


search(20)

logger.info("Done!")
logger.info(bounds_str())

"""
Example output:
2008-01-01, c02sbfd, c02sbfe
2008-03-01, c03biqt, c03biqt
2008-05-01, c03vir4, c03vir5
2008-07-01, c04jg0y, c04jg0z
2008-09-01, c058yua, c058yua
2008-11-01, c064jdt, c064jdu
2009-01-01, c06vwuk, c06vwum
2009-03-01, c07yn96, c07yn97
2009-05-01, c098svx, c098svy
2009-07-01, c0apqto, c0apqto
2009-09-01, c0cn3fy, c0cn3fz
2009-11-01, c0f6v74, c0f6v75
2010-01-01, c0i1e1i, c0i1e1j
2010-03-01, c0ld5e5, c0ld5e5
2010-05-01, c0p7jbw, c0p7jbw
2010-07-01, c0t9vme, c0t9vme
2010-09-01, c0y8lw2, c0y8lw2
2010-11-01, c141zi3, c141zi3
2011-01-01, c1b0v8e, c1b0v8e
2011-03-01, c1isfw7, c1isfw7
2011-05-01, c1rtn0n, c1rtn0n
2011-07-01, c22yg5q, c22yg5q
2011-09-01, c2gojmv, c2gojmv
2011-11-01, c2w1g4c, c2w1g4c
2012-01-01, c3cw037, c3cw037
2012-03-01, c3whsiz, c3whsiz
2012-05-01, c4im2yx, c4im2yx
2012-07-01, c57w3y1, c57w3y1
2012-09-01, c61sjpt, c61sjqc
2012-11-01, c6uovjd, c6uovjy
2013-01-01, c7p3d9x, c7p3dai
2013-03-01, c8np15h, c8np15h
2013-05-01, c9q3oei, c9q3oei
2013-07-01, catkswp, catksxa
2013-09-01, cbzf66v, cbzf66v
2013-11-01, cd482ug, cd484zf
2014-01-01, ceel86f, ceel87l
2014-03-01, cfrj67y, cfrj67y
2014-05-01, ch6m47h, ch6m47h
2014-07-01, cilges8, cilges8
2014-09-01, ck5znw5, ck5znw5
2014-11-01, clprctn, clprdcv
2015-01-01, cnaz5pt, cnaz5r0
2015-03-01, cp0fol1, cp0nabr
2015-05-01, cquovau, cquovau
2015-07-01, csoccfq, csocchi
2015-09-01, cumohyt, cumohyt
2015-11-01, cwk4wfb, cwk4wtq
2016-01-01, cyi1s5c, cyi24z9
2016-03-01, d0j0z9w, d0j0zdh
2016-05-01, d2niw2i, d2osiiw
2016-07-01, d4uxkz0, d4uxl0s
2016-09-01, d74zmls, d74zmnl
2016-11-01, d9gew9e, d9gewb7
2017-01-01, dbuwt1t, dbuwt3l
2017-03-01, dectjcz, dectjcz
2017-05-01, dgz0ctg, dgz37gy
2017-07-01, djmuqzr, djmur2g
2017-09-01, dmesrjg, dmesrm5
2017-11-01, dp6cz18, dp6cz3x
2018-01-01, drzzzzg, ds7lqpt
2018-03-01, dv02e2g, dv02e55
2018-05-01, dy928i6, dy928kv
2018-07-01, e1l12h6, e1l9mfq
2018-09-01, e572fe8, e572hss
2018-11-01, e8ty12t, e8ty12t
2019-01-01, ecztael, ecztael
2019-03-01, ehj7u9w, ehj8wrq
2019-05-01, em76lww, em76m2b
2019-07-01, esgge8f, esgij82
2019-09-01, eycldvj, eyrsvc9
2019-11-01, f53x5bj, f7n623z
2020-01-01, fc2unhw, fcpnvp0
"""
