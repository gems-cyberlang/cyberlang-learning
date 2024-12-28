"""
Take the comments.csv and missed-ids.txt files from runs from the old data
collector and put them into the SQLite database.

This will append to your existing data
"""

import os
import pandas as pd

from util import *

conn = create_db_conn()

out_dir = os.path.join(os.path.dirname(__file__), "out")
for run_dir in glob("run_*", root_dir=out_dir):
    run_dir = os.path.join(out_dir, run_dir)
    comments = pd.read_csv(os.path.join(run_dir, "comments.csv"))
    comments[ID] = comments[ID].map(lambda id: int(id, 36))
    comments[AUTHOR_ID] = comments[AUTHOR_ID].map(
        lambda id: int(id, 36) if isinstance(id, str) else id
    )
    comments = comments.rename(columns={"parent_fullname": PARENT_ID})
    comments[PARENT_ID] = comments[PARENT_ID].map(
        lambda id: int(id.removeprefix("t1_"), 36)
    )
    comments[POST_ID] = comments[POST_ID].map(
        lambda id: int(id.removeprefix("t3_"), 36)
    )
    comments.to_sql(COMMENTS_TABLE, conn, if_exists="append", index=False)

    with open(os.path.join(run_dir, "missed-ids.txt"), "r") as f:
        misses = [int(id, 36) for id in f.readlines()]
        misses_df = pd.DataFrame({"id": misses})
        misses_df.to_sql(MISSES_TABLE, conn, if_exists="append", index=False)
