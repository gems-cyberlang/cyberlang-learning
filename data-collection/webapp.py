import pandas as pd
import streamlit as st
import time

from runner import gems_runner


def run_app(runner: gems_runner):
    time.sleep(1)
    with st.empty():
        while runner.run_step():
            df = pd.DataFrame(
                [
                    [str(time_range.start_date), time_range.hits]
                    for time_range in runner.time_ranges.bins
                ],
                columns=["Date", "Hits"],
            )
            st.bar_chart(df, x="Date", y="Hits")
