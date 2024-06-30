import pandas as pd
import streamlit as st

from runner import gems_runner


def run_app(runner: gems_runner):
    hits_graph = st.empty()
    misses_graph = st.empty()
    while runner.run_step():
        df = pd.DataFrame(
            [
                [str(time_range.start_date), time_range.min, time_range.hits, time_range.misses]
                for time_range in runner.time_ranges.bins
            ],
            columns=["Date", "Min", "Hits", "Misses"],
        )
        hits_graph.bar_chart(df, x="Date", y="Hits")
        misses_graph.bar_chart(df, x="Date", y="Misses")
    st.write("Done")
