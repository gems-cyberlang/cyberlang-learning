from time import sleep
import streamlit as st

from config import Config
import util

st.set_page_config(page_title="Data collector dashboard", layout="wide")

col1, col2 = st.columns(2)

col1_elems = col1.container()
hits_graph = col1_elems.empty()
misses_graph = col1_elems.empty()

col2_elems = col2.container()
percent_graph = col2_elems.empty()
hit_rate_graph = col2_elems.empty()

cfg = Config.load()


def update_graph():
    comments, misses = util.load_data()
    df = util.time_range_stats(comments, misses, cfg.time_ranges)

    df["total"] = df["end_id"] - df["start_id"]
    df["Hit rate"] = df["hits"] / (df["hits"] + df["misses"])
    df["Percent"] = df["hits"] / df["total"]

    df = df.rename(columns={"start_date": "Date"})
    hits_graph.bar_chart(df, x="Date", y="hits")
    misses_graph.bar_chart(df, x="Date", y="misses")
    percent_graph.bar_chart(df, x="Date", y="Percent")
    hit_rate_graph.bar_chart(df, x="Date", y="Hit rate")


while True:
    update_graph()
    sleep(2)
