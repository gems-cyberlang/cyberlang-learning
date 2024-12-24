import argparse
from io import StringIO
import logging
import pandas as pd
import selectors
import socket
import streamlit as st

logger = logging.Logger("dashboard")

parser = argparse.ArgumentParser(description="Web dashboard for data collector")
parser.add_argument(
    "--port",
    "-p",
    help="Port that the server runs on",
    default=1234,
    type=int,
)
args = parser.parse_args()

sel = selectors.DefaultSelector()

st.set_page_config(page_title="Data collector dashboard", layout="wide")

col1, col2 = st.columns(2)

col1_elems = col1.container()
hits_graph = col1_elems.empty()
misses_graph = col1_elems.empty()

col2_elems = col2.container()
percent_graph = col2_elems.empty()
hit_rate_graph = col2_elems.empty()


def read(conn: socket.socket):
    data = conn.recv(2048)
    if not data:
        logger.error(f"Closing {conn} (reason: got empty message)")
        sel.unregister(conn)
        conn.close()
        logger.info("Done")
        exit()

    logger.debug(f"Got {data!r}")
    draw_graph(data.decode())


def draw_graph(csv: str):
    df = pd.read_csv(StringIO(csv))
    df["Hit rate"] = df["Hits"] / (df["Hits"] + df["Misses"])
    df["Percent"] = df["Hits"] / df["Total"]
    hits_graph.bar_chart(df, x="Date", y="Hits")
    misses_graph.bar_chart(df, x="Date", y="Misses")
    percent_graph.bar_chart(df, x="Date", y="Percent")
    hit_rate_graph.bar_chart(df, x="Date", y="Hit rate")


client_sock = socket.socket()
client_sock.connect(("", args.port))
client_sock.setblocking(False)
sel.register(client_sock, selectors.EVENT_READ, read)

while True:
    events = sel.select()
    for key, _mask in events:
        callback = key.data
        callback(key.fileobj)
