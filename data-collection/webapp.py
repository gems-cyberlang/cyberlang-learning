from io import StringIO
import logging
import pandas as pd
import selectors
import socket
import streamlit as st

logger = logging.Logger("dashboard")

sel = selectors.DefaultSelector()

hits_graph = st.empty()
misses_graph = st.empty()


def read(conn: socket.socket):
    data = conn.recv(1024)
    if not data:
        logger.error(f"Closing {conn} (reason: got empty message)")
        sel.unregister(conn)
        conn.close()
        raise Exception("Done")

    logger.warning(f"Got {data!r}")
    draw_graph(data.decode())


def draw_graph(csv: str):
    df = pd.read_csv(StringIO(csv))
    hits_graph.bar_chart(df, x="Date", y="Hits")
    misses_graph.bar_chart(df, x="Date", y="Misses")


client_sock = socket.socket()
client_sock.connect(("", 1234))
client_sock.setblocking(False)
sel.register(client_sock, selectors.EVENT_READ, read)

while True:
    events = sel.select()
    print(events)
    for key, _mask in events:
        print(key)
        callback = key.data
        callback(key.fileobj)
