import streamlit as st

from runner import gems_runner

def run_app(runner: gems_runner):
    while runner.run_step():
        runner.time_ranges
