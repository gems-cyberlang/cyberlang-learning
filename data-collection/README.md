# Reddit Comment Collector

## Config file

The configuration file is [`config.yaml`](./config.yaml). That will look
something like this:
```yaml
timeStart: 2020-01-01
timeStep: 16 # 16 weeks = 4 months
timeRanges:
  - start: k000000
    min: 100
  - start: ka00000
    min: 500
  - start: kw00000
    min: 400
    end: l000000
```

Each time range specifies the ID at the start of that time range. The ID at the
end of that time range is the same as the next time range's start. Only the last
time range should have an `end` field.

Note: The time ranges need to be sorted. The code could totally work without
them being sorted, but if they are not sorted, that's probably an indication of
a bug.

Each time range also specifies the minimum number of comments that need to be
collected in that time range. This is so we can do that oversampling thing Nesse
suggested.

TODO use find_ids.py to generate the config file

TODO save config file with each run to verify that new runs are compatible with previous runs

## Output

This will create an `out` directory to put all output, and within that, each run
`n` (0-indexed) will create a new `run_$n` directory. Inside that will be 4 files:
- `comments.csv`: All comments collected in that run
- `missed-ids.txt`: IDs that happened to be for deleted/inaccessible comments
- `program_data.json`: Currently just contains timestamp
- `run.log`: Logs for that run

It will look at how many IDs were requested in previous runs to figure out where
it left off last time.

## Running the data collection script

Open up a terminal and make sure you're inside the `data-collection` directory.
You can do this by running `cd ~/cyberlang-learning/data-collection`. The rest of these
instructions assume you're in that directory.

You'll need to install some packages before you can run stuff. Run `pip -r include.txt`
to do that (this looks at the list of packages in [include.txt](./include.txt)).

The script to run is in [`__main__.py`](./__main__.py). Normally, you'd run it with `python __main__.py`,
but `__main__.py` is a special name, so you can also run it with the path to the folder the
script is in. Here, you can run it with `python .` (`.` means "current directory").

It's probably best to run with `python . -v` so you can see all the logs right there.

If someone else did a bunch of runs and you want to pick up where they left off, they can
give you the `program_data.json` file in their latest run's folder, after which you can run
the following:

```sh
python . --prev-file /path/to/that/program_data.json
```

Use the `--help` flag to get help (`python . --help`). Here are the options at the time of writing:
```
 python . --help
usage: . [-h] [--config-file CONFIG_FILE] [--output-dir OUTPUT_DIR] [--env-file ENV_FILE] [--verbose] [--silent] [--overwrite] [--prev-file PREV_FILE]
         [--praw-log {info,debug,warn,error}] [--port PORT]

The Gems Reddit Data collector 9000 turdo

options:
  -h, --help            show this help message and exit
  --config-file CONFIG_FILE, -c CONFIG_FILE
  --output-dir OUTPUT_DIR
                        ouput directory
  --env-file ENV_FILE   the env file to use
  --verbose, -v         will print all logging to screen
  --silent              will log only errors
  --overwrite, -o       if exsting files should be overwritten
  --prev-file PREV_FILE
                        Get number of hits and misses from this file instead of looking at previous runs
  --praw-log {info,debug,warn,error}, -P {info,debug,warn,error}
                        Log level for PRAW output
  --port PORT, -p PORT  Port for the server to listen on
```

## Running the web dashboard

Open another terminal and again `cd` to `~/cyberlang-learning/data-collection`.

Streamlit should've been installed already (it's listed in [include.txt](./include.txt)),
but if it isn't installed, just run `pip install streamlit` to do that.

Run `streamlit run webapp.py` to start the dashboard. You'll see something like this:
```
 streamlit run webapp.py

  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://172.28.59.31:8501
```

Open up either one of those URLs in your browser.

## How the dashboard works

The server runs on port 1234. Every time it makes a request to Reddit and gets a
response back, it makes a CSV string and sends it to the dashboard. Each row of this
CSV corresponds to the data collected for a particular time range so far. The columns
are the start date of the time range, the minimum number of comments needed from that
range, the number of hits so far, and the number of misses so far.

## Collected fields

Example query: https://www.reddit.com/comments/1e8h7ar.json

Fields currently being collected (for comments):
- id
- created_utc
- subreddit: Subreddit name
- author_id
- parent_id
- link_id: Post on which comment was made
- ups: upvotes
- downs: downvotes
- body: Body as Markdown

Possibly interesting fields:
- depth: How deep in the comment tree a comment is
- body_html
- gilded: int
- gildings: object?
- distinguished: ???
- total_awards_received
- collapsed: boolean
- likes: What even is this?
- banned_at_utc, banned_by: Might want to remove comments by banned users?
