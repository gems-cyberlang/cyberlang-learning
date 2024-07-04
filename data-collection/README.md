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

## Output

This will create an `out` directory to put all output, and within that, each run
`n` (0-indexed) will create a new `run_$n` directory. Inside that will be 4 files:
- `comments.csv`: All comments collected in that run
- `missed-ids.txt`: IDs that happened to be for deleted/inaccessible comments
- `program_data.json`: Currently just contains timestamp
- `run.log`: Logs for that run

It will look at how many IDs were requested in previous runs to figure out where
it left off last time.
