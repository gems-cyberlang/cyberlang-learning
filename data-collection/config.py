"""For loading the data collector's config"""

from dataclasses import dataclass
import datetime
import functools
import yaml


CONFIG_FILE_NAME = "config.yaml"


@functools.total_ordering
@dataclass(frozen=True)
class TimeRange:
    start_id: int
    end_id: int
    start_date: datetime.date
    """Only used for displaying data"""
    end_date: datetime.date
    """Only used for displaying data"""
    min_comments: int
    """Stop when we've gotten these many comments"""

    def __lt__(self, other: "TimeRange"):
        return self.start_id < other.start_id

    def __eq__(self, other: "TimeRange"):
        return self.start_id == other.start_id and self.end_id == other.end_id


@dataclass(frozen=True)
class Config:
    start_date: datetime.date
    time_step: int
    """How many months long each time range is"""
    time_ranges: list[TimeRange]

    @staticmethod
    def load(path: str) -> "Config":
        with open(path) as file:
            config = yaml.safe_load(file)
        time_step: int = config["timeStep"]
        time_ranges_raw: list[dict] = config["timeRanges"]
        time_ranges = []
        start_date: datetime.date = config["timeStart"]
        curr_start = start_date
        for i, time_range in enumerate(time_ranges_raw):
            start_id = int(time_range["start"], 36)
            if "end" in time_range:
                end_id = int(time_range["end"], 36)
            elif i + 1 < len(time_ranges_raw):
                end_id = int(time_ranges_raw[i + 1]["start"], 36)
            else:
                raise AssertionError(
                    f"An end ID should've been given for time range {time_range}"
                )
            prev_time = curr_start
            year = curr_start.year + (curr_start.month + time_step) // 12
            month = (curr_start.month + time_step) % 12
            curr_start = curr_start.replace(year=year, month=month)
            time_ranges.append(
                TimeRange(
                    start_date=prev_time,
                    end_date=curr_start,
                    start_id=start_id,
                    end_id=end_id,
                    min_comments=time_range["min"],
                )
            )

        return Config(start_date, time_step, time_ranges)
