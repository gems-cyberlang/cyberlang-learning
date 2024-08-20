"""For loading the data collector's config"""

from bins import TimeRange

import datetime
import yaml


CONFIG_FILE_NAME = "config.yaml"


class Config:
    def __init__(
        self,
        start_date: datetime.date,
        time_step: int,
        time_ranges: list[TimeRange],
        path: str,
    ):
        self.start_date = start_date
        self.time_step = time_step
        """How many months long each time range is"""
        self._time_ranges = time_ranges
        self.path = path
        """Path to the config file"""

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

        return Config(start_date, time_step, time_ranges, path)

    @property
    def time_ranges(self) -> list[TimeRange]:
        return [time_range.copy() for time_range in self._time_ranges]
