from abc import ABC, abstractmethod
from collections import deque
import datetime
import itertools
import numpy as np
from typing import Generic, Optional, TypeVar

SIZE_OF_ITERATION = 1000000

class AbstractBin(ABC):
    @property
    @abstractmethod
    def start_id(self) -> int:
        pass

    @property
    @abstractmethod
    def end_id(self) -> int:
        pass

    @abstractmethod
    def requested(self) -> int:
        """How many IDs in this time range we've requested"""

    @abstractmethod
    def unrequested(self) -> int:
        """
        How many IDs in this range have not been requested yet.

        Note: This has nothing to do with the minimum number of IDs that must be
        requested in a time range.
        """

    def needed(self) -> int:
        """How many comments we still need from this range"""
        return 0

    @abstractmethod
    def notify_requested(self, id: int, hit: bool):
        """
        Record the fact that we requested the given ID.

        If `hit` is true, we were able to read the comment
        """

    @abstractmethod
    def next_ids(self, n: int) -> list[int]:
        """
        Generate the next n IDs in this range.

        For `BinBin`s, this modifies state (though not for `PermBin`s)
        """

    def __contains__(self, id: int) -> bool:
        """Is this necessary? No. But I've always wanted to overload `in`, so please let me have this"""
        return self.start_id <= id < self.end_id


class PermBin(AbstractBin):
    """For keeping track of work done in a range of IDs within a `TimeRange`. Each
    `TimeRange` is made up of a bunch of `PermBin`s"""

    def __init__(self, start: int, end: int):
        """
        # Arguments
        * `start` - First ID in this range (inclusive)
        * `end` - End of this range (exclusive)
        * `hits` - How

        """
        self._start_id = start
        self._end_id = end
        self.hits = 0
        """Number of IDs that we requested and actually got"""
        self.misses = 0
        """Number of IDs that we requested but turned out to be inaccessible"""

    def requested(self) -> int:
        """How many comments have been requested in this bin so far"""
        return self.hits + self.misses

    def unrequested(self) -> int:
        total_available = self.end_id - self.start_id
        return total_available - self.requested()

    def next_ids(self, n: int) -> list[int]:
        perm = np.random.default_rng(seed=[self.start_id, self.end_id]).permutation(
            np.arange(start=self.start_id, stop=self.end_id, dtype=np.uint64)
        )
        return list(map(int, perm[self.requested() : self.requested() + n]))

    def notify_requested(self, id: int, hit: bool):
        assert id in self, f"{id} not in {self}"
        if hit:
            self.hits += 1
        else:
            self.misses += 1

    @property
    def start_id(self):
        return self._start_id

    @property
    def end_id(self):
        return self._end_id


T = TypeVar("T", bound=AbstractBin)


class BinBin(Generic[T], AbstractBin):
    """A bin containing other bins (of type `T`)"""

    def __init__(self, bins: list[T]):
        self.bins = bins
        self._remaining = deque(self.bins)
        self._update_remaining()

    def requested(self) -> int:
        return sum(bin.requested() for bin in self.bins)

    def unrequested(self) -> int:
        return sum(bin.unrequested() for bin in self.bins)

    def find_bin(self, id: int) -> Optional[T]:
        """Find the bin that the given ID goes into (None if it doesn't go into any of the bins)"""
        if id in self:
            for bin in self.bins:
                if id in bin:
                    return bin
            raise AssertionError(
                f"ID {id} should have fit into one of the bins in {self}"
            )
        else:
            return None

    def notify_requested(self, id: int, hit: bool):
        bin = self.find_bin(id)
        assert bin is not None, f"{id} not in {self}"
        bin.notify_requested(id, hit)

    def next_ids(self, n: int) -> list[int]:
        """
        Get the next n IDs.

        Every time this is called, it will rotate through the remaining bins
        """
        self._update_remaining()

        # If we have more than n bins, only need to look at the first n
        num_bins = min(n, len(self._remaining))
        front_bins = list(itertools.islice(self._remaining, num_bins))
        if num_bins > len(self._remaining):
            self._remaining.rotate(-num_bins)

        # Number of IDs to request from each remaining bin
        num_ids = [1] * len(front_bins)

        print(f"got front_bins {len(front_bins)}")

        while sum(num_ids) < n and any(
            bin.unrequested() > num_ids[i] for i, bin in enumerate(front_bins)
        ):
            for i, bin in enumerate(front_bins):
                if bin.unrequested() > num_ids[i]:
                    num_ids[i] += 1
                    if sum(num_ids) == n:
                        break

        return list(
            itertools.chain.from_iterable(
                bin.next_ids(n) for bin, n in zip(front_bins, num_ids)
            )
        )

    def _update_remaining(self):
        self._remaining = deque(bin for bin in self._remaining if bin.unrequested() > 0)


class TimeRange(BinBin):
    """For keeping track of work done in a time range"""

    def __init__(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        start_id: int,
        end_id: int,
        min_comments: int,
    ):
        """
        # Arguments
        * `start_date` and `end_date` - Only used for displaying data.
        * `start_id` - First ID in this range (inclusive)
        * `end_id` - End of this range (exclusive)
        * `min_comments` - Minimum number of comments to collect in this time range.

        """
        self.start_date = start_date
        self.end_date = end_date
        self._start_id = start_id
        self._end_id = end_id
        self.min = min_comments

        super().__init__(
            [
                PermBin(
                    bin_start,
                    min(bin_start + SIZE_OF_ITERATION, self.end_id),
                )
                for bin_start in range(self.start_id, self.end_id, SIZE_OF_ITERATION)
            ]
        )

    def needed(self) -> int:
        return max(0, self.min - self.requested())

    @property
    def start_id(self):
        return self._start_id

    @property
    def end_id(self):
        return self._end_id


U = TypeVar("U", bound=BinBin)


class BinBinBin(BinBin[U]):
    """Couldn't think of a better name"""

    def __init__(self, bins: list[U]):
        super().__init__(bins)
        assert all(bins[i].end_id == bins[i + 1].start_id for i in range(len(bins) - 1))
        self._start_id = bins[0].start_id
        self._end_id = bins[len(bins) - 1].end_id

    @property
    def start_id(self):
        return self._start_id

    @property
    def end_id(self):
        return self._end_id
