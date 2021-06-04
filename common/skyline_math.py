#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import operator
from dataclasses import dataclass
from typing import Callable, Dict, Generator, List

from common.time_interval import Seconds, TimeInterval


@dataclass
class SkylineBlock:
    duration: TimeInterval
    size: float

    def __post_init__(self):
        assert self.size >= 0, "Skyline size must be positive"


class SkylineBoundsExceeded(Exception):
    pass


class SkylineTracker:
    """
    This abstraction represents a global skyline of jobs that have
    been scheduled to run at particular points in time.

    This abstraction implicitly expects non-negative values for all
    SkylineBlocks (i.e. the allowable range of a skyline is bounded
    by zero_baseline and max_size). Any operation that exceeds these
    bounds will raise an SkylineBoundsExceeded error
    """

    def __init__(self, granularity: TimeInterval, max_size: float) -> None:
        self._granularity = granularity
        self._max_size = max_size
        self.time_series: Dict[TimeInterval, float] = {}

    @property
    def granularity(self) -> TimeInterval:
        return self._granularity

    def can_add(self, start_time: TimeInterval, blocks: List[SkylineBlock]) -> bool:
        try:
            self._get_updated_time_series(start_time, blocks, operator.add)
            return True
        except Exception:
            return False

    def can_remove(self, start_time: TimeInterval, blocks: List[SkylineBlock]) -> bool:
        try:
            self._get_updated_time_series(start_time, blocks, operator.sub)
            return True
        except Exception:
            return False

    def add_job(self, start_time: TimeInterval, blocks: List[SkylineBlock]) -> None:
        self.time_series = self._get_updated_time_series(
            start_time, blocks, operator.add
        )

    def remove_job(self, start_time: TimeInterval, blocks: List[SkylineBlock]) -> None:
        self.time_series = self._get_updated_time_series(
            start_time, blocks, operator.sub
        )

    def _get_updated_time_series(
        self,
        start_time: TimeInterval,
        blocks: List[SkylineBlock],
        op: Callable[[float, float], float],
    ) -> Dict[TimeInterval, float]:
        return self._merge_time_series(
            self.time_series, self._make_time_series(start_time, blocks), op
        )

    def _merge_time_series(
        self,
        series_a: Dict[TimeInterval, float],
        series_b: Dict[TimeInterval, float],
        op: Callable[[float, float], float],
    ) -> Dict[TimeInterval, float]:
        all_times = set(series_a.keys()).union(series_b)
        new_time_series = {}
        for time in all_times:
            new_size = op(series_a.get(time, 0), series_b.get(time, 0))
            new_time_series[time] = new_size
            if not (0 <= new_size <= self._max_size):
                raise SkylineBoundsExceeded()
        return new_time_series

    def _make_time_series(
        self, start_time: TimeInterval, blocks: List[SkylineBlock]
    ) -> Dict[TimeInterval, float]:
        time = start_time
        block_to_time_bins = []
        for block in blocks:
            start = self._bin(time)
            inclusive_end = self._bin(time + block.duration - Seconds(1))
            bins = list(
                self._time_range(start, inclusive_end, self.granularity, inclusive=True)
            )
            block_to_time_bins.append((block, bins))
            time += block.duration

        time_series = {}
        for block, bins in block_to_time_bins:
            for t_bin in bins:
                time_series[t_bin] = max(time_series.get(t_bin, 0), block.size)
        return time_series

    def _bin(self, time: TimeInterval) -> TimeInterval:
        return Seconds(
            (time.seconds // self._granularity.seconds) * self._granularity.seconds
        )

    def _time_range(
        self,
        start: TimeInterval,
        end: TimeInterval,
        step: TimeInterval,
        inclusive: bool = False,
    ) -> Generator[TimeInterval, None, None]:
        time = start
        while time < end or (inclusive and time == end):
            yield time
            time += step
