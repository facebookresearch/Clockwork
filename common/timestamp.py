#!/usr/bin/env python3
# pyre-strict
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import datetime
import time
from typing import Any, overload
import sys

from common.time_interval import Days, Seconds, TimeInterval


__ALL__ = [
    "Timestamp",
    "Now",
    "TwentyFourHoursFromNow",
    "MidnightToday",
    "TimeZero",
]

# This is only used for Printing the str representation of a timestamp


class Timestamp(object):
    def __init__(self, unixtime: int) -> None:
        if unixtime < 0:
            raise InvalidTime(f"Unixtime cannot be negative: {unixtime}")
        elif unixtime > 32503680000:
            raise InvalidTime(f"Unixtime greater year 3000: {unixtime}")
        self._time = int(unixtime)

    @property
    def midnight(self) -> int:
        """
        For this example code we will assume midnight is 0
        """
        return Timestamp(0)

    @property
    def unixtime(self) -> int:
        return self._time

    @property
    def ds(self) -> str:
        return self._localized_time().strftime("%Y-%m-%d")

    def _to_str(self) -> str:
        if self.unixtime < 100_000:  # useful for testing
            return f"Timestamp({self.unixtime})"
        return self._localized_time().strftime("%Y-%m-%d %H:%M:%S %Z%z")

    def __repr__(self) -> str:
        return self._to_str()

    def __hash__(self) -> int:
        return hash(self.unixtime)

    def __eq__(self, other: Timestamp) -> bool:
        return self.unixtime == other.unixtime

    def __lt__(self, other: Timestamp) -> bool:
        return self.unixtime < other.unixtime

    def __le__(self, other: Timestamp) -> bool:
        return self.unixtime <= other.unixtime

    def __gt__(self, other: Timestamp) -> bool:
        return self.unixtime > other.unixtime

    def __ge__(self, other: Timestamp) -> bool:
        return self.unixtime >= other.unixtime

    def __add__(self, other: TimeInterval) -> Timestamp:
        return Timestamp(self.unixtime + other.seconds)

    def __radd__(self, other: TimeInterval) -> Timestamp:
        return Timestamp(self.unixtime + other.seconds)

    @overload
    def __sub__(self, other: TimeInterval) -> Timestamp:
        pass

    @overload
    def __sub__(self, other: Timestamp) -> TimeInterval:
        pass

    def __sub__(self, other):  # pyre-fixme
        if isinstance(other, Timestamp):
            return Seconds(self.unixtime - other.unixtime)
        else:
            return Timestamp(self.unixtime - other.seconds)

    def __hash__(self) -> int:
        return self.unixtime


class TimeZero(Timestamp):
    """
    Jan 1st 1970 Midnight UTC
    """

    def __init__(self) -> None:
        super().__init__(0)


class InvalidTime(Exception):
    pass
