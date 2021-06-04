#!/usr/bin/self.env python3
# pyre-strict
# pyre-ignore-all-errors[29]
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from enum import Enum
from datetime import timedelta


class TimeInterval(object):

    def __init__(self, value: int, unit: TimeUnit) -> None:
        self._time = timedelta(seconds=0)
        self._set_time(int(value), unit)

    @property
    def seconds(self) -> int:
        return self.get_time_scalar(TimeUnit.SECONDS)

    @property
    def minutes(self) -> int:
        return self.get_time_scalar(TimeUnit.MINUTES)

    @property
    def hours(self) -> int:
        return self.get_time_scalar(TimeUnit.HOURS)

    @property
    def days(self) -> int:
        return self.get_time_scalar(TimeUnit.DAYS)

    def get_time_scalar(self, unit: TimeUnit) -> int:
        return int(self._time.total_seconds()) // self._seconds_to_multiple(unit)

    def rescale(self, output_unit: TimeUnit) -> TimeInterval:
        return TimeInterval(self.get_time_scalar(output_unit), output_unit)

    def __repr__(self) -> str:
        return str(self._time)

    def __eq__(self, other: TimeInterval) -> bool:
        return other._time == self._time

    def __lt__(self, other: TimeInterval) -> bool:
        return self._time < other._time

    def __le__(self, other: TimeInterval) -> bool:
        return self._time <= other._time

    def __gt__(self, other: TimeInterval) -> bool:
        return self._time > other._time

    def __add__(self, other: TimeInterval) -> TimeInterval:
        total_time_delta = self._time + other._time
        total_seconds = int(total_time_delta.total_seconds())
        return TimeInterval(total_seconds, TimeUnit.SECONDS)

    def __sub__(self, other: TimeInterval) -> TimeInterval:
        if other._time > self._time:
            raise InvalidTimeAmount(
                "{} - {} results in negative time. Use the timedelta class.",
                self, other,
            )
        total_time_delta = self._time - other._time
        total_seconds = int(total_time_delta.total_seconds())
        return TimeInterval(total_seconds, TimeUnit.SECONDS)

    def __hash__(self) -> int:
        return self.seconds

    def _seconds_to_multiple(self, unit: TimeUnit) -> int:
        if unit == TimeUnit.SECONDS:
            return 1
        if unit == TimeUnit.MINUTES:
            return 60
        if unit == TimeUnit.HOURS:
            return 3600
        elif unit == TimeUnit.DAYS:
            return 3600 * 24
        else:
            raise NotImplementedError('Programmer failed to add a enum option.')

    def _set_time(self, value: int, unit: TimeUnit) -> None:
        if value < 0:
            raise InvalidTimeAmount(
                "Invalid input " + str(value) + " time is negative."
            )

        if unit == TimeUnit.SECONDS:
            self._time = timedelta(seconds=value)
        elif unit == TimeUnit.MINUTES:
            self._time = timedelta(minutes=value)
        elif unit == TimeUnit.HOURS:
            self._time = timedelta(hours=value)
        elif unit == TimeUnit.DAYS:
            self._time = timedelta(days=value)
        else:
            raise NotImplementedError('Programmer failed to add a enum option.')


class Seconds(TimeInterval):

    def __init__(self, seconds: int) -> None:
        super().__init__(seconds, TimeUnit.SECONDS)


class Minutes(TimeInterval):

    def __init__(self, minutes: int) -> None:
        super().__init__(minutes, TimeUnit.MINUTES)


class Hours(TimeInterval):

    def __init__(self, hours: int) -> None:
        super().__init__(hours, TimeUnit.HOURS)


class Days(TimeInterval):

    def __init__(self, days: int) -> None:
        super().__init__(days, TimeUnit.DAYS)


class TimeUnit(Enum):
    SECONDS = 's'
    MINUTES = 'm'
    HOURS = 'hr'
    DAYS = 'd'


class InvalidTimeAmount(Exception):
    pass
