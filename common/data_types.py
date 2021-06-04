#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, FrozenSet

from common.timestamp import Timestamp
from common.time_interval import Days, Seconds


__ALL__ = ["TaskInstance", "UnixtimeAssignments"]


@dataclass
class TaskInstance:
    task_id: str
    period_id: Timestamp

    def __hash__(self) -> int:
        # Note, the existence of __hash__ implies that TaskInstances
        # should always be immutable
        return hash((self.task_id, self.period_id.unixtime))

    def __eq__(self, other: TaskInstance) -> bool:
        return (self.task_id == other.task_id) and (self.period_id == other.period_id)

    @property
    def unique_task(self) -> UniqueTask:
        return UniqueTask(
            self.task_id,
            Seconds((self.period_id - self.period_id.midnight).seconds),
        )


@dataclass
class UniqueTask:
    task_id: str
    offset: Seconds

    def __post_init__(self) -> None:
        if self.offset >= Days(1):
            raise ValueError(
                f'A unique task offset: {self.offset.seconds} greater than a day makes no sense.'
            )

    def __hash__(self) -> int:
        # Note, the existence of __hash__ implies that UniqueTask
        # should always be immutable
        return hash((self.task_id, self.offset.seconds))


UnixtimeAssignments = Dict[TaskInstance, Timestamp]
UnscheduledTasks = FrozenSet[TaskInstance]
