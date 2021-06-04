#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from algorithm.algorithm import (
    SchedulingAlgorithm,
    NullAlgorithm,
    ReturnZero,
    RightBased,
)
from algorithm.task_fetchers import (
    TaskFetcher,
    HardCodedTaskFetcher,
)

from planner.plan_writer import PlanWriter


__ALL__ = [
    "PlannerConfig",
    "TaskPoolConfig",
    "get_algorithm",
    "get_task_fetcher",
]

def get_algorithm(short_name: str, *args, **kwargs) -> SchedulingAlgorithm:
    """
    This is the official registry of all available planning algorithms
    """
    registry = {
        "do_nothing": NullAlgorithm,
        "return_zero": ReturnZero,
        "right_based": RightBased,
    }
    return registry[short_name](*args, **kwargs)


def get_task_fetcher(short_name: str, *args, **kwargs) -> TaskFetcher:
    """
    This is the official registry of all available task fetchers
    """
    registry = {
        "hard_coded": HardCodedTaskFetcher,
    }
    return registry[short_name](*args, **kwargs)


@dataclass
class TaskPoolConfig:
    task_fetcher: TaskFetcher
    scheduling_algorithm: SchedulingAlgorithm


class PlannerConfig:
    def __init__(
        self,
        task_fetcher: TaskFetcher,
        scheduling_algorithm: SchedulingAlgorithm,
        plan_writer: Optional[PlanWriter] = None,
    ) -> None:
        self.task_pool: TaskPoolConfig = TaskPoolConfig(
            task_fetcher, scheduling_algorithm
        )
        self.plan_writer: Optional[PlanWriter] = plan_writer
