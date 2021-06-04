#!/usr/bin/env python3
# pyre-strict
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import asyncio
from abc import ABC
from typing import FrozenSet
import logging
import sys

from algorithm.right_based import (
    algorithm as rb_algo,
    metadata as rb_meta,
)
from common.data_types import TaskInstance, UnixtimeAssignments
from common.time_interval import Minutes
from common.timestamp import Timestamp

__ALL__ = ["SchedulingAlgorithm", "DummyTestPlan", "NullAlgorithm"]

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger: logging.Logger = logging.getLogger(__name__)


class SchedulingAlgorithm(ABC):
    """
    Your __init__ function must have a signature matching

    def __init__(self, *args: List[str], **kwargs: Dict[str, str]) -> None

    for example

    def __init__(self, namespace: str) -> None:
        ...

    """

    async def run(self, tasks: FrozenSet[TaskInstance]) -> UnixtimeAssignments:
        """
        If a specific subclass of SchedulingAlgorithm does requires arguments in
        addition to the list of tasks, they should be passed in through __init__

        run() should return a Dict of size `len(tasks)` which maps each task
        instance to a valid unixtime. The unixtime outputted here will be
        used as the dispatch time in the Dataswarm Scheduler service.
        """
        raise NotImplementedError()


class RightBased(SchedulingAlgorithm):
    async def run(self, tasks: FrozenSet[TaskInstance]) -> UnixtimeAssignments:
        granularity = Minutes(1)
        (
            spark_metadata,
            presto_metadata,
            spark_max_size,
            presto_max_size,
        ) = await asyncio.gather(
            rb_meta.get_spark_metadata(tasks),
            rb_meta.get_presto_metadata(tasks),
            rb_meta.get_max_spark_resources(),
            rb_meta.get_max_presto_resources(),
        )

        logger.debug(f'Presto Metadata Size {len(presto_metadata)}')
        presto_plan = rb_algo.schedule_tasks(
            presto_metadata, granularity=granularity, max_size=presto_max_size
        )
        logger.debug(f'Presto Plan Size {len(presto_plan)}')

        logger.debug(f'Spark Metadata Size {len(spark_metadata)}')
        spark_plan = rb_algo.schedule_tasks(
            spark_metadata, granularity=granularity, max_size=spark_max_size
        )
        logger.debug(f'Spark Metadata Size {len(spark_plan)}')

        plan = {}
        for task_instance in tasks:
            unique_task = task_instance.unique_task
            if unique_task in spark_plan:
                plan[task_instance] = task_instance.period_id.midnight + spark_plan[unique_task]
            elif unique_task in presto_plan:
                plan[task_instance] = task_instance.period_id.midnight + presto_plan[unique_task]
        return plan


class NullAlgorithm(SchedulingAlgorithm):
    async def run(self, tasks: FrozenSet[TaskInstance]) -> UnixtimeAssignments:
        return {}


class ReturnZero(SchedulingAlgorithm):
    """
    Schedules every task to run on January 1st 1970 (effectively 0 delay)
    """

    async def run(self, tasks: FrozenSet[TaskInstance]) -> UnixtimeAssignments:
        return {task: Timestamp(0) for task in tasks}
