#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from abc import ABC
from typing import FrozenSet

from common.data_types import TaskInstance
from common.timestamp import Timestamp


class TaskFetcher(ABC):
    """
    Your __init__ function must have a signature matching

    def __init__(self, *args: List[str], **kwargs: Dict[str, str]) -> None

    for example

    def __init__(self, namespace: str) -> None:
        ...

    """

    async def fetch(self) -> FrozenSet[TaskInstance]:
        """
        This method should be a self-contained callable taking no arguments.
        If a specific subclass TaskFetcher does require arguments,
        they should be passed in through __init__

        fetch() should return all matching and currently-active task instances.
        It is ultimately up to the individual fetcher what that means. One
        simple example might be to find the next-in-line to be scheduled task
        instances matching some filters
        """
        raise NotImplementedError()


class HardCodedTaskFetcher(TaskFetcher):

    async def fetch(self) -> FrozenSet[TaskInstance]:
        return frozenset([
            TaskInstance('task1', Timestamp(0)),
            TaskInstance('task2', Timestamp(0)),
            TaskInstance('task3', Timestamp(0)),
            TaskInstance('task4', Timestamp(10)),
            TaskInstance('task5', Timestamp(10)),
            TaskInstance('task6', Timestamp(10)),
        ])
