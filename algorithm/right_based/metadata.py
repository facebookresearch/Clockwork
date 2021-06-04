#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Tuple

from common.data_types import TaskInstance, UniqueTask
from common.skyline_math import SkylineBlock
from common.time_interval import Seconds


@dataclass
class RightBasedMetadata:
    min_start_time: Seconds
    max_start_time: Seconds
    skyline: List[SkylineBlock]
    order_tuple: Tuple[Seconds, Seconds] = field(init=False)

    def __post_init__(self) -> None:
        self.order_tuple = (self.min_start_time, self.max_start_time)
        assert self.max_start_time >= self.min_start_time

    def __lt__(self, other: RightBasedMetadata) -> bool:
        return self.order_tuple < other.order_tuple

    def __lte__(self, other: RightBasedMetadata) -> bool:
        return self.order_tuple <= other.order_tuple

    def __gt__(self, other: RightBasedMetadata) -> bool:
        return self.order_tuple > other.order_tuple

    def __gte__(self, other: RightBasedMetadata) -> bool:
        return self.order_tuple >= other.order_tuple


async def get_max_spark_resources() -> float:
    return 3


async def get_max_presto_resources() -> float:
    return 3


async def get_presto_metadata(
    tasks: FrozenSet[TaskInstance],
) -> Dict[UniqueTask, RightBasedMetadata]:
    return {
        UniqueTask('task1', Seconds(0)): RightBasedMetadata(
            min_start_time=Seconds(10),
            max_start_time=Seconds(20),
            skyline=[SkylineBlock(Seconds(1), 1.0)],
        ),
        UniqueTask('task5', Seconds(10)):RightBasedMetadata(
            min_start_time=Seconds(15),
            max_start_time=Seconds(35),
            skyline=[SkylineBlock(Seconds(5), 1.0)],
        ) ,
        UniqueTask('task6', Seconds(10)): RightBasedMetadata(
            min_start_time=Seconds(50),
            max_start_time=Seconds(60),
            skyline=[SkylineBlock(Seconds(5), 1.0)],
        ),
    }


async def get_spark_metadata(
    tasks: FrozenSet[TaskInstance],
) -> Dict[UniqueTask, RightBasedMetadata]:
    return {
        UniqueTask('task2', Seconds(0)): RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(100),
                skyline=[SkylineBlock(Seconds(1), 1.0), SkylineBlock(Seconds(2), 2.0)],
        ),
        UniqueTask('task3', Seconds(0)): RightBasedMetadata(
                min_start_time=Seconds(100),
                max_start_time=Seconds(100),
                skyline=[SkylineBlock(Seconds(5), 2.0)],
        ),
        UniqueTask('task4', Seconds(10)): RightBasedMetadata(
                min_start_time=Seconds(19),
                max_start_time=Seconds(59),
                skyline=[SkylineBlock(Seconds(4), 4.0)],
        ),
    }
