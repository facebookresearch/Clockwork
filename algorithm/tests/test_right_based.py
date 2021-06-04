#!/usr/bin/env python3
# pyre-strict
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import unittest

from algorithm.right_based.algorithm import schedule_tasks
from algorithm.right_based.metadata import RightBasedMetadata
from common.data_types import UniqueTask
from common.skyline_math import SkylineBlock
from common.time_interval import Seconds


class TestParallelGraph(unittest.TestCase):

    # Two jobs set to run at the same time where only one can run at a time
    def test_two_nodes_same_start_time(self) -> None:
        skyline = [SkylineBlock(Seconds(1), 1)]
        task_a = UniqueTask("task_a", Seconds(0))
        task_b = UniqueTask("task_b", Seconds(0))
        pool = {
            task_a: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(1),
                skyline=skyline,
            ),
            task_b: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(1),
                skyline=skyline,
            ),
        }
        res = schedule_tasks(pool, granularity=Seconds(1), max_size=1)
        self.assertIn(
            res,
            [
                {
                    task_a: Seconds(0),
                    task_b: Seconds(1),
                },
                {
                    task_a: Seconds(1),
                    task_b: Seconds(0),
                },
            ],
        )

class TestTandemGraph(unittest.TestCase):

    # Test a linked list of nodes
    def test_two_tasks_back_to_back(self) -> None:
        skyline = [SkylineBlock(Seconds(1), 1)]
        first = UniqueTask("first", Seconds(0))
        second = UniqueTask("second", Seconds(0))
        pool = {
            first: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(0),
                skyline=skyline
            ),
            second: RightBasedMetadata(
                min_start_time=Seconds(1),
                max_start_time=Seconds(1),
                skyline=skyline
            ),
        }
        res = schedule_tasks(pool, granularity=Seconds(1), max_size=2)
        self.assertEquals(
            res,
            {
                first: Seconds(0),
                second: Seconds(1)
            },
        )

class TestStaggardSkylines(unittest.TestCase):

    # Check that two skyline blocks can be stacked on top of each other
    def test_overlapping_skylines(self) -> None:
        skyline = [
            SkylineBlock(Seconds(1), 1),
            SkylineBlock(Seconds(1), 2),
        ]

        reflected_sykline = [
            SkylineBlock(Seconds(1), 2),
            SkylineBlock(Seconds(1), 1),
        ]

        task_a = UniqueTask("first", Seconds(0))
        task_b = UniqueTask("second", Seconds(0))
        pool = {
            task_a: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(0),
                skyline=skyline
            ),
            task_b: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(0),
                skyline=reflected_sykline
            )
        }
        res = schedule_tasks(pool, granularity=Seconds(1), max_size=3)
        self.assertIn(
            res,
            [
                {
                    task_a: Seconds(0),
                    task_b: Seconds(0),
                },
            ]
        )

    # these two skylines cannot be partially stacked
    def test_back_to_back(self) -> None:
        skyline = [
            SkylineBlock(Seconds(1), 1),
            SkylineBlock(Seconds(1), 2),
        ]

        task_a = UniqueTask("first", Seconds(0))
        task_b = UniqueTask("second", Seconds(0))
        pool = {
            task_a: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(1),
                skyline=skyline
            ),
            task_b: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(1),
                skyline=skyline
            )
        }
        res = schedule_tasks(pool, granularity=Seconds(1), max_size=3)
        self.assertIn(
            res,
            [
                {
                    task_a: Seconds(0),
                    task_b: Seconds(1),
                },
                {
                    task_a: Seconds(1),
                    task_b: Seconds(0),
                },
            ]
        )

class Infeasible(unittest.TestCase):

    def test_infeasible_simple(self) -> None:
        skyline = [SkylineBlock(Seconds(1), 1)]
        task_a = UniqueTask("task_a", Seconds(0))
        task_b = UniqueTask("task_b", Seconds(0))
        pool = {
            task_a: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(0),
                skyline=skyline,
            ),
            task_b: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(0),
                skyline=skyline,
            ),
        }
        res = schedule_tasks(pool, granularity=Seconds(1), max_size=1)
        self.assertIn(
            res,
            [
                {
                    task_a: Seconds(0),
                },
                {
                    task_b: Seconds(0),
                },
            ],
        )

    def test_infesible_complex(self) -> None:
        skyline = [SkylineBlock(Seconds(1), 1)]
        start_at_9a = UniqueTask("start_at_9a", Seconds(0))
        start_at_9b = UniqueTask("start_at_9b", Seconds(0))
        start_whenever = UniqueTask("start_whenever", Seconds(0))
        start_between_8_and_10 = UniqueTask("start_between_8_and_10", Seconds(0))
        pool = {
            start_at_9a: RightBasedMetadata(
                min_start_time=Seconds(9),
                max_start_time=Seconds(9),
                skyline=skyline,
            ),
            start_at_9b: RightBasedMetadata(
                min_start_time=Seconds(9),
                max_start_time=Seconds(9),
                skyline=skyline,
            ),
            start_whenever: RightBasedMetadata(
                min_start_time=Seconds(0),
                max_start_time=Seconds(10),
                skyline=skyline,
            ),
            start_between_8_and_10: RightBasedMetadata(
                min_start_time=Seconds(8),
                max_start_time=Seconds(10),
                skyline=skyline,
            ),
        }
        res = schedule_tasks(pool, granularity=Seconds(1), max_size=1)
        self.assertIn(
            res,
            [
                {
                    start_between_8_and_10: Seconds(10),
                    start_at_9a: Seconds(9),
                    start_whenever: Seconds(8),
                },
                {
                    start_between_8_and_10: Seconds(10),
                    start_at_9b: Seconds(9),
                    start_whenever: Seconds(8),
                },
            ],
        )
