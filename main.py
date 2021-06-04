#!/usr/bin/env python3
# pyre-stricts
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.s


import asyncio

from planner.config import (
    PlannerConfig,
    get_algorithm,
    get_task_fetcher,
)
from planner.plan_writer import PlanWriter
from planner.planner import Planner


"""
This file is not production code and is not gauranteed
to always run or be up to date. It is just a useful tool
for Varun to test the planner without modifying
any production code.
"""


def main() -> int:
    config = PlannerConfig(
        task_fetcher=get_task_fetcher("hard_coded"),
        scheduling_algorithm=get_algorithm("right_based"),
        plan_writer=PlanWriter(),
    )
    planner = Planner(config=config)
    return asyncio.run(planner.run())


if __name__ == "__main__":
    """
    Runs one iteration of the planner.
    """
    exit(main())
