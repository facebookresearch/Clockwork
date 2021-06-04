#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import logging
import sys
import traceback

from common.data_types import UnixtimeAssignments
from planner.config import PlannerConfig, TaskPoolConfig

__ALL__ = ["Planner"]

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger: logging.Logger = logging.getLogger(__name__)


class Planner:
    def __init__(self, config: PlannerConfig) -> None:
        self.config: PlannerConfig = config

    async def run(self) -> int:
        ret_code = 0
        try:
            pool = self.config.task_pool
            plan = await self.execute_task_pool(pool)
            await self.config.plan_writer.overwrite_plan(plan)
        except Exception:
            etype, value, tb = sys.exc_info()
            traceback.print_exception(etype, value, tb)
            ret_code = 1
        return ret_code

    @staticmethod
    async def execute_task_pool(pool: TaskPoolConfig) -> UnixtimeAssignments:
        tasks = await pool.task_fetcher.fetch()
        plan = await pool.scheduling_algorithm.run(tasks)
        missing_from_plan = frozenset(tasks - set(plan.keys()))
        logger.debug(
            f"Planning Finished | In Plan: {len(plan)} | Missing from Plan: {len(missing_from_plan)}",
        )
        return plan
