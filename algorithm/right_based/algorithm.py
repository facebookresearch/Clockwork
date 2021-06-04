#!/usr/bin/env python3
# pyre-strict
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from typing import Dict
import logging
import sys

from common.data_types import UniqueTask
from common.skyline_math import SkylineTracker
from common.time_interval import TimeInterval
from algorithm.right_based.metadata import RightBasedMetadata

__all__ = ["schedule_tasks"]


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger: logging.Logger = logging.getLogger(__name__)


def schedule_tasks(
    metadata: Dict[UniqueTask, RightBasedMetadata],
    granularity: TimeInterval,
    max_size: float,
) -> Dict[UniqueTask, TimeInterval]:
    global_skyline: SkylineTracker = SkylineTracker(
        granularity=granularity, max_size=max_size
    )

    task_metadata_tuples = sorted(metadata.items(), key=lambda x: x[1], reverse=True)
    assignments = {}
    for i, (task, meta) in enumerate(task_metadata_tuples):
        if i % 1000 == 0:
            logger.debug(f"Scheduled {i}/{len(task_metadata_tuples)}, {len(assignments)} accepted")
        start_time = meta.max_start_time
        while start_time >= meta.min_start_time:
            if global_skyline.can_add(start_time, meta.skyline):
                global_skyline.add_job(start_time, meta.skyline)
                assignments[task] = start_time
                break
            elif can_decrement(start_time, granularity):
                start_time -= granularity
            else:
                break
    return assignments


def can_decrement(time: TimeInterval, interval: TimeInterval) -> bool:
    try:
        _ = time - interval
        return True
    except Exception:
        return False
