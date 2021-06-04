#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

import logging
import sys

from common.data_types import UnixtimeAssignments


__ALL__ = ["PlanWriter"]

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger: logging.Logger = logging.getLogger(__name__)


class PlanWriter:
    """
    This class contains methods that writes the Clockwork plans to a production datastore
    """

    def __init__(self) -> None:
        pass

    async def overwrite_plan(self, plan: UnixtimeAssignments) -> None:
        logger.debug(f'Final Plan: {plan}')
