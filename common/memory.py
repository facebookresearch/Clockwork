#!/usr/bin/self.env python3
# pyre-strict
# pyre-ignore-all-errors[29]
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import annotations

from enum import Enum
from typing import Union


class Memory(object):
    def __init__(self, value: Union[float, int], unit: MemoryUnit) -> None:
        self._set_bytes(value, unit)

    @property
    def B(self) -> int:
        return self.get_memory_scalar(MemoryUnit.B)

    @property
    def KB(self) -> int:
        return self.get_memory_scalar(MemoryUnit.KB)

    @property
    def MB(self) -> int:
        return self.get_memory_scalar(MemoryUnit.MB)

    @property
    def GB(self) -> int:
        return self.get_memory_scalar(MemoryUnit.GB)

    @property
    def TB(self) -> int:
        return self.get_memory_scalar(MemoryUnit.TB)

    def get_memory_scalar(self, unit: MemoryUnit) -> int:
        return self._bytes // self._base_unit_multiple(unit)

    def rescale(self, output_unit: MemoryUnit) -> Memory:
        return Memory(self.get_memory_scalar(output_unit), output_unit)

    def get_mem_str(self, unit: MemoryUnit) -> str:
        return str(self.get_memory_scalar(unit)) + unit.value

    def __repr__(self) -> str:
        if self.B >= 1024 ** 4:
            return self.get_mem_str(unit=MemoryUnit.GB)
        elif self.B >= 1024 ** 3:
            return self.get_mem_str(unit=MemoryUnit.MB)
        elif self.B >= 1024 ** 2:
            return self.get_mem_str(unit=MemoryUnit.KB)
        return self.get_mem_str(unit=MemoryUnit.B)

    def __bool__(self) -> bool:
        return self > B(0)

    def __eq__(self, other: Memory) -> bool:
        return self.B == other.B

    def __lt__(self, other: Memory) -> bool:
        return self.B < other.B

    def __le__(self, other: Memory) -> bool:
        return self.B <= other.B

    def __gt__(self, other: Memory) -> bool:
        return self.B > other.B

    def __ge__(self, other: Memory) -> bool:
        return self.B >= other.B

    def __add__(self, other: Memory) -> Memory:
        return Memory(other.B + self.B, MemoryUnit.B)

    def __mul__(self, other: Union[float, int]) -> Memory:
        return Memory(other * self.B, MemoryUnit.B)

    def __rmul__(self, other: Union[float, int]) -> Memory:
        return self * other

    def __sub__(self, other: Memory) -> Memory:
        if other.B > self.B:
            raise InvalidMemoryAmount(f"{self} - {other} results in negative memory.")
        return Memory(self.B - other.B, MemoryUnit.B)

    def _set_bytes(self, value: Union[float, int], input_unit: MemoryUnit) -> None:
        if value < 0:
            raise InvalidMemoryAmount("Memory must always be positive.")
        elif value in {float("inf"), float("NaN"), float("-inf")}:
            raise InvalidMemoryAmount(f"Memory must be real: {value}")
        self._bytes = int(value * self._base_unit_multiple(input_unit))

    def _base_unit_multiple(self, unit: MemoryUnit) -> int:
        if unit == MemoryUnit.B:
            return 1
        elif unit == MemoryUnit.KB:
            return 1024
        elif unit == MemoryUnit.MB:
            return 1024 ** 2
        elif unit == MemoryUnit.GB:
            return 1024 ** 3
        elif unit == MemoryUnit.TB:
            return 1024 ** 4
        else:
            raise NotImplementedError("Programmer failed to add a enum option.")


class B(Memory):
    def __init__(self, B: Union[float, int]) -> None:
        super().__init__(B, MemoryUnit.B)


class KB(Memory):
    def __init__(self, KB: Union[float, int]) -> None:
        super().__init__(KB, MemoryUnit.KB)


class MB(Memory):
    def __init__(self, MB: Union[float, int]) -> None:
        super().__init__(MB, MemoryUnit.MB)


class GB(Memory):
    def __init__(self, GB: Union[float, int]) -> None:
        super().__init__(GB, MemoryUnit.GB)


class TB(Memory):
    def __init__(self, TB: Union[float, int]) -> None:
        super().__init__(TB, MemoryUnit.TB)


class TB(Memory):
    def __init__(self, TB: Union[float, int]) -> None:
        super().__init__(TB, MemoryUnit.TB)


def str_to_memory(mem_repr: str) -> Memory:
    """
    Input Examples: "100mb", "22gb", "8tb", "0", "8.5 TB"
    """
    mem_repr = mem_repr.upper().strip()
    if mem_repr == "0":
        return Memory(0, unit=MemoryUnit.B)

    units = [
        MemoryUnit.TB,
        MemoryUnit.GB,
        MemoryUnit.MB,
        MemoryUnit.KB,
        MemoryUnit.B,  # Bytes must go last, b/c every other unit ends in B
    ]
    for unit in units:
        if mem_repr.endswith(unit.value):
            try:
                digit = mem_repr[: -len(unit.value)].strip()
                value = float(digit)
            except ValueError:
                raise InvalidMemoryInput(f"Memory value is not a number: {digit}")
            return Memory(value, unit)
    raise InvalidMemoryInput(f"Invalid memory units. Input: {mem_repr}")


class MemoryUnit(Enum):
    B = "B"
    KB = "KB"
    MB = "MB"
    GB = "GB"
    TB = "TB"


class InvalidMemoryAmount(ValueError):
    pass


class InvalidMemoryInput(ValueError):
    pass
