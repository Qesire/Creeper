"""Finite query-family expansion for deterministic exploration regions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from itertools import product
from typing import Any


class QueryFamilyExpansionError(ValueError):
    """Raised when a finite family exceeds its pre-I/O expansion bound."""


@dataclass(frozen=True)
class QueryFamily:
    """A finite Cartesian product rendered through one format template."""

    template: str
    dimensions: Mapping[str, tuple[Any, ...]]

    def __post_init__(self) -> None:
        if not isinstance(self.template, str) or not self.template:
            raise ValueError("query-family template must be non-empty")
        normalized: dict[str, tuple[Any, ...]] = {}
        for name, values in self.dimensions.items():
            if not isinstance(name, str) or not name:
                raise ValueError("query-family dimension names must be non-empty")
            if (
                isinstance(values, (str, bytes))
                or not isinstance(values, Iterable)
                or not hasattr(values, "__len__")
            ):
                raise ValueError(
                    f"dimension {name!r} must be a finite sized container"
                )
            materialized = tuple(values)
            if not materialized:
                raise ValueError(f"dimension {name!r} must not be empty")
            normalized[name] = materialized
        object.__setattr__(self, "dimensions", normalized)

    @property
    def cardinality(self) -> int:
        result = 1
        for values in self.dimensions.values():
            result *= len(values)
        return result

    def expand(self, *, max_queries: int | None = None) -> tuple[str, ...]:
        cardinality = self.cardinality
        if max_queries is not None and cardinality > max_queries:
            raise QueryFamilyExpansionError(
                f"query family cardinality {cardinality} "
                f"exceeds max_queries={max_queries}"
            )
        names = tuple(self.dimensions)
        if not names:
            return (self.template,)
        try:
            return tuple(
                self.template.format(**dict(zip(names, values, strict=True)))
                for values in product(*(self.dimensions[name] for name in names))
            )
        except (KeyError, IndexError, ValueError) as exc:
            raise ValueError("query-family template cannot be rendered") from exc

    @classmethod
    def from_mapping(
        cls, template: str, dimensions: Mapping[str, Iterable[Any]]
    ) -> "QueryFamily":
        return cls(
            template=template,
            dimensions={name: tuple(values) for name, values in dimensions.items()},
        )
