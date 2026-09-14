"""Frozen search calibration packs and deterministic seeded query synthesis."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping, Sequence

from creeper.distributed.identity import stable_identity


_ALLOWED_FIELDS = frozenset(
    {"anchor", "source", "era", "topology", "language", "operator"}
)


def _clean_terms(
    values: Sequence[str],
    *,
    name: str,
    allow_blank: bool = False,
) -> tuple[str, ...]:
    cleaned: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        item = value.strip()
        if not item and not allow_blank:
            continue
        cleaned.append(item)
    result = tuple(cleaned)
    if not result:
        raise ValueError(f"search campaign dimension is empty: {name}")
    if len(set(result)) != len(result):
        raise ValueError(f"search campaign dimension contains duplicates: {name}")
    return result


@dataclass(frozen=True)
class SearchCampaign:
    """One centrally calibrated immutable search vocabulary."""

    name: str
    templates: tuple[str, ...]
    anchors: tuple[str, ...]
    source_terms: tuple[str, ...]
    era_terms: tuple[str, ...]
    topology_terms: tuple[str, ...]
    language_terms: tuple[str, ...] = ("",)
    operators: tuple[str, ...] = ("",)
    version: str = "search-campaign-v1"

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.version.strip():
            raise ValueError("search campaign name/version are required")
        if not self.templates:
            raise ValueError("search campaign requires at least one template")
        for template in self.templates:
            if not template.strip():
                raise ValueError("search campaign template must be non-empty")
            fields = {
                part.split("}", 1)[0]
                for part in template.split("{")[1:]
                if "}" in part
            }
            unknown = fields - _ALLOWED_FIELDS
            if unknown:
                raise ValueError(
                    "unsupported search campaign placeholders: "
                    + ",".join(sorted(unknown))
                )

        object.__setattr__(
            self,
            "anchors",
            _clean_terms(self.anchors, name="anchors"),
        )
        object.__setattr__(
            self,
            "source_terms",
            _clean_terms(self.source_terms, name="source_terms"),
        )
        object.__setattr__(
            self,
            "era_terms",
            _clean_terms(self.era_terms, name="era_terms"),
        )
        object.__setattr__(
            self,
            "topology_terms",
            _clean_terms(self.topology_terms, name="topology_terms"),
        )
        object.__setattr__(
            self,
            "language_terms",
            _clean_terms(
                self.language_terms,
                name="language_terms",
                allow_blank=True,
            ),
        )
        object.__setattr__(
            self,
            "operators",
            _clean_terms(
                self.operators,
                name="operators",
                allow_blank=True,
            ),
        )

    @property
    def campaign_id(self) -> str:
        return stable_identity("search-campaign", self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name.strip(),
            "version": self.version.strip(),
            "templates": list(self.templates),
            "anchors": list(self.anchors),
            "source_terms": list(self.source_terms),
            "era_terms": list(self.era_terms),
            "topology_terms": list(self.topology_terms),
            "language_terms": list(self.language_terms),
            "operators": list(self.operators),
        }

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "SearchCampaign":
        def values(key: str, default: tuple[str, ...] | None = None) -> tuple[str, ...]:
            value = raw.get(key, default)
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"search campaign {key} must be a list")
            return tuple(str(item) for item in value)

        return cls(
            name=str(raw.get("name", "")),
            version=str(raw.get("version", "search-campaign-v1")),
            templates=values("templates"),
            anchors=values("anchors"),
            source_terms=values("source_terms"),
            era_terms=values("era_terms"),
            topology_terms=values("topology_terms"),
            language_terms=values("language_terms", ("",)),
            operators=values("operators", ("",)),
        )

    def _pick(self, dimension: str, values: tuple[str, ...], seed: int, slot: int) -> str:
        digest = hashlib.sha256(
            f"{self.campaign_id}\0{int(seed)}\0{int(slot)}\0{dimension}".encode(
                "utf-8"
            )
        ).digest()
        index = int.from_bytes(digest[:8], "big") % len(values)
        return values[index]

    def render(self, *, seed: int, slot: int) -> str:
        if int(seed) < 0 or int(slot) < 0:
            raise ValueError("search campaign seed/slot must be non-negative")
        template = self._pick("template", self.templates, seed, slot)
        parts = {
            "anchor": self._pick("anchor", self.anchors, seed, slot),
            "source": self._pick("source", self.source_terms, seed, slot),
            "era": self._pick("era", self.era_terms, seed, slot),
            "topology": self._pick("topology", self.topology_terms, seed, slot),
            "language": self._pick("language", self.language_terms, seed, slot),
            "operator": self._pick("operator", self.operators, seed, slot),
        }
        query = " ".join(template.format(**parts).split())
        if not query:
            raise ValueError("rendered search query is empty")
        return query

    def render_slice(
        self,
        *,
        seed: int,
        slot_start: int,
        slot_count: int,
    ) -> tuple[str, ...]:
        if slot_start < 0 or slot_count < 1:
            raise ValueError("invalid search slice")
        return tuple(
            self.render(seed=seed, slot=slot)
            for slot in range(slot_start, slot_start + slot_count)
        )
