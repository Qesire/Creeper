"""Curated source-research memory for bounded discovery agents.

This module records only search-level knowledge established by reproducible
public source research.  It never grants source admission, evidence authority,
novelty, or submission authority.  Entries are intentionally compact so they
can be copied into the child-agent context without exposing runtime state.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final


SOURCE_RESEARCH_MEMORY: Final = (
    MappingProxyType(
        {
            "lead_id": "ucb-home-ip-1996-public-trace",
            "status": "REJECT_IDENTITY_LOSS",
            "mechanism": "client-side HTTP packet trace",
            "target_period": "1996-11",
            "identity": (
                "public trace irreversibly anonymizes destination server IP "
                "and request URL with keyed hashes"
            ),
            "instruction": (
                "do not re-propose the public UCB Home IP trace as a hostname "
                "source; seek a different acquisition mechanism instead"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "dec-proxy-v1.2-public-trace",
            "status": "REJECT_IDENTITY_LOSS",
            "mechanism": "proxy HTTP request trace",
            "target_period": "1996",
            "identity": (
                "distributed trace replaces server names and URL components "
                "with opaque identifiers without a public reverse mapping"
            ),
            "instruction": (
                "do not spend search budget rediscovering the distributed DEC "
                "trace for hostname extraction"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "nlanr-uc-20000714",
            "status": "RECOVER_PUBLIC_MIRROR",
            "mechanism": "sanitized Squid/proxy access log",
            "target_period": "2000-07-14",
            "identity": "published trace name uc.sanitized-access.20000714",
            "instruction": (
                "search exact filename and institutional mirrors; verify the "
                "download still preserves requested URL/server identity before proposing"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "canetii-19990919-20",
            "status": "RECOVER_PUBLIC_MIRROR",
            "mechanism": "sanitized Squid/proxy access log",
            "target_period": "1999-09-19/20",
            "identity": (
                "published filenames access.1999-09-19.gz and "
                "access.1999-09-20.gz"
            ),
            "instruction": (
                "search exact filenames and mirrors of the historical CA*netII "
                "rawlogs; verify requested URL preservation before proposing"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "bu98flt",
            "status": "RECOVER_PUBLIC_MIRROR",
            "mechanism": "client-proxy HTTP request trace",
            "target_period": "1998-04-06/1998-05-21",
            "identity": "published trace label bu98flt",
            "instruction": (
                "search Internet Traffic Archive/W3C successors and research "
                "mirrors for the actual trace, then inspect destination identity"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "dmoz-2001-content",
            "status": "RECOVER_PUBLIC_MIRROR",
            "mechanism": "human-curated web directory export",
            "target_period": "2001-01",
            "identity": "historical content.rdf.u8.gz dump",
            "instruction": (
                "find a provenance-safe independent January 2001 dump or "
                "research-held copy; modern 2013/2016 mirrors are off-window"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "ripe-isc-historical-hostcount",
            "status": "RECOVER_PUBLIC_MIRROR",
            "mechanism": "DNS hostcount/zone enumeration",
            "target_period": "1996-2001",
            "identity": (
                "historical summaries remain public; raw per-host output or "
                "mirrors are the useful candidate reservoir"
            ),
            "instruction": (
                "search RIPE/ISC mirrors and cited raw-output repositories; do "
                "not mistake aggregate count reports for hostname data"
            ),
        }
    ),
    MappingProxyType(
        {
            "lead_id": "nus-nlanr-sample",
            "status": "HOLD_PROVENANCE",
            "mechanism": "proxy trace teaching mirror",
            "target_period": "unknown",
            "identity": (
                "live NUS mirror exposes 10k/200k NLANR sample packages and a "
                "converted server/path trace, but the source date is not stated"
            ),
            "instruction": (
                "determine the originating NLANR cache/date before proposing "
                "the sample as a target-period source"
            ),
        }
    ),
)


def source_research_memory() -> tuple[dict[str, str], ...]:
    """Return JSON-safe copies for inclusion in one bounded agent request."""
    return tuple(dict(item) for item in SOURCE_RESEARCH_MEMORY)
