"""Conservative RDAP registrable-domain candidate derivation."""

from __future__ import annotations


def rdap_parent_candidate(hostname: str) -> str | None:
    """Return a conservative registrable-domain candidate without PSL data.

    Two-letter ccTLDs keep one additional label (example.co.uk) to avoid
    querying a bare public-suffix-like pair such as co.uk. This intentionally
    prefers false negatives over broad false-positive RDAP queries.
    """
    labels = hostname.strip(".").lower().split(".")
    if len(labels) < 2 or any(not label for label in labels):
        return None
    tld = labels[-1]
    if len(tld) == 2:
        if len(labels) < 3:
            return None
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])
