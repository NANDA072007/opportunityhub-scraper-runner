"""
Connector Capabilities
======================
Enum-based capability model describing what a source connector can produce.

Capabilities are declared by each connector (via the ``capabilities`` class
attribute on ``BaseConnector``) and power analytics, routing, and future
recommendation signals. They carry ZERO business logic.

Lives in its own module (rather than inside ``connector_registry``) so that
``base_connector`` can reference it without creating a circular import.
"""

from __future__ import annotations

from enum import Enum
from typing import Iterable, List


class Capability(str, Enum):
    """Supported source capabilities. Values are stable wire strings."""

    JOB = "job"
    INTERNSHIP = "internship"
    COMPETITION = "competition"
    RESEARCH = "research"
    SCHOLARSHIP = "scholarship"
    FELLOWSHIP = "fellowship"
    GOVERNMENT = "government"
    FREELANCE = "freelance"
    REMOTE = "remote"
    HYBRID = "hybrid"
    ONSITE = "onsite"


def capability_names(
    capabilities: Iterable[Capability],
) -> List[str]:
    """Normalize an iterable of Capability members into sorted stable strings."""
    names: List[str] = []
    for capability in capabilities:
        if isinstance(capability, Capability):
            names.append(capability.value)
        else:
            names.append(str(capability))
    return sorted(set(names))


def parse_capabilities(values: Iterable[str]) -> List[Capability]:
    """Convert stable string values back into Capability members."""
    result: List[Capability] = []
    for value in values:
        try:
            result.append(Capability(str(value)))
        except ValueError:
            continue
    return result


__all__ = [
    "Capability",
    "capability_names",
    "parse_capabilities",
]
