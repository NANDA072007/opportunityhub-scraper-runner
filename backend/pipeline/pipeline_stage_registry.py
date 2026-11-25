"""
PipelineStageRegistry
=====================
Pluggable registry for pipeline processing stages.

The canonical ``PipelineOrchestrator`` runs a fixed sequence of stages
(Fetch, Normalize, Validate, Deduplicate, Quality, Lifecycle, Store, Publish).
This registry makes those stages independently registerable so the sequence
can be extended or re-ordered WITHOUT rewriting the orchestrator.

This module provides ONLY the registry abstraction and the stage interface.
It does not modify ``PipelineOrchestrator``.

Stage contract
--------------
A ``PipelineStage`` exposes a stable ``name``, an ``order`` (lower runs
first), and a ``run(context)`` method that mutates/reads the shared context.
The stage implementation itself owns how it processes the context.
"""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Type, Union

logger = logging.getLogger(__name__)

# Canonical pipeline stage names (matches PipelineOrchestrator sequence).
DEFAULT_STAGES = [
    "fetch",
    "normalize",
    "validate",
    "deduplicate",
    "quality",
    "auto_review",
    "lifecycle",
    "store",
    "publish",
]

# Relative ordering for the default stages. Plugins may place themselves
# between or around them by choosing their own order value.
DEFAULT_STAGE_ORDER: Dict[str, int] = {
    "fetch": 50,
    "normalize": 100,
    "validate": 200,
    "deduplicate": 300,
    "quality": 400,
    "auto_review": 500,
    "lifecycle": 550,
    "store": 600,
    "publish": 700,
}


class StageName(str, Enum):
    """Stable identifiers for the canonical pipeline stages."""

    FETCH = "fetch"
    NORMALIZE = "normalize"
    VALIDATE = "validate"
    DEDUPLICATE = "deduplicate"
    QUALITY = "quality"
    LIFECYCLE = "lifecycle"
    STORE = "store"
    PUBLISH = "publish"


class PipelineStage(ABC):
    """Abstract interface for a pluggable pipeline processing stage."""

    name: str = ""
    order: int = 0

    @abstractmethod
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute this stage against the shared context.

        Args:
            context: Mutable dict shared across stages (records, counters,
                database clients, etc.).

        Returns:
            The (possibly mutated) context.
        """
        ...

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r} order={self.order}>"


@dataclass
class _StageEntry:
    stage: PipelineStage


class PipelineStageRegistry:
    """Singleton registry of pluggable pipeline stages.

    Public methods are classmethods so consumers can call
    ``PipelineStageRegistry.register(...)`` / ``get(...)`` without holding an
    instance, mirroring the ConnectorRegistry convention.
    """

    _instance: ClassVar[Optional["PipelineStageRegistry"]] = None
    _init_lock: ClassVar[threading.Lock] = threading.Lock()

    def __new__(cls) -> "PipelineStageRegistry":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._stages: Dict[str, _StageEntry] = {}
                    instance._lock = threading.RLock()
                    cls._instance = instance
        return cls._instance

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    @classmethod
    def register(
        cls,
        stage: Union[PipelineStage, Type[PipelineStage]],
        *,
        name: Optional[str] = None,
        order: Optional[int] = None,
    ) -> str:
        """Register a stage instance or subclass.

        Args:
            stage: PipelineStage instance, or a subclass instantiated with no
                arguments.
            name: Override the stage name.
            order: Override the execution order (lower runs first).

        Returns:
            The normalized stage name used as the key.
        """
        registry = cls._get_instance()
        if isinstance(stage, type):
            if not issubclass(stage, PipelineStage):
                raise TypeError(
                    f"Unsupported stage type '{stage.__name__}'; expected a "
                    "PipelineStage instance or subclass."
                )
            stage = stage()
        if not isinstance(stage, PipelineStage):
            raise TypeError(
                f"Unsupported stage type '{type(stage).__name__}'; expected a "
                "PipelineStage instance or subclass."
            )
        stage_name = (name or stage.name or "").strip().lower()
        if not stage_name:
            raise ValueError("PipelineStage must declare a non-empty name.")
        stage_order = order if order is not None else stage.order
        with registry._lock:
            previous = registry._stages.get(stage_name)
            if previous is not None and previous.stage is not stage:
                logger.warning(
                    "Overwriting existing pipeline stage registration for '%s'.",
                    stage_name,
                )
            registry._stages[stage_name] = _StageEntry(stage=stage)
        stage.name = stage_name
        stage.order = int(stage_order)
        logger.info("Registered pipeline stage: '%s' (order=%d)", stage_name, stage_order)
        return stage_name

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    @classmethod
    def get(cls, name: str) -> Optional[PipelineStage]:
        """Return a registered stage by name, or None."""
        registry = cls._get_instance()
        entry = registry._stages.get((name or "").strip().lower())
        return entry.stage if entry else None

    @classmethod
    def get_all(cls) -> List[PipelineStage]:
        """Return all registered stages ordered by ``order`` (lowest first)."""
        registry = cls._get_instance()
        with registry._lock:
            stages = [entry.stage for entry in registry._stages.values()]
        stages.sort(key=lambda stage: stage.order)
        return stages

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check whether a stage name is registered."""
        registry = cls._get_instance()
        return (name or "").strip().lower() in registry._stages

    @classmethod
    def stage_names(cls) -> List[str]:
        """Return registered stage names ordered by ``order``."""
        return [stage.name for stage in cls.get_all()]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @classmethod
    def clear(cls) -> None:
        """Remove all registered stages."""
        registry = cls._get_instance()
        with registry._lock:
            registry._stages.clear()

    @classmethod
    def register_defaults(cls) -> None:
        """Register the canonical stage names with their default ordering.

        Each canonical stage is registered as a no-op placeholder so the full
        sequence is present and overridable by real implementations.
        """
        registry = cls._get_instance()
        for name in DEFAULT_STAGES:
            order = DEFAULT_STAGE_ORDER.get(name, 0)
            placeholder = _PlaceholderStage(name=name, order=order)
            with registry._lock:
                registry._stages[name] = _StageEntry(stage=placeholder)
        logger.info("Registered %d default pipeline stages.", len(DEFAULT_STAGES))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @classmethod
    def _get_instance(cls) -> "PipelineStageRegistry":
        return cls()


class _PlaceholderStage(PipelineStage):
    """No-op placeholder used by ``register_defaults``.

    Real stage implementations should register their own classes with the
    same name to override the placeholder.
    """

    def __init__(self, name: str, order: int) -> None:
        self.name = name
        self.order = order

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        return context


# Global stage registry instance.
stage_registry = PipelineStageRegistry()

__all__ = [
    "PipelineStageRegistry",
    "PipelineStage",
    "StageName",
    "stage_registry",
    "DEFAULT_STAGES",
    "DEFAULT_STAGE_ORDER",
]
