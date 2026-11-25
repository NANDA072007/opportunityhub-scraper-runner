"""
ConnectorRegistry
=================
Production-grade, dependency-injected registry for OpportunityHub source connectors.
Responsible ONLY for connector orchestration:

- Dynamic registration of connector instances, classes, or factories.
- Package-based discovery of connectors without pipeline code changes.
- Validation of every connector BEFORE it is allowed to register.
- Priority-ordered access to registered connectors.
- Runtime enable/disable/deprecate/remove of individual connectors.
- Explicit lifecycle transitions (NEW/HEALTHY/DEGRADED/OFFLINE/DISABLED/DEPRECATED/REMOVED).
- Rich, inspectable connector metadata.
- Per-connector telemetry (fetched/stored/duplicates/latency/error rate).

No business, scraping, taxonomy, or recommendation logic lives here.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, Type, Union

from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors.capabilities import (
    Capability,
    capability_names,
)

logger = logging.getLogger(__name__)

# Default package scanned when no explicit package is supplied to discover().
DEFAULT_CONNECTOR_PACKAGE = "backend.pipeline.connectors"

# Priority levels (higher runs first). Connectors may declare a `priority`
# class attribute; otherwise the registry default is used.
PRIORITY_CRITICAL = 1000
PRIORITY_HIGH = 750
PRIORITY_NORMAL = 500
PRIORITY_LOW = 250
DEFAULT_PRIORITY = PRIORITY_NORMAL

# Health thresholds mirror the legacy BaseSourceConnector behaviour.
DEGRADED_AFTER_FAILURES = 2
OFFLINE_AFTER_FAILURES = 5

# Connectors kept in code but excluded from the enabled set. A disabled
# connector is still registered (visible in health summaries) but
# `get()`/`get_all()` never return it, so the pipeline never fetches it.
#
# Play-listing freeze: re-enabling any slug here REQUIRES updating the
# store description AND the in-app About screen first (see risk_review.md).
DISABLED_SOURCE_SLUGS = frozenset({
    # Scraping disabled: blocked upstream / not covered by the listing.
    "internshala",
    # Legacy duplicate of `greenhouse` that re-fetches the same boards through
    # raw urllib (no SSRF guard, retries, or rate limiting). Keeping it enabled
    # stores a second copy of every Greenhouse job under a different
    # source_slug, which defeats cross-source dedup.
    "greenhouse_lever",
    # Honest-empty stubs: no live upstream exists, so these connectors always
    # return [] and would otherwise report a healthy 0-item run. Re-enable only
    # after a verified live source lands (and re-check the Play listing).
    "aicte",
    "scholarships",
})

# Anything that can be registered: an instance, a subclass, or a zero-arg
# factory callable (the injection seam for connector dependencies).
ConnectorLike = Union[BaseConnector, Type[BaseConnector], Callable[[], BaseConnector]]


class ConnectorLifecycle(str, Enum):
    """Explicit lifecycle states a registered connector can occupy."""

    NEW = "new"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    DISABLED = "disabled"
    DEPRECATED = "deprecated"
    REMOVED = "removed"


# Backward-compatible alias (previously ConnectorHealth).
ConnectorHealth = ConnectorLifecycle


# Explicit lifecycle transition table. A transition NOT listed is invalid.
_LIFECYCLE_TRANSITIONS: Dict[ConnectorLifecycle, Set[ConnectorLifecycle]] = {
    ConnectorLifecycle.NEW: {
        ConnectorLifecycle.HEALTHY,
        ConnectorLifecycle.DEGRADED,
        ConnectorLifecycle.OFFLINE,
        ConnectorLifecycle.DISABLED,
        ConnectorLifecycle.DEPRECATED,
        ConnectorLifecycle.REMOVED,
    },
    ConnectorLifecycle.HEALTHY: {
        ConnectorLifecycle.DEGRADED,
        ConnectorLifecycle.OFFLINE,
        ConnectorLifecycle.DISABLED,
        ConnectorLifecycle.DEPRECATED,
        ConnectorLifecycle.REMOVED,
    },
    ConnectorLifecycle.DEGRADED: {
        ConnectorLifecycle.HEALTHY,
        ConnectorLifecycle.OFFLINE,
        ConnectorLifecycle.DISABLED,
        ConnectorLifecycle.DEPRECATED,
        ConnectorLifecycle.REMOVED,
    },
    ConnectorLifecycle.OFFLINE: {
        ConnectorLifecycle.HEALTHY,
        ConnectorLifecycle.DISABLED,
        ConnectorLifecycle.DEPRECATED,
        ConnectorLifecycle.REMOVED,
    },
    ConnectorLifecycle.DISABLED: {
        ConnectorLifecycle.HEALTHY,
        ConnectorLifecycle.DEGRADED,
        ConnectorLifecycle.OFFLINE,
        ConnectorLifecycle.DEPRECATED,
        ConnectorLifecycle.REMOVED,
    },
    ConnectorLifecycle.DEPRECATED: {
        ConnectorLifecycle.HEALTHY,
        ConnectorLifecycle.REMOVED,
    },
    ConnectorLifecycle.REMOVED: set(),
}


@dataclass
class ConnectorMetadata:
    """Static and runtime metadata for a registered connector."""

    source_slug: str
    display_name: str
    priority: int = DEFAULT_PRIORITY
    enabled: bool = True
    lifecycle: ConnectorLifecycle = ConnectorLifecycle.NEW
    consecutive_failures: int = 0
    source_reputation: int = 85
    capabilities: List[str] = field(default_factory=list)
    version: str = "1.0.0"
    owner: str = "platform"
    supports_incremental_sync: bool = False
    supports_full_sync: bool = True
    estimated_rate_limit: float = 2.0
    registered_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    last_success_at: Optional[str] = None
    last_failure_at: Optional[str] = None
    last_error: Optional[str] = None

    @property
    def health(self) -> ConnectorLifecycle:
        """Backward-compatible accessor: health is the lifecycle state."""
        return self.lifecycle

    def to_dict(self) -> Dict[str, Any]:
        """Serializable dict form for telemetry and dashboards."""
        return {
            "source_slug": self.source_slug,
            "display_name": self.display_name,
            "priority": self.priority,
            "enabled": self.enabled,
            "lifecycle": self.lifecycle.value,
            "health": self.lifecycle.value,
            "consecutive_failures": self.consecutive_failures,
            "source_reputation": self.source_reputation,
            "capabilities": list(self.capabilities),
            "version": self.version,
            "owner": self.owner,
            "supports_incremental_sync": self.supports_incremental_sync,
            "supports_full_sync": self.supports_full_sync,
            "estimated_rate_limit": self.estimated_rate_limit,
            "registered_at": self.registered_at,
            "last_success_at": self.last_success_at,
            "last_failure_at": self.last_failure_at,
            "last_error": self.last_error,
        }


@dataclass
class ConnectorTelemetry:
    """Run-time telemetry counters collected for a registered connector."""

    items_fetched: int = 0
    items_stored: int = 0
    items_updated: int = 0
    duplicates_removed: int = 0
    rejected: int = 0
    archived: int = 0
    retry_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_latency_ms: float = 0.0
    run_count: int = 0

    def record_run(
        self,
        *,
        items_fetched: int = 0,
        items_stored: int = 0,
        items_updated: int = 0,
        duplicates_removed: int = 0,
        rejected: int = 0,
        archived: int = 0,
        retry_count: int = 0,
        latency_ms: float = 0.0,
    ) -> None:
        self.items_fetched += items_fetched
        self.items_stored += items_stored
        self.items_updated += items_updated
        self.duplicates_removed += duplicates_removed
        self.rejected += rejected
        self.archived += archived
        self.retry_count += retry_count
        self.total_latency_ms += latency_ms
        self.run_count += 1

    @property
    def avg_latency_ms(self) -> float:
        if not self.run_count:
            return 0.0
        return round(self.total_latency_ms / self.run_count, 2)

    @property
    def error_rate(self) -> float:
        total = self.success_count + self.failure_count
        if not total:
            return 0.0
        return round(self.failure_count / total, 4)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "items_fetched": self.items_fetched,
            "items_stored": self.items_stored,
            "items_updated": self.items_updated,
            "duplicates_removed": self.duplicates_removed,
            "rejected": self.rejected,
            "archived": self.archived,
            "retry_count": self.retry_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "avg_latency_ms": self.avg_latency_ms,
            "error_rate": self.error_rate,
            "run_count": self.run_count,
        }


@dataclass
class _ConnectorEntry:
    """Internal holder combining the resolver, metadata, and telemetry."""

    resolver: Callable[[], BaseConnector]
    metadata: ConnectorMetadata
    telemetry: ConnectorTelemetry = field(default_factory=ConnectorTelemetry)
    instance: Optional[BaseConnector] = None


class ConnectorRegistry:
    """Singleton registry orchestrating connector lifecycle and discovery.

    Public methods are classmethods, preserving the original
    ``ConnectorRegistry.get(...)`` calling convention. They delegate to the
    shared singleton instance, so the module-level ``connector_registry``
    instance and any reference obtained via ``ConnectorRegistry()`` are
    interchangeable (dependency-injection friendly).
    """

    _instance: ClassVar[Optional["ConnectorRegistry"]] = None
    _init_lock: ClassVar[threading.Lock] = threading.Lock()

    def __new__(cls) -> "ConnectorRegistry":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._entries: Dict[str, _ConnectorEntry] = {}
                    instance._lock = threading.RLock()
                    instance._discovered = False
                    cls._instance = instance
        return cls._instance

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    @classmethod
    def register(
        cls,
        connector: ConnectorLike,
        *,
        source_slug: Optional[str] = None,
        display_name: Optional[str] = None,
        priority: Optional[int] = None,
        enabled: bool = True,
    ) -> str:
        """Register a connector instance, subclass, or factory callable.

        Every connector is validated before it is stored. Invalid connectors
        raise ``ValueError`` and are never registered.

        Args:
            connector: BaseConnector instance, subclass, or zero-arg factory.
            source_slug: Override the slug (required for bare factories).
            display_name: Optional override for the human-readable name.
            priority: Optional override; defaults to connector or registry value.
            enabled: Whether the connector is active immediately.

        Returns:
            The normalized (lower-cased) source slug used as the key.

        Raises:
            TypeError: If ``connector`` is not a supported registration type.
            ValueError: If the connector fails validation.
        """
        registry = cls._get_instance()
        resolver = registry._build_resolver(connector)
        slug = (source_slug or "").lower()
        entry = _ConnectorEntry(
            resolver=resolver,
            metadata=ConnectorMetadata(
                source_slug=slug,
                display_name=display_name or "",
                priority=priority if priority is not None else DEFAULT_PRIORITY,
                enabled=enabled,
            ),
        )
        errors = registry._hydrate(entry, explicit_slug=slug)
        slug = entry.metadata.source_slug
        if not slug:
            errors.append(
                "Connector must define a non-empty source_slug or receive one."
            )
        errors.extend(registry._validate_connector(entry))
        if errors:
            raise ValueError(
                "Connector validation failed: " + "; ".join(errors)
            )
        with registry._lock:
            previous = registry._entries.get(slug)
            if previous is not None:
                logger.warning(
                    "Overwriting existing connector registration for '%s'.", slug
                )
            registry._entries[slug] = entry
        logger.info(
            "Registered source connector: '%s' (%s)",
            entry.metadata.display_name,
            slug,
        )
        return slug

    @classmethod
    def register_factory(
        cls,
        source_slug: str,
        factory: Callable[[], BaseConnector],
        *,
        display_name: Optional[str] = None,
        priority: Optional[int] = None,
        enabled: bool = True,
    ) -> str:
        """Register a lazy factory that injects connector dependencies."""
        if not callable(factory):
            raise TypeError("factory must be a zero-arg callable.")
        return cls.register(
            factory,
            source_slug=source_slug,
            display_name=display_name,
            priority=priority,
            enabled=enabled,
        )

    @classmethod
    def validate(cls, connector: ConnectorLike) -> List[str]:
        """Validate a connector WITHOUT registering it.

        Returns:
            A list of human-readable validation errors (empty if valid).
        """
        registry = cls._get_instance()
        entry = _ConnectorEntry(
            resolver=registry._build_resolver(connector),
            metadata=ConnectorMetadata(
                source_slug="",
                display_name="",
            ),
        )
        errors = registry._hydrate(entry, explicit_slug=None)
        errors.extend(registry._validate_connector(entry))
        return errors

    @classmethod
    def unregister(cls, source_slug: str) -> bool:
        """Remove a connector registration. Returns True if it existed."""
        registry = cls._get_instance()
        with registry._lock:
            return (
                registry._entries.pop((source_slug or "").lower(), None) is not None
            )

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    @classmethod
    def get(cls, source_slug: str) -> Optional[BaseConnector]:
        """Return the usable connector for a slug, or None.

        Triggers package discovery on first miss so newly added connector
        modules are picked up without pipeline code changes. Removed or
        disabled connectors are not returned.
        """
        registry = cls._get_instance()
        slug = (source_slug or "").lower()
        if not slug:
            return None
        entry = registry._entries.get(slug)
        if entry is None:
            registry.discover()
            entry = registry._entries.get(slug)
        if entry is None or not entry.metadata.enabled:
            return None
        if entry.metadata.lifecycle is ConnectorLifecycle.REMOVED:
            return None
        with registry._lock:
            registry._hydrate(entry, explicit_slug=slug)
            return entry.instance

    @classmethod
    def get_all(cls, include_disabled: bool = False) -> List[BaseConnector]:
        """Return registered connectors ordered by priority (highest first)."""
        registry = cls._get_instance()
        registry.discover()
        connectors: List[BaseConnector] = []
        with registry._lock:
            ordered = registry._ordered_slugs(include_disabled=include_disabled)
            for slug in ordered:
                entry = registry._entries[slug]
                registry._hydrate(entry, explicit_slug=slug)
                if entry.instance is not None:
                    connectors.append(entry.instance)
        return connectors

    @classmethod
    def is_registered(cls, source_slug: str) -> bool:
        """Check whether a slug is registered (regardless of enabled state)."""
        registry = cls._get_instance()
        return (source_slug or "").lower() in registry._entries

    @classmethod
    def is_enabled(cls, source_slug: str) -> bool:
        """Check whether a slug is registered and currently enabled."""
        registry = cls._get_instance()
        entry = registry._entries.get((source_slug or "").lower())
        return bool(entry and entry.metadata.enabled)

    # ------------------------------------------------------------------
    # Enable / Disable / Deprecate / Remove
    # ------------------------------------------------------------------

    @classmethod
    def enable(cls, source_slug: str) -> bool:
        """Enable a connector, recovering it to HEALTHY. True if registered."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None or entry.metadata.lifecycle is ConnectorLifecycle.REMOVED:
                return False
            entry.metadata.enabled = True
            if entry.metadata.lifecycle is ConnectorLifecycle.DISABLED:
                registry._transition(entry, ConnectorLifecycle.HEALTHY)
            return True

    @classmethod
    def disable(cls, source_slug: str) -> bool:
        """Disable a connector, moving it to DISABLED. True if registered."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None or entry.metadata.lifecycle is ConnectorLifecycle.REMOVED:
                return False
            entry.metadata.enabled = False
            registry._transition(entry, ConnectorLifecycle.DISABLED)
            return True

    @classmethod
    def deprecate(cls, source_slug: str) -> bool:
        """Mark a connector DEPRECATED (still registered). True if registered."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None:
                return False
            registry._transition(entry, ConnectorLifecycle.DEPRECATED)
            return True

    @classmethod
    def mark_removed(cls, source_slug: str) -> bool:
        """Tombstone a connector as REMOVED without deleting the entry."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None:
                return False
            registry._transition(entry, ConnectorLifecycle.REMOVED)
            entry.metadata.enabled = False
            return True

    @classmethod
    def set_lifecycle(cls, source_slug: str, state: ConnectorLifecycle) -> bool:
        """Explicitly transition a connector to ``state``.

        Raises:
            ValueError: If the transition is not allowed by the lifecycle table.
        """
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None:
                return False
            registry._transition(entry, state)
            return True

    # ------------------------------------------------------------------
    # Lifecycle / Health
    # ------------------------------------------------------------------

    @classmethod
    def mark_success(cls, source_slug: str) -> None:
        """Record a successful fetch, recovering the connector to HEALTHY."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None:
                return
            metadata = entry.metadata
            if metadata.lifecycle is ConnectorLifecycle.REMOVED:
                return
            registry._transition(entry, ConnectorLifecycle.HEALTHY)
            metadata.consecutive_failures = 0
            metadata.last_success_at = datetime.now(timezone.utc).isoformat()
            metadata.last_error = None
            entry.telemetry.success_count += 1

    @classmethod
    def mark_failure(
        cls, source_slug: str, error: Optional[str] = None
    ) -> None:
        """Record a failed fetch, escalating lifecycle after repeated failures."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None:
                return
            metadata = entry.metadata
            if metadata.lifecycle is ConnectorLifecycle.REMOVED:
                return
            metadata.consecutive_failures += 1
            failures = metadata.consecutive_failures
            if failures >= OFFLINE_AFTER_FAILURES:
                target = ConnectorLifecycle.OFFLINE
            else:
                target = ConnectorLifecycle.DEGRADED
            registry._transition(entry, target)
            metadata.last_failure_at = datetime.now(timezone.utc).isoformat()
            metadata.last_error = error
            entry.telemetry.failure_count += 1

    @classmethod
    def get_health(cls, source_slug: str) -> Optional[ConnectorLifecycle]:
        """Return the lifecycle state for a slug (None if not registered)."""
        registry = cls._get_instance()
        entry = registry._entries.get((source_slug or "").lower())
        return entry.metadata.lifecycle if entry else None

    @classmethod
    def get_lifecycle(cls, source_slug: str) -> Optional[ConnectorLifecycle]:
        """Alias for :meth:`get_health`."""
        return cls.get_health(source_slug)

    @classmethod
    def health_summary(cls) -> List[Dict[str, Any]]:
        """Return serializable health/metadata rows for all registrations."""
        registry = cls._get_instance()
        with registry._lock:
            return [
                entry.metadata.to_dict()
                for entry in registry._entries.values()
            ]

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @classmethod
    def get_metadata(cls, source_slug: str) -> Optional[ConnectorMetadata]:
        """Return metadata for a slug, or None if not registered."""
        registry = cls._get_instance()
        entry = registry._entries.get((source_slug or "").lower())
        return entry.metadata if entry else None

    @classmethod
    def get_all_metadata(
        cls, include_disabled: bool = True
    ) -> List[ConnectorMetadata]:
        """Return metadata for all registrations ordered by priority."""
        registry = cls._get_instance()
        with registry._lock:
            return [
                registry._entries[slug].metadata
                for slug in registry._ordered_slugs(include_disabled=include_disabled)
            ]

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    @classmethod
    def record_run(
        cls,
        source_slug: str,
        *,
        items_fetched: int = 0,
        items_stored: int = 0,
        items_updated: int = 0,
        duplicates_removed: int = 0,
        rejected: int = 0,
        archived: int = 0,
        retry_count: int = 0,
        latency_ms: float = 0.0,
    ) -> None:
        """Accumulate a single ingestion run's telemetry for a connector."""
        registry = cls._get_instance()
        with registry._lock:
            entry = registry._entries.get((source_slug or "").lower())
            if entry is None:
                return
            entry.telemetry.record_run(
                items_fetched=items_fetched,
                items_stored=items_stored,
                items_updated=items_updated,
                duplicates_removed=duplicates_removed,
                rejected=rejected,
                archived=archived,
                retry_count=retry_count,
                latency_ms=latency_ms,
            )

    @classmethod
    def get_telemetry(cls, source_slug: str) -> Optional[Dict[str, Any]]:
        """Return telemetry for a slug as a serializable dict, or None."""
        registry = cls._get_instance()
        entry = registry._entries.get((source_slug or "").lower())
        if entry is None:
            return None
        return entry.telemetry.to_dict()

    @classmethod
    def all_telemetry(cls) -> List[Dict[str, Any]]:
        """Return telemetry dicts for all registrations."""
        registry = cls._get_instance()
        with registry._lock:
            return [
                entry.telemetry.to_dict()
                for entry in registry._entries.values()
            ]

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    @classmethod
    def discover(cls, package: str = DEFAULT_CONNECTOR_PACKAGE) -> List[str]:
        """Scan a package for concrete BaseConnector subclasses and register them.

        Runs at most once per registry lifetime (or until ``clear()``).
        Connectors requiring constructor injection are skipped with a warning
        and must be registered explicitly via ``register``/``register_factory``.

        Returns:
            List of newly registered source slugs.
        """
        registry = cls._get_instance()
        if registry._discovered:
            return list(registry._entries.keys())

        found: List[str] = []
        try:
            pkg = importlib.import_module(package)
        except ImportError as exc:
            logger.warning(
                "Connector discovery failed to import package '%s': %s",
                package,
                exc,
            )
            registry._discovered = True
            return found

        package_path = getattr(pkg, "__path__", None)
        if package_path is None:
            registry._discovered = True
            return found

        for module_info in pkgutil.iter_modules(package_path):
            module_name = f"{package}.{module_info.name}"
            try:
                module = importlib.import_module(module_name)
            except Exception as exc:
                logger.warning(
                    "Connector discovery skipped module '%s': %s",
                    module_name,
                    exc,
                )
                continue

            for _, candidate in inspect.getmembers(module, inspect.isclass):
                if candidate is BaseConnector or not issubclass(
                    candidate, BaseConnector
                ):
                    continue
                if inspect.isabstract(candidate):
                    continue
                slug = registry._register_candidate(candidate)
                if slug and slug not in found:
                    found.append(slug)

        with registry._lock:
            registry._discovered = True
        return found

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @classmethod
    def clear(cls) -> None:
        """Remove all registrations and reset the discovery flag."""
        registry = cls._get_instance()
        with registry._lock:
            registry._entries.clear()
            registry._discovered = False

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @classmethod
    def _get_instance(cls) -> "ConnectorRegistry":
        return cls()

    def _build_resolver(self, connector: ConnectorLike) -> Callable[[], BaseConnector]:
        if isinstance(connector, BaseConnector):
            return lambda conn=connector: conn
        if inspect.isclass(connector) and issubclass(connector, BaseConnector):
            return connector
        if callable(connector):
            return connector
        raise TypeError(
            f"Unsupported connector type '{type(connector).__name__}'; expected a "
            "BaseConnector instance, subclass, or zero-arg factory callable."
        )

    def _hydrate(
        self, entry: _ConnectorEntry, explicit_slug: Optional[str] = None
    ) -> List[str]:
        """Resolve the connector instance and sync metadata from it.

        Returns a list of hydration errors (non-empty if unresolved).
        """
        if entry.instance is not None:
            return []
        errors: List[str] = []
        try:
            instance = entry.resolver()
        except Exception as exc:
            errors.append(f"Could not instantiate connector: {exc}")
            return errors
        if not isinstance(instance, BaseConnector):
            errors.append(
                "Resolver did not return a BaseConnector instance."
            )
            return errors
        entry.instance = instance
        metadata = entry.metadata
        slug = (explicit_slug or instance.source_slug or "").lower()
        if slug:
            metadata.source_slug = slug
        metadata.display_name = metadata.display_name or instance.display_name
        reputation = getattr(instance, "default_source_reputation", None)
        if reputation is not None:
            metadata.source_reputation = int(reputation)
        declared_priority = getattr(instance, "priority", None)
        if (
            declared_priority is not None
            and metadata.priority == DEFAULT_PRIORITY
        ):
            metadata.priority = int(declared_priority)
        caps = getattr(instance, "capabilities", None)
        if caps:
            metadata.capabilities = capability_names(caps)
        metadata.version = getattr(instance, "version", metadata.version)
        metadata.owner = getattr(instance, "owner", metadata.owner)
        metadata.supports_incremental_sync = bool(
            getattr(instance, "supports_incremental_sync", False)
        )
        metadata.supports_full_sync = bool(
            getattr(instance, "supports_full_sync", True)
        )
        rate = getattr(instance, "estimated_rate_limit", None)
        if rate is not None:
            try:
                metadata.estimated_rate_limit = float(rate)
            except (TypeError, ValueError):
                pass
        return errors

    def _validate_connector(self, entry: _ConnectorEntry) -> List[str]:
        """Validate a hydrated entry. Returns a list of error strings."""
        errors: List[str] = []
        instance = entry.instance
        if instance is None:
            return errors
        slug = entry.metadata.source_slug
        if not slug or not str(slug).strip():
            errors.append("source_slug must be a non-empty string.")
        if not entry.metadata.display_name or not str(
            entry.metadata.display_name
        ).strip():
            errors.append("display_name must be a non-empty string.")
        priority = entry.metadata.priority
        if not isinstance(priority, int) or isinstance(priority, bool):
            errors.append(f"priority must be an integer, got {priority!r}.")
        reputation = entry.metadata.source_reputation
        if reputation is None or not (0 <= reputation <= 100):
            errors.append(
                f"default_source_reputation must be within 0..100, got {reputation!r}."
            )
        fetch = getattr(instance, "fetch_raw_opportunities", None)
        if not callable(fetch) or getattr(fetch, "__isabstractmethod__", False):
            errors.append("fetch_raw_opportunities must be implemented.")
        elif self._is_stub_method(fetch):
            errors.append(
                "fetch_raw_opportunities raises NotImplementedError; it is not "
                "implemented."
            )
        capabilities = entry.metadata.capabilities
        if not capabilities:
            errors.append("capabilities must be a non-empty set of Capability members.")
        else:
            for name in capabilities:
                if name not in Capability._value2member_map_:
                    errors.append(f"Unknown capability '{name}'.")
        return errors

    @staticmethod
    def _is_stub_method(method: Callable[..., Any]) -> bool:
        """Return True if a method body is just ``raise NotImplementedError``."""
        try:
            code = inspect.getsource(method)
        except (OSError, TypeError):
            return False
        stripped = "".join(line.strip() for line in code.splitlines())
        return "raiseNotImplementedError" in stripped.replace(" ", "")

    def _register_candidate(self, candidate: Type[BaseConnector]) -> Optional[str]:
        try:
            slug = self.register(candidate)
        except Exception as exc:
            logger.warning(
                "Connector discovery skipped '%s': %s", candidate.__name__, exc
            )
            return None
        with self._lock:
            entry = self._entries.get(slug)
            if entry is not None and entry.instance is None:
                del self._entries[slug]
                logger.warning(
                    "Connector '%s' requires dependency injection; register it "
                    "explicitly with a factory.",
                    slug,
                )
                return None
            if entry is not None and slug in DISABLED_SOURCE_SLUGS:
                entry.metadata.enabled = False
                try:
                    self._transition(entry, ConnectorLifecycle.DISABLED)
                except ValueError:
                    pass
                logger.warning(
                    "Connector '%s' is in DISABLED_SOURCE_SLUGS: kept in "
                    "code but excluded from the enabled set.",
                    slug,
                )
        return slug

    def _transition(
        self,
        entry: _ConnectorEntry,
        target: ConnectorLifecycle,
    ) -> None:
        """Apply an explicit lifecycle transition, raising on illegal moves."""
        current = entry.metadata.lifecycle
        if target is current:
            return
        allowed = _LIFECYCLE_TRANSITIONS.get(current, set())
        if target not in allowed:
            raise ValueError(
                f"Illegal lifecycle transition for '{entry.metadata.source_slug}': "
                f"{current.value} -> {target.value}"
            )
        entry.metadata.lifecycle = target

    def _ordered_slugs(self, include_disabled: bool = False) -> List[str]:
        slugs = [
            slug
            for slug, entry in self._entries.items()
            if (
                (include_disabled or entry.metadata.enabled)
                and entry.metadata.lifecycle is not ConnectorLifecycle.REMOVED
            )
        ]
        slugs.sort(
            key=lambda slug: self._entries[slug].metadata.priority, reverse=True
        )
        return slugs


# Global registry instance (backward-compatible alias used by the pipeline).
connector_registry = ConnectorRegistry()

__all__ = [
    "ConnectorRegistry",
    "ConnectorMetadata",
    "ConnectorTelemetry",
    "ConnectorLifecycle",
    "ConnectorHealth",
    "Capability",
    "ConnectorLike",
    "connector_registry",
    "DEFAULT_CONNECTOR_PACKAGE",
    "DEFAULT_PRIORITY",
    "PRIORITY_CRITICAL",
    "PRIORITY_HIGH",
    "PRIORITY_NORMAL",
    "PRIORITY_LOW",
    "DEGRADED_AFTER_FAILURES",
    "OFFLINE_AFTER_FAILURES",
]
