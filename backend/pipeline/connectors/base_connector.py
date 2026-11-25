"""
BaseConnector Interface
======================
Abstract Base Class for all source connectors.
Rule: Connectors ONLY fetch raw payloads (OpportunityRaw) from external APIs/web pages.
Connectors MUST NOT contain any domain or branch-specific business logic.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Set
from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.capabilities import Capability


class ConnectorFetchError(RuntimeError):
    """Raised when a connector cannot retrieve ANY of its configured upstreams.

    Connectors must degrade gracefully on partial failure (one dead board out
    of six, one blocked feed out of two): return the rows that were fetched and
    log the rest. But returning an empty list after a TOTAL upstream failure
    makes the pipeline mark the source healthy with 0 items, hiding outages.

    Raise this only when every configured upstream failed. A successful
    request that legitimately contains zero items is NOT a failure.
    """


class BaseConnector(ABC):
    """Abstract interface for all source scrapers & API connectors."""

    # ── Default metadata / capability declarations (overridable per connector) ──
    priority: int = 500
    version: str = "1.0.0"
    owner: str = "platform"
    capabilities: Set[Capability] = {Capability.JOB}
    supports_incremental_sync: bool = False
    supports_full_sync: bool = True
    estimated_rate_limit: float = 2.0

    @property
    @abstractmethod
    def source_slug(self) -> str:
        """Unique identifier slug matching company_sources.source_slug (e.g. 'internshala')."""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name of the source."""
        pass

    @property
    @abstractmethod
    def default_source_reputation(self) -> int:
        """Base reputation score for items from this source (0-100)."""
        pass

    @abstractmethod
    def fetch_raw_opportunities(
        self, 
        config: Dict[str, Any], 
        query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch raw items from source API or HTML feed.
        
        Args:
            config: Source configuration loaded dynamically from database.
            query: Optional search keyword or category filter.
            
        Returns:
            List of OpportunityRaw instances.
        """
        pass
