"""Abstract base class for data source connectors."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional

from om_ingest.config.schema import EntityConfig, EntityType, SourceConfig


class DataSourceError(Exception):
    """Raised when data source operations fail."""

    pass


class DataSource(ABC):
    """
    Abstract base class for data source connectors.

    Data sources are responsible for:
    - Connecting to external systems (databases, data lakes, APIs, etc.)
    - Discovering entities (tables, topics, pipelines, etc.)
    - Extracting schema metadata
    - Fetching sample data for profiling
    """

    def __init__(self, config: SourceConfig):
        """
        Initialize data source.

        Args:
            config: Source configuration from YAML
        """
        self.config = config
        self.name = config.name
        self.properties = config.properties
        self._connected = False

    @property
    @abstractmethod
    def source_type(self) -> str:
        """
        Get the source type identifier.

        Returns:
            Source type string (e.g., "s3_hudi", "postgres")
        """
        pass

    @property
    @abstractmethod
    def supported_entity_types(self) -> List[EntityType]:
        """
        Get list of entity types this source can discover.

        Returns:
            List of supported EntityType values
        """
        pass

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the data source.

        Raises:
            DataSourceError: If connection fails
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close connection to the data source.

        Raises:
            DataSourceError: If disconnect fails
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Test that connection is working.

        Returns:
            True if connection is valid, False otherwise
        """
        pass

    @abstractmethod
    def discover_entities(
        self,
        entity_type: EntityType,
        filters: Optional[Dict[str, Any]] = None,
        include_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
    ) -> Iterator[EntityConfig]:
        """
        Discover entities from the data source.

        Args:
            entity_type: Type of entities to discover
            filters: Source-specific filters (e.g., schema name, database name)
            include_pattern: Regex pattern for entities to include
            exclude_pattern: Regex pattern for entities to exclude

        Yields:
            EntityConfig objects for discovered entities

        Raises:
            DataSourceError: If discovery fails
        """
        pass

    @abstractmethod
    def extract_schema(
        self, entity_type: EntityType, entity_identifier: str
    ) -> Dict[str, Any]:
        """
        Extract schema metadata for a specific entity.

        Args:
            entity_type: Type of entity
            entity_identifier: Entity identifier (name, path, etc.)

        Returns:
            Dictionary containing schema metadata (columns, types, etc.)

        Raises:
            DataSourceError: If schema extraction fails
        """
        pass

    @abstractmethod
    def fetch_sample_data(
        self,
        entity_type: EntityType,
        entity_identifier: str,
        sample_size: Optional[int] = None,
    ) -> Any:
        """
        Fetch sample data from an entity for profiling.

        Args:
            entity_type: Type of entity
            entity_identifier: Entity identifier
            sample_size: Number of rows to sample (None = source default)

        Returns:
            Sample data (pandas DataFrame, dict, or source-specific format)

        Raises:
            DataSourceError: If data fetch fails
        """
        pass

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
