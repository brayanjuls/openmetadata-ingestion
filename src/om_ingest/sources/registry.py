"""Source connector registry for plugin management."""

import logging
from typing import Dict, Type

from om_ingest.config.schema import SourceConfig, SourceType
from om_ingest.sources.base import DataSource

logger = logging.getLogger(__name__)


class SourceRegistry:
    """
    Registry for data source connectors.

    Uses decorator pattern for plugin registration:

    @SourceRegistry.register(SourceType.S3_HUDI)
    class S3HudiConnector(DataSource):
        ...
    """

    _sources: Dict[SourceType, Type[DataSource]] = {}

    @classmethod
    def register(cls, source_type: SourceType):
        """
        Decorator to register a data source connector.

        Args:
            source_type: Source type enum value

        Returns:
            Decorator function

        Example:
            @SourceRegistry.register(SourceType.S3_HUDI)
            class S3HudiConnector(DataSource):
                pass
        """

        def decorator(source_class: Type[DataSource]):
            if source_type in cls._sources:
                logger.warning(
                    f"Source connector for {source_type.value} is being overridden"
                )
            cls._sources[source_type] = source_class
            logger.debug(
                f"Registered source connector: {source_type.value} -> {source_class.__name__}"
            )
            return source_class

        return decorator

    @classmethod
    def get_source_class(cls, source_type: SourceType) -> Type[DataSource]:
        """
        Get source connector class by type.

        Args:
            source_type: Source type enum

        Returns:
            DataSource class

        Raises:
            ValueError: If source type not registered
        """
        if source_type not in cls._sources:
            available = ", ".join([st.value for st in cls._sources.keys()])
            raise ValueError(
                f"No source connector registered for type: {source_type.value}. "
                f"Available sources: {available or 'none'}"
            )
        return cls._sources[source_type]

    @classmethod
    def create_source(cls, config: SourceConfig) -> DataSource:
        """
        Create a data source instance from configuration.

        Args:
            config: Source configuration

        Returns:
            DataSource instance

        Raises:
            ValueError: If source type not registered
        """
        source_class = cls.get_source_class(config.type)
        return source_class(config)

    @classmethod
    def list_sources(cls) -> Dict[SourceType, Type[DataSource]]:
        """
        Get all registered source connectors.

        Returns:
            Dictionary mapping source types to connector classes
        """
        return cls._sources.copy()

    @classmethod
    def is_registered(cls, source_type: SourceType) -> bool:
        """
        Check if a source type is registered.

        Args:
            source_type: Source type to check

        Returns:
            True if registered, False otherwise
        """
        return source_type in cls._sources
