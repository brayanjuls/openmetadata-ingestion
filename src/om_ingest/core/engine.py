"""Main ingestion engine - orchestrates the entire ingestion process."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from om_ingest.config.loader import ConfigLoader
from om_ingest.config.schema import EntityConfig, IngestionConfig
from om_ingest.core.client import OpenMetadataClient
from om_ingest.core.context import ExecutionContext
from om_ingest.core.dependency_resolver import DependencyResolver
from om_ingest.core.executor import EntityExecutor, ExecutionResult

logger = logging.getLogger(__name__)


@dataclass
class IngestionSummary:
    """Summary of ingestion execution."""

    total_entities: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    duration_seconds: float = 0.0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    results: List[ExecutionResult] = field(default_factory=list)

    def add_result(self, result: ExecutionResult) -> None:
        """
        Add execution result to summary.

        Args:
            result: Execution result
        """
        self.results.append(result)
        self.total_entities += 1

        if result.success:
            if result.skipped:
                self.skipped += 1
            else:
                self.successful += 1
        else:
            self.failed += 1

    def finalize(self) -> None:
        """Finalize the summary with end time and duration."""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()

    def __str__(self) -> str:
        """String representation."""
        lines = [
            "=" * 60,
            "Ingestion Summary",
            "=" * 60,
            f"Total Entities:     {self.total_entities}",
            f"Successful:         {self.successful}",
            f"Skipped:            {self.skipped}",
            f"Failed:             {self.failed}",
            f"Duration:           {self.duration_seconds:.2f}s",
            "=" * 60,
        ]

        if self.failed > 0:
            lines.append("\nFailed Entities:")
            for result in self.results:
                if not result.success:
                    lines.append(f"  - {result}")

        return "\n".join(lines)


class IngestionEngine:
    """
    Main ingestion engine.

    Orchestrates the entire ingestion process:
    1. Load and validate configuration
    2. Initialize OpenMetadata client
    3. Resolve entity dependencies
    4. Execute entities in topological order
    5. Generate summary and audit logs
    """

    def __init__(self, config_path: str):
        """
        Initialize ingestion engine.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: Optional[IngestionConfig] = None
        self.client: Optional[OpenMetadataClient] = None
        self.context: Optional[ExecutionContext] = None

    def run(self) -> IngestionSummary:
        """
        Run the complete ingestion process.

        Returns:
            IngestionSummary with execution results

        Raises:
            Various exceptions for fatal errors
        """
        summary = IngestionSummary()

        try:
            # Step 1: Load configuration
            logger.info(f"Loading configuration from {self.config_path}")
            self.config = self._load_config()
            logger.info(f"Loaded ingestion config: {self.config.metadata.name}")

            # Step 2: Initialize OpenMetadata client
            logger.info("Initializing OpenMetadata client")
            self.client = self._initialize_client()
            logger.info(f"Connected to OpenMetadata: {self.config.openmetadata.host}")

            # Step 3: Initialize execution context
            self.context = ExecutionContext(
                config=self.config,
                client=self.client,
            )

            if self.config.execution.dry_run:
                logger.warning("DRY RUN MODE - No changes will be made to OpenMetadata")

            # Step 4: Expand discovery configs into actual entities
            logger.info("Expanding discovery configurations")
            expanded_entities = self._expand_discovery()
            logger.info(f"Expanded to {len(expanded_entities)} entities")

            # Step 5: Resolve dependencies
            logger.info("Resolving entity dependencies")
            ordered_entities = self._resolve_dependencies(expanded_entities)
            logger.info(f"Resolved {len(ordered_entities)} entities in dependency order")

            # Step 5: Execute entities
            logger.info("Starting entity execution")
            executor = EntityExecutor(self.context)

            for entity_config in ordered_entities:
                result = executor.execute(entity_config)
                summary.add_result(result)

                # Log result
                if result.success:
                    if result.skipped:
                        logger.info(f"⊘ {result}")
                    else:
                        logger.info(f"✓ {result}")
                else:
                    logger.error(f"✗ {result}")

                    # Check if we should fail fast
                    if result.error and self.context.config.execution.fail_fast_on_dependency:
                        from om_ingest.strategies.error_handling import DependencyValidationError
                        if isinstance(result.error, DependencyValidationError):
                            logger.error("Stopping execution due to dependency error")
                            break

            # Step 6: Finalize summary
            summary.finalize()
            logger.info("\n" + str(summary))

            return summary

        except Exception as e:
            logger.error(f"Fatal error during ingestion: {e}", exc_info=True)
            summary.finalize()
            raise

    def _load_config(self) -> IngestionConfig:
        """
        Load and validate configuration.

        Returns:
            IngestionConfig

        Raises:
            Various configuration errors
        """
        loader = ConfigLoader()
        return loader.load(self.config_path)

    def _initialize_client(self) -> OpenMetadataClient:
        """
        Initialize OpenMetadata client.

        Returns:
            OpenMetadataClient instance

        Raises:
            Connection errors
        """
        return OpenMetadataClient(self.config.openmetadata)

    def _expand_discovery(self) -> List[EntityConfig]:
        """
        Expand discovery configurations into actual entity configurations.

        For entities with `discovery` field, connect to the data source
        and discover actual entities.

        Returns:
            List of EntityConfig (mix of static and discovered)
        """
        from om_ingest.sources import SourceRegistry

        expanded = []

        # Build source lookup map
        source_map = {}
        if self.config.sources:
            for source_config in self.config.sources:
                source_map[source_config.name] = source_config

        for entity_config in self.config.entities:
            if entity_config.discovery:
                # This entity uses discovery
                discovery = entity_config.discovery
                source_name = discovery.source

                if source_name not in source_map:
                    logger.error(f"Unknown source '{source_name}' in discovery config")
                    continue

                source_config = source_map[source_name]

                logger.info(f"Discovering {entity_config.type.value} entities from source '{source_name}'")

                try:
                    # Create and connect to source
                    source = SourceRegistry.create_source(source_config)
                    source.connect()

                    try:
                        # Discover entities
                        discovered = source.discover_entities(
                            entity_type=entity_config.type,
                            filters=discovery.filter,
                            include_pattern=discovery.include_pattern,
                            exclude_pattern=discovery.exclude_pattern,
                        )

                        discovered_list = list(discovered)
                        logger.info(f"Discovered {len(discovered_list)} {entity_config.type.value} entities")

                        expanded.extend(discovered_list)

                    finally:
                        source.disconnect()

                except Exception as e:
                    logger.error(f"Failed to discover from source '{source_name}': {e}")
                    # Continue with other entities
                    continue

            else:
                # Static entity configuration
                expanded.append(entity_config)

        return expanded

    def _resolve_dependencies(self, entities: List[EntityConfig]) -> List[EntityConfig]:
        """
        Resolve entity dependencies and determine execution order.

        Args:
            entities: List of entity configurations

        Returns:
            List of EntityConfig in topological order

        Raises:
            Circular dependency errors
        """
        resolver = DependencyResolver(entities)
        return resolver.resolve()


def run_ingestion(config_path: str) -> IngestionSummary:
    """
    Convenience function to run ingestion.

    Args:
        config_path: Path to YAML configuration

    Returns:
        IngestionSummary
    """
    engine = IngestionEngine(config_path)
    return engine.run()
