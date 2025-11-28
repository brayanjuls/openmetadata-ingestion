"""Execution context for managing ingestion state."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel

from om_ingest.config.schema import EntityType, IngestionConfig
from om_ingest.core.client import OMClient


@dataclass
class ExecutionStats:
    """Statistics for an ingestion execution."""

    total_entities: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    updated: int = 0
    created: int = 0
    dry_run: int = 0
    validation_errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate execution duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            "total_entities": self.total_entities,
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "updated": self.updated,
            "created": self.created,
            "dry_run": self.dry_run,
            "validation_errors": self.validation_errors,
            "duration_seconds": self.duration_seconds,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
        }


@dataclass
class ProcessedEntity:
    """Information about a processed entity."""

    entity_type: EntityType
    name: str
    fqn: str
    om_entity: Optional[BaseModel] = None  # The actual OpenMetadata entity
    success: bool = True
    error: Optional[str] = None
    created: bool = False
    updated: bool = False
    skipped: bool = False


class ExecutionContext:
    """
    Manages state during ingestion execution.

    Tracks:
    - OpenMetadata client
    - Processed entities cache
    - Configuration
    - Execution statistics
    """

    def __init__(
        self,
        config: IngestionConfig,
        client: OMClient,
    ):
        """
        Initialize execution context.

        Args:
            config: Ingestion configuration
            client: OpenMetadata client
        """
        self.config = config
        self.client = client
        self.stats = ExecutionStats()

        # Cache of processed entities: FQN -> ProcessedEntity
        self._processed: Dict[str, ProcessedEntity] = {}

        # Start timer
        self.stats.start_time = datetime.now()

    def register_entity(
        self,
        entity_type: EntityType,
        name: str,
        fqn: str,
        om_entity: Optional[BaseModel] = None,
        created: bool = False,
        updated: bool = False,
        skipped: bool = False,
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        """
        Register a processed entity in the context.

        Args:
            entity_type: Type of entity
            name: Entity name
            fqn: Fully qualified name
            om_entity: OpenMetadata entity object
            created: Whether entity was created
            updated: Whether entity was updated
            skipped: Whether entity was skipped
            success: Whether operation succeeded
            error: Error message if failed
        """
        processed = ProcessedEntity(
            entity_type=entity_type,
            name=name,
            fqn=fqn,
            om_entity=om_entity,
            success=success,
            error=error,
            created=created,
            updated=updated,
            skipped=skipped,
        )

        self._processed[fqn] = processed

        # Update stats
        self.stats.total_entities += 1
        if success:
            self.stats.successful += 1
            if created:
                self.stats.created += 1
            elif updated:
                self.stats.updated += 1
            elif skipped:
                self.stats.skipped += 1
        else:
            self.stats.failed += 1

    def register_dry_run(
        self,
        entity_type: EntityType,
        name: str,
        fqn: str,
    ) -> None:
        """
        Register a dry-run entity.

        Args:
            entity_type: Type of entity
            name: Entity name
            fqn: Fully qualified name
        """
        processed = ProcessedEntity(
            entity_type=entity_type,
            name=name,
            fqn=fqn,
            success=True,
        )

        self._processed[fqn] = processed
        self.stats.total_entities += 1
        self.stats.dry_run += 1

    def register_validation_error(
        self,
        entity_type: EntityType,
        name: str,
        error: str,
    ) -> None:
        """
        Register a validation error.

        Args:
            entity_type: Type of entity
            name: Entity name
            error: Error message
        """
        # Use name as FQN for validation errors
        fqn = f"{entity_type.value}:{name}"

        processed = ProcessedEntity(
            entity_type=entity_type,
            name=name,
            fqn=fqn,
            success=False,
            error=error,
        )

        self._processed[fqn] = processed
        self.stats.total_entities += 1
        self.stats.validation_errors += 1
        self.stats.failed += 1

    def get_processed_entity(self, fqn: str) -> Optional[ProcessedEntity]:
        """
        Get a processed entity by FQN.

        Args:
            fqn: Fully qualified name

        Returns:
            ProcessedEntity if found, None otherwise
        """
        return self._processed.get(fqn)

    def get_entity(self, fqn: str) -> Optional[BaseModel]:
        """
        Get the OpenMetadata entity object by FQN.

        Args:
            fqn: Fully qualified name

        Returns:
            OpenMetadata entity if found, None otherwise
        """
        processed = self._processed.get(fqn)
        return processed.om_entity if processed else None

    def entity_processed(self, fqn: str) -> bool:
        """
        Check if an entity has been processed.

        Args:
            fqn: Fully qualified name

        Returns:
            True if entity was processed
        """
        return fqn in self._processed

    def entity_exists_successfully(self, fqn: str) -> bool:
        """
        Check if an entity was successfully processed.

        Args:
            fqn: Fully qualified name

        Returns:
            True if entity was processed successfully
        """
        entity = self._processed.get(fqn)
        return entity is not None and entity.success

    def entity_exists(self, fqn: str) -> bool:
        """
        Check if an entity exists either in local context or in OpenMetadata.

        Args:
            fqn: Fully qualified name

        Returns:
            True if entity exists in context or OpenMetadata
        """
        import logging
        logger = logging.getLogger(__name__)

        # First check local context (entities created/updated in this execution)
        if self.entity_processed(fqn):
            logger.debug(f"Entity {fqn} found in local context")
            return True

        # Then check OpenMetadata for pre-existing entities
        # Determine entity type from FQN structure
        # FQN formats:
        # - service: "service_name"
        # - database: "service.database"
        # - schema: "service.database.schema"
        # - table: "service.database.schema.table"
        parts = fqn.split(".")
        entity_type = None

        if len(parts) == 1:
            entity_type = EntityType.DATABASE_SERVICE
        elif len(parts) == 2:
            entity_type = EntityType.DATABASE
        elif len(parts) == 3:
            entity_type = EntityType.DATABASE_SCHEMA
        elif len(parts) == 4:
            entity_type = EntityType.TABLE

        logger.debug(f"Checking if entity {fqn} (type: {entity_type}) exists in OpenMetadata")

        if entity_type:
            try:
                existing = self.client.get_entity(entity_type, fqn)
                result = existing is not None
                logger.debug(f"Entity {fqn} exists in OpenMetadata: {result}")
                return result
            except Exception as e:
                # Entity doesn't exist in OpenMetadata
                logger.debug(f"Entity {fqn} not found in OpenMetadata: {e}")
                return False

        logger.debug(f"Could not determine entity type for {fqn}")
        return False

    def get_all_processed(self) -> list[ProcessedEntity]:
        """
        Get all processed entities.

        Returns:
            List of all processed entities
        """
        return list(self._processed.values())

    def get_failed_entities(self) -> list[ProcessedEntity]:
        """
        Get all failed entities.

        Returns:
            List of failed entities
        """
        return [e for e in self._processed.values() if not e.success]

    def finalize(self) -> ExecutionStats:
        """
        Finalize the execution and return stats.

        Returns:
            Execution statistics
        """
        self.stats.end_time = datetime.now()
        return self.stats

    @property
    def dry_run(self) -> bool:
        """Check if in dry-run mode."""
        return self.config.execution.dry_run if self.config.execution else False

    @property
    def continue_on_error(self) -> bool:
        """Check if should continue on entity errors."""
        return (
            self.config.execution.continue_on_error
            if self.config.execution
            else True
        )

    @property
    def fail_fast_on_dependency(self) -> bool:
        """Check if should fail fast on dependency errors."""
        return (
            self.config.execution.fail_fast_on_dependency
            if self.config.execution
            else True
        )
