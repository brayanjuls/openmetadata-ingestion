"""Entity executor - handles execution of individual entities."""

import logging
from dataclasses import dataclass
from typing import Any, Optional

from om_ingest.config.schema import EntityConfig, IdempotencyMode, Operation
from om_ingest.core.context import ExecutionContext
from om_ingest.core.schema_comparator import SchemaComparison, SchemaComparator
from om_ingest.entities.base import EntityHandler
from om_ingest.entities.registry import EntityRegistry
from om_ingest.strategies.error_handling import (
    DependencyValidationError,
    EntityProcessingError,
)
from om_ingest.strategies.idempotency import (
    IdempotencyAction,
    IdempotencyDecision,
    IdempotencyStrategyFactory,
)

# Import to trigger handler registration via decorators
import om_ingest.entities  # noqa: F401

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Result of entity execution."""

    entity_config: EntityConfig
    operation: Operation
    success: bool
    error: Optional[Exception] = None
    schema_changes: Optional[SchemaComparison] = None
    entity_fqn: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None

    def __str__(self) -> str:
        """String representation."""
        if self.success:
            if self.skipped:
                return f"SKIPPED: {self.entity_fqn} - {self.skip_reason}"
            else:
                changes = f" ({self.schema_changes.summary()})" if self.schema_changes else ""
                return f"{self.operation.value.upper()}: {self.entity_fqn}{changes}"
        else:
            return f"FAILED: {self.entity_fqn} - {str(self.error)}"


class EntityExecutor:
    """
    Executes individual entities.

    Handles the complete lifecycle:
    1. Validate dependencies
    2. Build entity
    3. Check if exists
    4. Apply idempotency strategy
    5. Detect schema changes
    6. Execute operation
    7. Run profiling (if enabled)
    8. Log audit event
    """

    def __init__(self, context: ExecutionContext):
        """
        Initialize entity executor.

        Args:
            context: Execution context
        """
        self.context = context

    def execute(self, entity_config: EntityConfig) -> ExecutionResult:
        """
        Execute a single entity.

        Args:
            entity_config: Entity configuration

        Returns:
            ExecutionResult with outcome
        """
        handler = None
        entity_fqn = None

        try:
            # Step 1: Get handler and validate
            handler = self._get_handler(entity_config)
            entity_fqn = handler.get_fqn()

            logger.info(f"Processing {entity_config.type.value}: {entity_fqn}")

            # Step 2: Validate dependencies
            self._validate_dependencies(handler)

            # Step 3: Build entity
            new_entity = handler.build_entity()

            # Step 4: Check if entity exists
            existing_entity = self._check_entity_exists(entity_fqn, entity_config.type)

            # Step 5: Detect schema changes (if applicable)
            schema_changes = self._detect_schema_changes(
                handler, existing_entity, new_entity
            )

            # Step 6: Apply idempotency strategy
            decision = self._apply_idempotency(
                entity_config, existing_entity, new_entity, schema_changes
            )

            # Step 7: Execute operation based on decision
            if decision.should_skip():
                return ExecutionResult(
                    entity_config=entity_config,
                    operation=Operation.SKIP,
                    success=True,
                    entity_fqn=entity_fqn,
                    schema_changes=schema_changes,
                    skipped=True,
                    skip_reason=decision.reason,
                )

            if decision.should_fail():
                raise EntityProcessingError(
                    message=decision.reason,
                    entity_type=entity_config.type,
                    entity_name=entity_config.name,
                )

            # Execute CREATE or UPDATE
            operation = decision.action.value
            if not self.context.dry_run:
                if decision.action == IdempotencyAction.CREATE:
                    created_entity = self.context.client.create_entity(
                        entity_config.type, new_entity
                    )
                    self.context.register_entity(
                        entity_type=entity_config.type,
                        name=entity_config.name or entity_fqn,
                        fqn=entity_fqn,
                        om_entity=created_entity,
                        created=True,
                        success=True,
                    )
                elif decision.action == IdempotencyAction.UPDATE:
                    updated_entity = self.context.client.update_entity(
                        entity_config.type, entity_fqn, new_entity
                    )
                    self.context.register_entity(
                        entity_type=entity_config.type,
                        name=entity_config.name or entity_fqn,
                        fqn=entity_fqn,
                        om_entity=updated_entity,
                        updated=True,
                        success=True,
                    )
            else:
                # Dry run - just register the FQN
                self.context.register_dry_run(
                    entity_type=entity_config.type,
                    name=entity_config.name or entity_fqn,
                    fqn=entity_fqn,
                )

            # Step 8: Run profiling (if enabled)
            # TODO: Implement profiling integration

            logger.info(
                f"Successfully {operation}d {entity_config.type.value}: {entity_fqn}"
            )

            return ExecutionResult(
                entity_config=entity_config,
                operation=Operation(operation),
                success=True,
                entity_fqn=entity_fqn,
                schema_changes=schema_changes,
            )

        except DependencyValidationError as e:
            # Dependency errors always fail fast
            logger.error(f"Dependency validation failed: {e}")
            return ExecutionResult(
                entity_config=entity_config,
                operation=Operation.SKIP,
                success=False,
                error=e,
                entity_fqn=entity_fqn,
            )

        except Exception as e:
            # Wrap in EntityProcessingError if not already
            if not isinstance(e, EntityProcessingError):
                e = EntityProcessingError(
                    message=str(e),
                    entity_type=entity_config.type,
                    entity_name=entity_config.name,
                    original_exception=e,
                )

            logger.error(f"Failed to process entity: {e}")

            return ExecutionResult(
                entity_config=entity_config,
                operation=Operation.SKIP,
                success=False,
                error=e,
                entity_fqn=entity_fqn,
            )

    def _get_handler(self, entity_config: EntityConfig) -> EntityHandler:
        """
        Get entity handler for config.

        Args:
            entity_config: Entity configuration

        Returns:
            EntityHandler instance

        Raises:
            ValueError: If handler not found
        """
        return EntityRegistry.create_handler(entity_config)

    def _validate_dependencies(self, handler: EntityHandler) -> None:
        """
        Validate that all dependencies exist.

        Args:
            handler: Entity handler

        Raises:
            DependencyValidationError: If dependency missing
        """
        dependencies = handler.get_dependencies()

        for dep_fqn in dependencies:
            if not self.context.entity_exists(dep_fqn):
                raise DependencyValidationError(
                    message=f"Missing dependency: {dep_fqn}",
                    entity_type=handler.entity_type,
                    entity_name=handler.name,
                    missing_dependency=dep_fqn,
                )

    def _check_entity_exists(self, fqn: str, entity_type: Any) -> Optional[Any]:
        """
        Check if entity exists in OpenMetadata.

        Args:
            fqn: Fully qualified name
            entity_type: Entity type

        Returns:
            Existing entity or None
        """
        if self.context.dry_run:
            # In dry run, check local cache only
            return self.context.get_entity(fqn)

        # Check OpenMetadata
        try:
            return self.context.client.get_entity(entity_type, fqn)
        except Exception:
            # Entity doesn't exist
            return None

    def _detect_schema_changes(
        self,
        handler: EntityHandler,
        existing_entity: Optional[Any],
        new_entity: Any,
    ) -> Optional[SchemaComparison]:
        """
        Detect schema changes if applicable.

        Args:
            handler: Entity handler
            existing_entity: Existing entity (if any)
            new_entity: New entity

        Returns:
            SchemaComparison or None
        """
        if not handler.supports_schema_evolution:
            return None

        if existing_entity is None:
            return None

        # Detect changes based on entity type
        # For now, only support tables
        return SchemaComparator.compare_table_schemas(existing_entity, new_entity)

    def _apply_idempotency(
        self,
        entity_config: EntityConfig,
        existing_entity: Optional[Any],
        new_entity: Any,
        schema_changes: Optional[SchemaComparison],
    ) -> IdempotencyDecision:
        """
        Apply idempotency strategy.

        Args:
            entity_config: Entity configuration
            existing_entity: Existing entity (if any)
            new_entity: New entity
            schema_changes: Schema comparison (if applicable)

        Returns:
            IdempotencyDecision
        """
        # Determine mode (entity-level override or default)
        mode = entity_config.idempotency or self.context.config.defaults.idempotency

        # Get strategy
        strategy = IdempotencyStrategyFactory.get_strategy(mode)

        # Make decision
        return strategy.decide(
            entity_exists=existing_entity is not None,
            existing_entity=existing_entity,
            new_entity=new_entity,
            schema_changes=schema_changes,
        )
