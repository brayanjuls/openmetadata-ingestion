"""Idempotency strategies for entity processing."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from om_ingest.config.schema import IdempotencyMode
from om_ingest.core.schema_comparator import SchemaComparison


class IdempotencyAction(str, Enum):
    """Actions to take based on idempotency strategy."""

    CREATE = "create"  # Entity doesn't exist, create it
    UPDATE = "update"  # Entity exists, update it
    SKIP = "skip"  # Entity exists, skip it
    FAIL = "fail"  # Entity exists, fail


@dataclass
class IdempotencyDecision:
    """Decision from idempotency strategy."""

    action: IdempotencyAction
    reason: str
    existing_entity: Optional[Any] = None
    schema_changes: Optional[SchemaComparison] = None

    def should_proceed(self) -> bool:
        """Check if we should proceed with operation."""
        return self.action in (IdempotencyAction.CREATE, IdempotencyAction.UPDATE)

    def should_skip(self) -> bool:
        """Check if we should skip."""
        return self.action == IdempotencyAction.SKIP

    def should_fail(self) -> bool:
        """Check if we should fail."""
        return self.action == IdempotencyAction.FAIL


class IdempotencyStrategy(ABC):
    """
    Abstract base class for idempotency strategies.

    Determines what action to take when an entity already exists.
    """

    @abstractmethod
    def decide(
        self,
        entity_exists: bool,
        existing_entity: Optional[Any] = None,
        new_entity: Optional[Any] = None,
        schema_changes: Optional[SchemaComparison] = None,
    ) -> IdempotencyDecision:
        """
        Decide what action to take.

        Args:
            entity_exists: Whether entity already exists
            existing_entity: Existing entity from OpenMetadata (if exists)
            new_entity: New entity to be created/updated
            schema_changes: Schema comparison result (if applicable)

        Returns:
            IdempotencyDecision with action and reason
        """
        pass


class SkipStrategy(IdempotencyStrategy):
    """
    Skip if entity exists (default behavior).

    This is the safest strategy - never overwrites existing data.
    """

    def decide(
        self,
        entity_exists: bool,
        existing_entity: Optional[Any] = None,
        new_entity: Optional[Any] = None,
        schema_changes: Optional[SchemaComparison] = None,
    ) -> IdempotencyDecision:
        """Skip if entity exists, create if not."""
        if entity_exists:
            return IdempotencyDecision(
                action=IdempotencyAction.SKIP,
                reason="Entity already exists (skip mode)",
                existing_entity=existing_entity,
                schema_changes=schema_changes,
            )
        else:
            return IdempotencyDecision(
                action=IdempotencyAction.CREATE,
                reason="Entity does not exist",
            )


class UpdateStrategy(IdempotencyStrategy):
    """
    Update if entity exists, create if not.

    Updates existing entities with new data.
    Only triggers actual update if there are schema changes.
    """

    def decide(
        self,
        entity_exists: bool,
        existing_entity: Optional[Any] = None,
        new_entity: Optional[Any] = None,
        schema_changes: Optional[SchemaComparison] = None,
    ) -> IdempotencyDecision:
        """Update if entity exists, create if not."""
        if entity_exists:
            # Check if there are actual changes
            if schema_changes and schema_changes.has_changes:
                return IdempotencyDecision(
                    action=IdempotencyAction.UPDATE,
                    reason=f"Entity exists with schema changes: {schema_changes.summary()}",
                    existing_entity=existing_entity,
                    schema_changes=schema_changes,
                )
            else:
                # No changes detected, skip update
                return IdempotencyDecision(
                    action=IdempotencyAction.SKIP,
                    reason="Entity exists but no schema changes detected",
                    existing_entity=existing_entity,
                    schema_changes=schema_changes,
                )
        else:
            return IdempotencyDecision(
                action=IdempotencyAction.CREATE,
                reason="Entity does not exist",
            )


class FailStrategy(IdempotencyStrategy):
    """
    Fail if entity exists.

    Use when you want to ensure entities are created fresh.
    """

    def decide(
        self,
        entity_exists: bool,
        existing_entity: Optional[Any] = None,
        new_entity: Optional[Any] = None,
        schema_changes: Optional[SchemaComparison] = None,
    ) -> IdempotencyDecision:
        """Fail if entity exists, create if not."""
        if entity_exists:
            return IdempotencyDecision(
                action=IdempotencyAction.FAIL,
                reason="Entity already exists (fail mode)",
                existing_entity=existing_entity,
                schema_changes=schema_changes,
            )
        else:
            return IdempotencyDecision(
                action=IdempotencyAction.CREATE,
                reason="Entity does not exist",
            )


class IdempotencyStrategyFactory:
    """Factory for creating idempotency strategies."""

    _strategies = {
        IdempotencyMode.SKIP: SkipStrategy,
        IdempotencyMode.UPDATE: UpdateStrategy,
        IdempotencyMode.FAIL: FailStrategy,
    }

    @classmethod
    def get_strategy(cls, mode: IdempotencyMode) -> IdempotencyStrategy:
        """
        Get strategy instance for the given mode.

        Args:
            mode: Idempotency mode

        Returns:
            IdempotencyStrategy instance

        Raises:
            ValueError: If mode is not supported
        """
        strategy_class = cls._strategies.get(mode)
        if not strategy_class:
            raise ValueError(f"Unknown idempotency mode: {mode}")

        return strategy_class()

    @classmethod
    def register_strategy(
        cls, mode: IdempotencyMode, strategy_class: type[IdempotencyStrategy]
    ) -> None:
        """
        Register a custom strategy.

        Args:
            mode: Idempotency mode
            strategy_class: Strategy class
        """
        cls._strategies[mode] = strategy_class
