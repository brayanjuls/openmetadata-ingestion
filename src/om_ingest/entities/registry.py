"""Entity handler registry."""

from typing import Dict, Type

from om_ingest.config.schema import EntityConfig, EntityType
from om_ingest.entities.base import EntityHandler


class EntityRegistry:
    """
    Registry for entity handlers.

    Provides decorator-based registration and lookup of entity handlers.
    """

    _handlers: Dict[EntityType, Type[EntityHandler]] = {}

    @classmethod
    def register(cls, entity_type: EntityType):
        """
        Decorator to register an entity handler.

        Args:
            entity_type: Entity type to register handler for

        Returns:
            Decorator function

        Example:
            @EntityRegistry.register(EntityType.TABLE)
            class TableHandler(EntityHandler):
                ...
        """

        def decorator(handler_class: Type[EntityHandler]):
            # Validate that the handler class has the entity_type attribute
            if not hasattr(handler_class, "entity_type"):
                handler_class.entity_type = entity_type

            # Register the handler
            cls._handlers[entity_type] = handler_class
            return handler_class

        return decorator

    @classmethod
    def get_handler_class(cls, entity_type: EntityType) -> Type[EntityHandler]:
        """
        Get handler class for an entity type.

        Args:
            entity_type: Entity type

        Returns:
            Handler class

        Raises:
            ValueError: If no handler registered for type
        """
        if entity_type not in cls._handlers:
            raise ValueError(f"No handler registered for entity type: {entity_type}")

        return cls._handlers[entity_type]

    @classmethod
    def create_handler(cls, config: EntityConfig) -> EntityHandler:
        """
        Create a handler instance for an entity configuration.

        Args:
            config: Entity configuration

        Returns:
            Handler instance

        Raises:
            ValueError: If no handler registered for entity type
        """
        handler_class = cls.get_handler_class(config.type)
        return handler_class(config)

    @classmethod
    def list_registered_types(cls) -> list[EntityType]:
        """
        Get list of all registered entity types.

        Returns:
            List of registered entity types
        """
        return list(cls._handlers.keys())

    @classmethod
    def is_registered(cls, entity_type: EntityType) -> bool:
        """
        Check if an entity type is registered.

        Args:
            entity_type: Entity type to check

        Returns:
            True if registered, False otherwise
        """
        return entity_type in cls._handlers

    @classmethod
    def clear_registry(cls) -> None:
        """
        Clear all registered handlers.

        Mainly for testing purposes.
        """
        cls._handlers.clear()
