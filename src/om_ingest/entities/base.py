"""Base entity handler interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type

from pydantic import BaseModel

from om_ingest.config.schema import EntityConfig, EntityType


class EntityValidationError(Exception):
    """Raised when entity validation fails."""

    pass


class EntityHandler(ABC):
    """
    Abstract base class for entity handlers.

    Each entity type (Table, Database, etc.) has a handler that knows how to:
    - Convert config to OpenMetadata entity
    - Extract dependencies
    - Validate configuration
    - Determine FQN
    """

    # Entity type this handler is responsible for
    entity_type: EntityType

    # OpenMetadata entity class this handler creates
    om_entity_class: Type[BaseModel]

    # Whether this entity type supports schema evolution tracking
    supports_schema_evolution: bool = False

    def __init__(self, config: EntityConfig):
        """
        Initialize entity handler.

        Args:
            config: Entity configuration

        Raises:
            EntityValidationError: If configuration is invalid
        """
        if config.type != self.entity_type:
            raise EntityValidationError(
                f"Config type {config.type} doesn't match handler type {self.entity_type}"
            )

        self.config = config
        self.validate()

    @abstractmethod
    def build_entity(self) -> BaseModel:
        """
        Build OpenMetadata entity from configuration.

        Returns:
            OpenMetadata entity object

        Raises:
            EntityValidationError: If entity cannot be built
        """
        pass

    @abstractmethod
    def get_fqn(self) -> str:
        """
        Get the fully qualified name for this entity.

        Returns:
            Fully qualified name

        Example FQNs:
            - Database Service: "service_name"
            - Database: "service_name.database_name"
            - Schema: "service_name.database_name.schema_name"
            - Table: "service_name.database_name.schema_name.table_name"
        """
        pass

    @abstractmethod
    def get_dependencies(self) -> List[str]:
        """
        Extract parent dependency FQNs from configuration.

        Returns:
            List of parent entity FQNs this entity depends on

        Example:
            A table depends on its schema:
            ["service_name.database_name.schema_name"]
        """
        pass

    def validate(self) -> None:
        """
        Validate entity configuration.

        Raises:
            EntityValidationError: If validation fails

        Default implementation does basic checks.
        Override for entity-specific validation.
        """
        if not self.config.name and not self.config.discovery:
            raise EntityValidationError(
                f"{self.entity_type.value}: Either 'name' or 'discovery' must be provided"
            )

    def get_property(self, key: str, required: bool = False) -> Optional[Any]:
        """
        Get a property from entity configuration.

        Args:
            key: Property key
            required: If True, raise error if missing

        Returns:
            Property value or None

        Raises:
            EntityValidationError: If required property is missing
        """
        value = self.config.properties.get(key)

        if value is None and required:
            raise EntityValidationError(
                f"{self.entity_type.value} '{self.config.name}': Missing required property '{key}'"
            )

        return value

    def get_property_or_default(self, key: str, default: Any) -> Any:
        """
        Get a property with a default value.

        Args:
            key: Property key
            default: Default value if not found

        Returns:
            Property value or default
        """
        return self.config.properties.get(key, default)

    @property
    def name(self) -> str:
        """Get entity name."""
        if not self.config.name:
            raise EntityValidationError(
                f"{self.entity_type.value}: Name not available (discovery entity?)"
            )
        return self.config.name

    @property
    def description(self) -> Optional[str]:
        """Get entity description from properties."""
        return self.get_property("description")

    def __repr__(self) -> str:
        """String representation."""
        return f"<{self.__class__.__name__} type={self.entity_type.value} name={self.config.name}>"
