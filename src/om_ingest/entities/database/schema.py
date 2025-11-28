"""Database schema entity handler."""

from typing import List

from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from pydantic import BaseModel

from om_ingest.config.schema import EntityType
from om_ingest.entities.base import EntityHandler, EntityValidationError
from om_ingest.entities.registry import EntityRegistry


@EntityRegistry.register(EntityType.DATABASE_SCHEMA)
class DatabaseSchemaHandler(EntityHandler):
    """Handler for Database Schema entities."""

    entity_type = EntityType.DATABASE_SCHEMA
    om_entity_class = DatabaseSchema
    supports_schema_evolution = False

    def validate(self) -> None:
        """Validate database schema configuration."""
        super().validate()

        # Validate required properties
        self.get_property("database", required=True)
        self.get_property("service", required=True)

    def build_entity(self) -> CreateDatabaseSchemaRequest:
        """Build database schema entity."""
        database_name = self.get_property("database", required=True)
        service_name = self.get_property("service", required=True)

        # Construct database FQN
        database_fqn = f"{service_name}.{database_name}"

        # Build create request
        create_request = CreateDatabaseSchemaRequest(
            name=self.name,
            description=self.description,
            database=database_fqn,
        )

        return create_request

    def get_fqn(self) -> str:
        """Get fully qualified name for database schema."""
        database_name = self.get_property("database", required=True)
        service_name = self.get_property("service", required=True)
        return f"{service_name}.{database_name}.{self.name}"

    def get_dependencies(self) -> List[str]:
        """Database schema depends on its database."""
        database_name = self.get_property("database", required=True)
        service_name = self.get_property("service", required=True)
        database_fqn = f"{service_name}.{database_name}"
        return [database_fqn]
