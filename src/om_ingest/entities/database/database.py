"""Database entity handler."""

from typing import List

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.entity.data.database import Database
from pydantic import BaseModel

from om_ingest.config.schema import EntityType
from om_ingest.entities.base import EntityHandler, EntityValidationError
from om_ingest.entities.registry import EntityRegistry


@EntityRegistry.register(EntityType.DATABASE)
class DatabaseHandler(EntityHandler):
    """Handler for Database entities."""

    entity_type = EntityType.DATABASE
    om_entity_class = Database
    supports_schema_evolution = False

    def validate(self) -> None:
        """Validate database configuration."""
        super().validate()

        # Validate required properties
        self.get_property("service", required=True)

    def build_entity(self) -> CreateDatabaseRequest:
        """Build database entity."""
        service_name = self.get_property("service", required=True)

        # Build create request
        create_request = CreateDatabaseRequest(
            name=self.name,
            description=self.description,
            service=service_name,
        )

        return create_request

    def get_fqn(self) -> str:
        """Get fully qualified name for database."""
        service_name = self.get_property("service", required=True)
        return f"{service_name}.{self.name}"

    def get_dependencies(self) -> List[str]:
        """Database depends on its service."""
        service_name = self.get_property("service", required=True)
        return [service_name]
