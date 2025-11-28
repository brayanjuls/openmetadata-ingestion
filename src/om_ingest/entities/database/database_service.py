"""Database service entity handler."""

from typing import List

from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseService,
    DatabaseServiceType,
)
from metadata.generated.schema.entity.services.connections.database.datalakeConnection import (
    DatalakeConnection,
)
from pydantic import BaseModel

from om_ingest.config.schema import EntityType
from om_ingest.entities.base import EntityHandler, EntityValidationError
from om_ingest.entities.registry import EntityRegistry


@EntityRegistry.register(EntityType.DATABASE_SERVICE)
class DatabaseServiceHandler(EntityHandler):
    """Handler for Database Service entities."""

    entity_type = EntityType.DATABASE_SERVICE
    om_entity_class = DatabaseService
    supports_schema_evolution = False

    def validate(self) -> None:
        """Validate database service configuration."""
        super().validate()

        # Validate required properties
        service_type = self.get_property("service_type", required=True)

        # Validate service type is valid
        try:
            DatabaseServiceType(service_type)
        except ValueError:
            raise EntityValidationError(
                f"Invalid service_type '{service_type}'. "
                f"Must be one of: {[t.value for t in DatabaseServiceType]}"
            )

    def build_entity(self) -> CreateDatabaseServiceRequest:
        """Build database service entity."""
        service_type = DatabaseServiceType(
            self.get_property("service_type", required=True)
        )

        # Build connection config based on service type
        connection_config = self._build_connection_config(service_type)

        # Build database connection
        database_connection = DatabaseConnection(
            config=connection_config,
        )

        # Build create request
        create_request = CreateDatabaseServiceRequest(
            name=self.name,
            serviceType=service_type,
            description=self.description,
            connection=database_connection,
        )

        return create_request

    def _build_connection_config(self, service_type: DatabaseServiceType):
        """
        Build connection configuration based on service type.

        Args:
            service_type: Database service type

        Returns:
            Connection configuration object
        """
        # For Datalake (S3 Hudi)
        if service_type == DatabaseServiceType.Datalake:
            config_source = self.get_property("config_source", required=False)

            connection = DatalakeConnection(
                configSource=config_source if config_source else {}
            )
            return connection

        # For other database types, we can add more specific connection configs
        # For now, return a minimal connection
        # This will need to be expanded based on the specific database type

        return {}

    def get_fqn(self) -> str:
        """Get fully qualified name for database service."""
        return self.name

    def get_dependencies(self) -> List[str]:
        """Database services have no dependencies."""
        return []
