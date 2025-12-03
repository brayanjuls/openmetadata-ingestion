"""ML Model Service entity handler."""

from typing import List

from metadata.generated.schema.api.services.createMlModelService import (
    CreateMlModelServiceRequest,
)
from metadata.generated.schema.entity.services.mlmodelService import (
    MlModelConnection,
    MlModelService,
    MlModelServiceType,
)
from metadata.generated.schema.entity.services.connections.mlmodel.mlflowConnection import (
    MlflowConnection,
)
from pydantic import BaseModel

from om_ingest.config.schema import EntityType
from om_ingest.entities.base import EntityHandler, EntityValidationError
from om_ingest.entities.registry import EntityRegistry


@EntityRegistry.register(EntityType.ML_MODEL_SERVICE)
class MLModelServiceHandler(EntityHandler):
    """Handler for ML Model Service entities."""

    entity_type = EntityType.ML_MODEL_SERVICE
    om_entity_class = MlModelService
    supports_schema_evolution = False

    def validate(self) -> None:
        """Validate ML model service configuration."""
        super().validate()

        # Validate required properties
        service_type = self.get_property("service_type", required=True)

        # Validate service type is valid
        try:
            MlModelServiceType(service_type)
        except ValueError:
            raise EntityValidationError(
                f"Invalid service_type '{service_type}'. "
                f"Must be one of: {[t.value for t in MlModelServiceType]}"
            )

    def build_entity(self) -> CreateMlModelServiceRequest:
        """Build ML model service entity."""
        service_type = MlModelServiceType(
            self.get_property("service_type", required=True)
        )

        # Build connection config based on service type
        connection_config = self._build_connection_config(service_type)

        # Build ML model connection
        ml_model_connection = MlModelConnection(
            config=connection_config,
        )

        # Build create request
        create_request = CreateMlModelServiceRequest(
            name=self.name,
            serviceType=service_type,
            description=self.description,
            connection=ml_model_connection,
        )

        return create_request

    def _build_connection_config(self, service_type: MlModelServiceType):
        """
        Build connection configuration based on service type.

        Args:
            service_type: ML model service type

        Returns:
            Connection configuration object
        """
        # For MLflow
        if service_type == MlModelServiceType.Mlflow:
            tracking_uri = self.get_property("tracking_uri", required=True)
            registry_uri = self.get_property("registry_uri", required=False)

            connection = MlflowConnection(
                trackingUri=tracking_uri,
                registryUri=registry_uri if registry_uri else tracking_uri,
            )
            return connection

        # For other ML model service types, we can add more specific connection configs
        # For now, return a minimal connection
        return {}

    def get_fqn(self) -> str:
        """Get fully qualified name for ML model service."""
        return self.name

    def get_dependencies(self) -> List[str]:
        """ML model services have no dependencies."""
        return []
