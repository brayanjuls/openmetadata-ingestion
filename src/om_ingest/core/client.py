"""OpenMetadata client wrapper for API interactions."""

import time
from typing import Any, Dict, Optional, Type, TypeVar

from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from pydantic import BaseModel

from om_ingest.config.schema import AuthType, OpenMetadataConfig

T = TypeVar("T", bound=BaseModel)


class OpenMetadataClientError(Exception):
    """Raised when OpenMetadata API operations fail."""

    pass


class EntityNotFoundError(OpenMetadataClientError):
    """Raised when an entity is not found."""

    pass


class OMClient:
    """Wrapper around OpenMetadata SDK for entity operations."""

    def __init__(
        self, config: OpenMetadataConfig, dry_run: bool = False
    ):
        """
        Initialize OpenMetadata client.

        Args:
            config: OpenMetadata connection configuration
            dry_run: If True, skip actual API calls

        Raises:
            OpenMetadataClientError: If connection fails
        """
        self.config = config
        self.dry_run = dry_run
        self._client: Optional[OpenMetadata] = None

        if not dry_run:
            self._connect()

    def _connect(self) -> None:
        """
        Establish connection to OpenMetadata server.

        Raises:
            OpenMetadataClientError: If connection fails
        """
        try:
            # Build OpenMetadata connection config
            # verifySSL must be a string enum: "no-ssl", "ignore", or "validate"
            verify_ssl = "validate" if self.config.verify_ssl else "ignore"

            # Validate JWT token is provided when using openmetadata auth
            auth_provider = self._get_auth_provider()
            if auth_provider == "openmetadata":
                if not self.config.auth or not self.config.auth.jwt_token:
                    raise OpenMetadataClientError(
                        "JWT token is required when using 'openmetadata' auth type. "
                        "Please set the OPENMETADATA_JWT_TOKEN environment variable or "
                        "provide jwt_token in the configuration. "
                        "Get your token from: http://localhost:8585 → Settings → Bots → ingestion-bot"
                    )

            # Build security config if JWT token is provided
            security_config = None
            if self.config.auth and self.config.auth.jwt_token:
                security_config = OpenMetadataJWTClientConfig(
                    jwtToken=self.config.auth.jwt_token
                )

            server_config = OpenMetadataConnection(
                hostPort=self.config.host,
                authProvider=self._get_auth_provider(),
                verifySSL=verify_ssl,
                securityConfig=security_config,
            )

            # Create client
            self._client = OpenMetadata(server_config)

            # Test connection
            health = self._client.health_check()
            if not health:
                raise OpenMetadataClientError(
                    f"OpenMetadata health check failed for {self.config.host}"
                )

        except Exception as e:
            raise OpenMetadataClientError(
                f"Failed to connect to OpenMetadata at {self.config.host}: {e}"
            )

    def _get_auth_provider(self) -> str:
        """
        Get auth provider string based on auth config.

        Returns:
            Auth provider name (must be valid OpenMetadata enum)
        """
        # For local development without auth, use "openmetadata"
        if not self.config.auth or self.config.auth.type == AuthType.NO_AUTH:
            return "openmetadata"
        elif self.config.auth.type == AuthType.OPENMETADATA:
            return "openmetadata"
        elif self.config.auth.type == AuthType.JWT:
            return "google"
        elif self.config.auth.type == AuthType.BASIC:
            return "basic"
        else:
            return "openmetadata"

    @property
    def client(self) -> OpenMetadata:
        """
        Get the underlying OpenMetadata client.

        Returns:
            OpenMetadata client instance

        Raises:
            OpenMetadataClientError: If in dry-run mode or not connected
        """
        if self.dry_run:
            raise OpenMetadataClientError(
                "Cannot access client in dry-run mode"
            )
        if not self._client:
            raise OpenMetadataClientError("Client not connected")
        return self._client

    def create_or_update(
        self,
        entity: BaseModel,
    ) -> BaseModel:
        """
        Create or update an entity in OpenMetadata.

        Args:
            entity: Pydantic entity model

        Returns:
            Created/updated entity from server

        Raises:
            OpenMetadataClientError: If operation fails
        """
        if self.dry_run:
            # In dry-run mode, just return the entity as-is
            return entity

        try:
            # Use the OpenMetadata SDK's create_or_update method
            result = self.client.create_or_update(entity)
            return result
        except Exception as e:
            entity_type = type(entity).__name__
            raise OpenMetadataClientError(
                f"Failed to create/update {entity_type}: {e}"
            )

    def get_by_name(
        self,
        entity_class: Type[T],
        fqn: str,
        fields: Optional[list[str]] = None,
    ) -> Optional[T]:
        """
        Get an entity by its fully qualified name.

        Args:
            entity_class: Entity class type
            fqn: Fully qualified name
            fields: Optional fields to include

        Returns:
            Entity if found, None otherwise

        Raises:
            OpenMetadataClientError: If operation fails
        """
        # Note: We DO NOT skip reads in dry-run mode
        # Dependency validation requires reading existing entities
        try:
            entity = self.client.get_by_name(
                entity=entity_class,
                fqn=fqn,
                fields=fields,
            )
            return entity
        except Exception:
            # Entity not found
            return None

    def entity_exists(self, entity_class: Type[T], fqn: str) -> bool:
        """
        Check if an entity exists by FQN.

        Args:
            entity_class: Entity class type
            fqn: Fully qualified name

        Returns:
            True if entity exists, False otherwise
        """
        if self.dry_run:
            return False

        entity = self.get_by_name(entity_class, fqn)
        return entity is not None

    def delete(self, entity_class: Type[T], fqn: str) -> None:
        """
        Delete an entity by FQN.

        Args:
            entity_class: Entity class type
            fqn: Fully qualified name

        Raises:
            OpenMetadataClientError: If operation fails
        """
        if self.dry_run:
            return

        try:
            self.client.delete(
                entity=entity_class,
                entity_id=fqn,
                hard_delete=False,
            )
        except Exception as e:
            raise OpenMetadataClientError(
                f"Failed to delete {entity_class.__name__} '{fqn}': {e}"
            )

    def retry_on_failure(
        self,
        func,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        *args,
        **kwargs,
    ) -> Any:
        """
        Retry a function on failure with exponential backoff.

        Args:
            func: Function to call
            max_retries: Maximum number of retries
            backoff_factor: Backoff multiplier
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Function result

        Raises:
            OpenMetadataClientError: If all retries fail
        """
        last_error = None
        wait_time = 1.0

        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                else:
                    raise OpenMetadataClientError(
                        f"Operation failed after {max_retries} retries: {last_error}"
                    )

    def create_entity(self, entity_type: Any, entity: BaseModel) -> BaseModel:
        """
        Create a new entity.

        Args:
            entity_type: Type of entity
            entity: Entity to create

        Returns:
            Created entity

        Raises:
            OpenMetadataClientError: If creation fails
        """
        return self.create_or_update(entity)

    def update_entity(
        self, entity_type: Any, fqn: str, entity: BaseModel
    ) -> BaseModel:
        """
        Update an existing entity.

        Args:
            entity_type: Type of entity
            fqn: Fully qualified name
            entity: Updated entity data

        Returns:
            Updated entity

        Raises:
            OpenMetadataClientError: If update fails
        """
        return self.create_or_update(entity)

    def get_entity(self, entity_type: Any, fqn: str) -> Optional[BaseModel]:
        """
        Get an entity by FQN.

        Args:
            entity_type: Type of entity (EntityType enum)
            fqn: Fully qualified name

        Returns:
            Entity if found, None otherwise

        Raises:
            OpenMetadataClientError: If retrieval fails
        """
        # Map EntityType enum to OpenMetadata SDK classes
        from om_ingest.config.schema import EntityType
        from metadata.generated.schema.entity.services.databaseService import DatabaseService
        from metadata.generated.schema.entity.data.database import Database
        from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
        from metadata.generated.schema.entity.data.table import Table

        type_mapping = {
            EntityType.DATABASE_SERVICE: DatabaseService,
            EntityType.DATABASE: Database,
            EntityType.DATABASE_SCHEMA: DatabaseSchema,
            EntityType.TABLE: Table,
        }

        entity_class = type_mapping.get(entity_type)
        if not entity_class:
            raise OpenMetadataClientError(f"Unsupported entity type: {entity_type}")

        return self.get_by_name(entity_class, fqn)

    def close(self) -> None:
        """Close the client connection."""
        if self._client:
            # The OpenMetadata SDK doesn't have an explicit close method
            # but we can clean up our reference
            self._client = None


# Alias for consistency
OpenMetadataClient = OMClient
