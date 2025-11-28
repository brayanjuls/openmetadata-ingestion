"""Configuration schema using Pydantic models for YAML validation."""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class IdempotencyMode(str, Enum):
    """Entity idempotency handling mode."""

    SKIP = "skip"  # Skip if exists (default)
    UPDATE = "update"  # Update if exists
    FAIL = "fail"  # Fail if exists


class AuthType(str, Enum):
    """OpenMetadata authentication type."""

    NO_AUTH = "no_auth"
    BASIC = "basic"
    JWT = "jwt"
    OPENMETADATA = "openmetadata"


class EventType(str, Enum):
    """Audit event types."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    DRY_RUN = "dry_run"
    VALIDATION_ERROR = "validation_error"


class Operation(str, Enum):
    """Entity operations."""

    CREATE = "create"
    UPDATE = "update"
    SKIP = "skip"
    DELETE = "delete"


class EntityType(str, Enum):
    """Supported entity types."""

    # Database entities
    DATABASE_SERVICE = "database_service"
    DATABASE = "database"
    DATABASE_SCHEMA = "database_schema"
    TABLE = "table"

    # Pipeline entities
    PIPELINE_SERVICE = "pipeline_service"
    PIPELINE = "pipeline"
    TASK = "task"

    # Messaging entities
    MESSAGING_SERVICE = "messaging_service"
    TOPIC = "topic"

    # ML entities
    ML_MODEL_SERVICE = "ml_model_service"
    ML_MODEL = "ml_model"

    # Search entities
    SEARCH_SERVICE = "search_service"
    SEARCH_INDEX = "search_index"

    # Governance entities
    TAG_CATEGORY = "tag_category"
    TAG = "tag"
    USER = "user"
    TEAM = "team"
    GLOSSARY = "glossary"
    GLOSSARY_TERM = "glossary_term"


class SourceType(str, Enum):
    """Data source types."""

    S3_HUDI = "s3_hudi"
    POSTGRES = "postgres"
    OPENSEARCH = "opensearch"
    AIRFLOW = "airflow"
    MLFLOW = "mlflow"
    KAFKA = "kafka"


class MetadataConfig(BaseModel):
    """Ingestion metadata configuration."""

    name: str = Field(..., description="Ingestion job name")
    version: str = Field(default="1.0", description="Configuration version")
    description: Optional[str] = Field(None, description="Job description")


class AuthConfig(BaseModel):
    """Authentication configuration."""

    type: AuthType = Field(default=AuthType.NO_AUTH, description="Auth type")
    username: Optional[str] = Field(None, description="Username for basic auth")
    password: Optional[str] = Field(None, description="Password for basic auth")
    jwt_token: Optional[str] = Field(None, description="JWT token")

    @field_validator("password", "jwt_token", mode="before")
    @classmethod
    def substitute_env_vars(cls, v: Optional[str]) -> Optional[str]:
        """Substitute environment variables in sensitive fields."""
        if v and v.startswith("${") and v.endswith("}"):
            import os

            var_name = v[2:-1]
            return os.getenv(var_name)
        return v


class OpenMetadataConfig(BaseModel):
    """OpenMetadata connection configuration."""

    host: str = Field(..., description="OpenMetadata server URL")
    auth: Optional[AuthConfig] = Field(
        default_factory=lambda: AuthConfig(), description="Authentication config"
    )
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    api_version: str = Field(default="v1", description="API version")


class DiscoveryConfig(BaseModel):
    """Entity discovery configuration."""

    source: str = Field(..., description="Source name to discover from")
    filter: Optional[Dict[str, Any]] = Field(
        default=None, description="Discovery filters"
    )
    include_pattern: Optional[str] = Field(None, description="Include regex pattern")
    exclude_pattern: Optional[str] = Field(None, description="Exclude regex pattern")


class ProfilingMetrics(BaseModel):
    """Data profiling metrics configuration."""

    row_count: bool = Field(default=True, description="Include row count")
    null_percentage: bool = Field(default=True, description="Include null percentage")
    distinct_count: bool = Field(default=True, description="Include distinct count")
    min_max: bool = Field(default=True, description="Include min/max values")
    mean_median: bool = Field(default=True, description="Include mean/median")
    std_dev: bool = Field(default=True, description="Include standard deviation")


class ProfilingConfig(BaseModel):
    """Profiling configuration."""

    enabled: bool = Field(default=False, description="Enable profiling")
    sample_percentage: float = Field(
        default=10.0, ge=0.0, le=100.0, description="Sample percentage"
    )
    metrics: Optional[ProfilingMetrics] = Field(
        default_factory=ProfilingMetrics, description="Metrics to collect"
    )
    push_to_openmetadata: bool = Field(
        default=False, description="Push profiles to OpenMetadata"
    )


class AuditConfig(BaseModel):
    """Audit logging configuration."""

    enabled: bool = Field(default=True, description="Enable audit logging")
    output_dir: str = Field(default="./audit_logs", description="Audit log directory")
    include_success: bool = Field(
        default=True, description="Log successful operations"
    )
    include_skipped: bool = Field(default=True, description="Log skipped operations")


class ExecutionConfig(BaseModel):
    """Execution configuration."""

    dry_run: bool = Field(default=False, description="Dry run mode")
    continue_on_error: bool = Field(
        default=True, description="Continue on entity errors (not dependency errors)"
    )
    fail_fast_on_dependency: bool = Field(
        default=True, description="Fail fast on dependency validation errors"
    )


class DefaultsConfig(BaseModel):
    """Default configuration values."""

    idempotency: IdempotencyMode = Field(
        default=IdempotencyMode.SKIP, description="Default idempotency mode"
    )
    profiling: Optional[ProfilingConfig] = Field(
        default=None, description="Default profiling config"
    )


class EntityConfig(BaseModel):
    """Entity configuration."""

    type: EntityType = Field(..., description="Entity type")
    name: Optional[str] = Field(None, description="Entity name")
    fqn: Optional[str] = Field(None, description="Fully qualified name")
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Entity-specific properties"
    )
    discovery: Optional[DiscoveryConfig] = Field(
        None, description="Discovery configuration"
    )
    idempotency: Optional[IdempotencyMode] = Field(
        None, description="Idempotency mode override"
    )
    profiling: Optional[ProfilingConfig] = Field(
        None, description="Profiling config override"
    )

    @model_validator(mode="after")
    def validate_name_or_discovery(self):
        """Ensure either name or discovery is provided."""
        if not self.name and not self.discovery:
            raise ValueError("Either 'name' or 'discovery' must be provided")
        return self


class SourceConfig(BaseModel):
    """Data source configuration."""

    name: str = Field(..., description="Source name (used for reference)")
    type: SourceType = Field(..., description="Source type")
    properties: Dict[str, Any] = Field(
        default_factory=dict, description="Source-specific connection properties"
    )

    @field_validator("properties", mode="before")
    @classmethod
    def substitute_env_vars_in_properties(
        cls, v: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Substitute environment variables in property values."""
        if not v:
            return v

        import os

        result = {}
        for key, value in v.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                var_name = value[2:-1]
                result[key] = os.getenv(var_name, value)
            else:
                result[key] = value
        return result


class IngestionConfig(BaseModel):
    """Root ingestion configuration."""

    metadata: MetadataConfig = Field(..., description="Ingestion metadata")
    openmetadata: OpenMetadataConfig = Field(
        ..., description="OpenMetadata connection"
    )
    sources: Optional[List[SourceConfig]] = Field(
        default=None, description="Data sources"
    )
    defaults: Optional[DefaultsConfig] = Field(
        default_factory=DefaultsConfig, description="Default configuration"
    )
    entities: List[EntityConfig] = Field(..., description="Entities to ingest")
    audit: Optional[AuditConfig] = Field(
        default_factory=AuditConfig, description="Audit configuration"
    )
    execution: Optional[ExecutionConfig] = Field(
        default_factory=ExecutionConfig, description="Execution configuration"
    )

    @model_validator(mode="after")
    def validate_source_references(self):
        """Validate that entity discovery references existing sources."""
        if not self.sources:
            source_names = set()
        else:
            source_names = {source.name for source in self.sources}

        for entity in self.entities:
            if entity.discovery and entity.discovery.source not in source_names:
                raise ValueError(
                    f"Entity {entity.type} references unknown source: {entity.discovery.source}"
                )

        return self

    class Config:
        """Pydantic config."""

        use_enum_values = False
        validate_assignment = True
