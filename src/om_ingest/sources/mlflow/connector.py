"""MLflow data source connector."""

import logging
import os
import re
from typing import Any, Dict, Iterator, List, Optional

try:
    from mlflow import MlflowClient
    from mlflow.exceptions import MlflowException
except ImportError:
    MlflowClient = None
    MlflowException = Exception

from om_ingest.config.schema import EntityConfig, EntityType, SourceType
from om_ingest.sources.base import DataSource, DataSourceError
from om_ingest.sources.registry import SourceRegistry

logger = logging.getLogger(__name__)


@SourceRegistry.register(SourceType.MLFLOW)
class MLflowConnector(DataSource):
    """
    MLflow Model Registry connector.

    Discovers registered ML models from MLflow tracking server.

    Configuration properties:
    - tracking_uri: MLflow tracking server URI (required)
    - registry_uri: MLflow registry URI (optional, defaults to tracking_uri)
    - service_name: ML Model Service name (optional, default: "{connector_name}_service")
    - username: MLflow username for basic auth (optional, can use env var)
    - password: MLflow password for basic auth (optional, can use env var)
    """

    def __init__(self, config):
        """Initialize MLflow connector."""
        super().__init__(config)

        if MlflowClient is None:
            raise DataSourceError(
                "mlflow is required for MLflow connector. Install with: pip install mlflow"
            )

        # Extract configuration
        self.tracking_uri = self.properties.get("tracking_uri")
        if not self.tracking_uri:
            raise DataSourceError("MLflow tracking_uri is required in source configuration")

        self.registry_uri = self.properties.get("registry_uri", self.tracking_uri)

        # Entity naming configuration
        self.service_name = self.properties.get("service_name", f"{self.name}_service")

        # Auth credentials (from properties or env vars)
        self.username = self.properties.get("username")
        self.password = self.properties.get("password")

        # State
        self.mlflow_client: Optional[MlflowClient] = None
        self._discovered_models: Optional[List[Dict[str, Any]]] = None

    @property
    def source_type(self) -> str:
        """Get source type identifier."""
        return "mlflow"

    @property
    def supported_entity_types(self) -> List[EntityType]:
        """Get supported entity types."""
        return [EntityType.ML_MODEL_SERVICE, EntityType.ML_MODEL]

    def connect(self) -> None:
        """
        Establish connection to MLflow tracking server.

        Raises:
            DataSourceError: If connection fails
        """
        try:
            # Set auth environment variables if provided
            if self.username:
                os.environ["MLFLOW_TRACKING_USERNAME"] = self.username
            if self.password:
                os.environ["MLFLOW_TRACKING_PASSWORD"] = self.password

            # Create MLflow client
            self.mlflow_client = MlflowClient(
                tracking_uri=self.tracking_uri,
                registry_uri=self.registry_uri,
            )

            # Test connection by attempting to list models (limit 1 for speed)
            self.mlflow_client.search_registered_models(max_results=1)

            self._connected = True
            logger.info(f"Connected to MLflow at {self.tracking_uri}")

        except MlflowException as e:
            raise DataSourceError(f"Failed to connect to MLflow: {e}")
        except Exception as e:
            raise DataSourceError(f"Unexpected error connecting to MLflow: {e}")

    def disconnect(self) -> None:
        """Close MLflow connection."""
        self.mlflow_client = None
        self._connected = False
        self._discovered_models = None
        logger.info("Disconnected from MLflow")

    def validate_connection(self) -> bool:
        """
        Test that connection is working.

        Returns:
            True if connection is valid, False otherwise
        """
        if not self._connected or not self.mlflow_client:
            return False

        try:
            self.mlflow_client.search_registered_models(max_results=1)
            return True
        except Exception:
            return False

    def discover_entities(
        self,
        entity_type: EntityType,
        filters: Optional[Dict[str, Any]] = None,
        include_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
    ) -> Iterator[EntityConfig]:
        """
        Discover entities from MLflow.

        Args:
            entity_type: Type of entity to discover
            filters: Source-specific filters (not used currently)
            include_pattern: Regex pattern for names to include
            exclude_pattern: Regex pattern for names to exclude

        Yields:
            EntityConfig objects

        Raises:
            DataSourceError: If not connected or discovery fails
        """
        if not self._connected:
            raise DataSourceError("Not connected. Call connect() first.")

        # Lazy discovery - discover once and cache
        if self._discovered_models is None:
            self._discovered_models = self._discover_mlflow_models()

        if entity_type == EntityType.ML_MODEL_SERVICE:
            # Service entity - yield single config
            yield self._create_ml_model_service_config()

        elif entity_type == EntityType.ML_MODEL:
            # Model entities - yield one per discovered model
            for model_info in self._discovered_models:
                # Apply filtering
                if include_pattern and not re.match(include_pattern, model_info["name"]):
                    continue
                if exclude_pattern and re.match(exclude_pattern, model_info["name"]):
                    continue

                yield self._create_ml_model_config(model_info)

        else:
            raise DataSourceError(f"Unsupported entity type for MLflow: {entity_type}")

    def extract_schema(
        self, entity_type: EntityType, entity_identifier: str
    ) -> Dict[str, Any]:
        """
        Extract schema/metadata for an entity.

        Args:
            entity_type: Type of entity
            entity_identifier: Entity identifier (model name)

        Returns:
            Schema metadata dictionary

        Raises:
            DataSourceError: If entity not found or extraction fails
        """
        if entity_type != EntityType.ML_MODEL:
            raise DataSourceError("Schema extraction only supported for ML_MODEL")

        if self._discovered_models is None:
            self._discovered_models = self._discover_mlflow_models()

        model_info = self._find_model(entity_identifier)
        return model_info.get("metadata", {})

    def fetch_sample_data(
        self,
        entity_type: EntityType,
        entity_identifier: str,
        sample_size: Optional[int] = None,
    ) -> Any:
        """
        Fetch sample data (model artifacts/metadata) for profiling.

        Args:
            entity_type: Type of entity
            entity_identifier: Entity identifier (model name)
            sample_size: Not used for ML models

        Returns:
            Profiling data dictionary

        Raises:
            DataSourceError: If entity not found
        """
        if entity_type != EntityType.ML_MODEL:
            raise DataSourceError("Sample data only supported for ML_MODEL")

        model_info = self._find_model(entity_identifier)

        # Return profiling data
        return {
            "parameters": model_info.get("parameters", {}),
            "metrics": model_info.get("metrics", {}),
            "tags": model_info.get("tags", {}),
            "signature": model_info.get("signature"),
        }

    def _discover_mlflow_models(self) -> List[Dict[str, Any]]:
        """
        Discover all registered models (latest version only).

        Returns:
            List of model info dictionaries

        Raises:
            DataSourceError: If discovery fails
        """
        logger.info("Discovering MLflow models...")

        models = []

        try:
            # Get all registered models
            registered_models = self.mlflow_client.search_registered_models()

            for reg_model in registered_models:
                try:
                    model_name = reg_model.name
                    logger.debug(f"Processing model: {model_name}")

                    # Get latest version
                    versions = self.mlflow_client.search_model_versions(
                        f"name='{model_name}'",
                        order_by=["version_number DESC"],
                        max_results=1,
                    )

                    if not versions:
                        logger.warning(f"No versions found for model: {model_name}")
                        continue

                    latest_version = versions[0]

                    # Extract metadata
                    model_info = self._extract_model_metadata(reg_model, latest_version)
                    models.append(model_info)

                except Exception as e:
                    logger.error(f"Failed to process model {model_name}: {e}")
                    continue

            logger.info(f"Discovered {len(models)} MLflow models")
            return models

        except MlflowException as e:
            raise DataSourceError(f"Failed to discover models: {e}")

    def _extract_model_metadata(self, reg_model, model_version) -> Dict[str, Any]:
        """
        Extract metadata from a model version.

        Args:
            reg_model: RegisteredModel object
            model_version: ModelVersion object

        Returns:
            Model info dictionary
        """
        model_info = {
            "name": reg_model.name,
            "description": reg_model.description or f"Model: {reg_model.name}",
            "version": model_version.version,
            "status": model_version.status,
            "source": model_version.source,
        }

        # Get run details if available
        if model_version.run_id:
            try:
                run = self.mlflow_client.get_run(model_version.run_id)
                model_info["parameters"] = dict(run.data.params)
                model_info["metrics"] = {k: float(v) for k, v in run.data.metrics.items()}
                model_info["tags"] = dict(run.data.tags)
            except Exception as e:
                logger.warning(f"Failed to get run details for {reg_model.name}: {e}")
                model_info["parameters"] = {}
                model_info["metrics"] = {}
                model_info["tags"] = {}

        # Get model signature if available
        try:
            import mlflow
            import mlflow.models

            # Set tracking URI for this operation
            mlflow.set_tracking_uri(self.tracking_uri)

            model_uri = f"models:/{reg_model.name}/{model_version.version}"
            model_meta = mlflow.models.get_model_info(model_uri)

            if model_meta.signature:
                model_info["signature"] = {
                    "inputs": str(model_meta.signature.inputs),
                    "outputs": str(model_meta.signature.outputs),
                }

            if model_meta.flavors:
                model_info["flavors"] = list(model_meta.flavors.keys())
        except Exception as e:
            logger.warning(f"Failed to get model signature for {reg_model.name}: {e}")

        return model_info

    def _create_ml_model_service_config(self) -> EntityConfig:
        """
        Create EntityConfig for ML Model Service.

        Returns:
            EntityConfig for service
        """
        return EntityConfig(
            type=EntityType.ML_MODEL_SERVICE,
            name=self.service_name,
            properties={
                "service_type": "Mlflow",
                "tracking_uri": self.tracking_uri,
                "registry_uri": self.registry_uri,
                "description": f"MLflow service at {self.tracking_uri}",
            },
        )

    def _create_ml_model_config(self, model_info: Dict[str, Any]) -> EntityConfig:
        """
        Create EntityConfig for ML Model.

        Args:
            model_info: Model information dictionary

        Returns:
            EntityConfig for model
        """
        # Build ML features from signature
        ml_features = None
        if "signature" in model_info and model_info["signature"]:
            ml_features = self._parse_model_signature(model_info["signature"])

        # Build hyperparameters
        ml_hyper_parameters = None
        if model_info.get("parameters"):
            ml_hyper_parameters = [
                {"name": k, "value": str(v)} for k, v in model_info["parameters"].items()
            ]

        # Build ML store
        ml_store = None
        if model_info.get("source"):
            ml_store = {"storage": model_info["source"]}

        # Determine algorithm from flavors (use first flavor as algorithm)
        algorithm = "mlmodel"  # Default
        if model_info.get("flavors"):
            algorithm = model_info["flavors"][0]

        return EntityConfig(
            type=EntityType.ML_MODEL,
            name=model_info["name"],
            properties={
                "service": self.service_name,
                "description": model_info.get("description"),
                "algorithm": algorithm,
                "mlFeatures": ml_features,
                "mlHyperParameters": ml_hyper_parameters,
                "mlStore": ml_store,
                "sourceUrl": f"{self.tracking_uri}/#/models/{model_info['name']}",
                # Store additional metadata (not part of OpenMetadata schema, but useful)
                "version": model_info.get("version"),
                "status": model_info.get("status"),
                "metrics": model_info.get("metrics", {}),
            },
        )

    def _parse_model_signature(self, signature: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Parse MLflow model signature to extract features.

        MLflow signatures contain input/output schemas. We parse the input schema
        to extract individual features (columns) with their data types.

        Args:
            signature: Dictionary with "inputs" and "outputs" keys (string representations)

        Returns:
            List of feature dictionaries with name, dataType, and description
        """
        if not signature or "inputs" not in signature:
            return None

        features = []
        inputs_str = signature["inputs"]

        try:
            # MLflow signature inputs are string representations like:
            # "['col1': string, 'col2': double, 'col3': long]"
            # or tensor representations

            # Try to parse column-based signatures
            if "[" in inputs_str and "]" in inputs_str:
                # Extract content between brackets
                content = inputs_str.strip()
                if content.startswith("[") and content.endswith("]"):
                    content = content[1:-1]

                # Parse individual columns
                # Format: 'name': type, 'name2': type2
                import re
                pattern = r"'([^']+)':\s*(\w+)"
                matches = re.findall(pattern, content)

                for col_name, col_type in matches:
                    features.append({
                        "name": col_name,
                        "dataType": self._map_mlflow_type_to_feature_type(col_type),
                        "description": f"Input feature: {col_name} ({col_type})",
                    })

            # If no features parsed, try alternative parsing for tensor signatures
            if not features and ":" in inputs_str:
                # Tensor format might be like "Tensor('double', (-1, 10))"
                # For now, create a generic feature
                features.append({
                    "name": "input_tensor",
                    "dataType": "numerical",
                    "description": f"Model input: {inputs_str}",
                })

            # If still no features, return None (will be handled upstream)
            return features if features else None

        except Exception as e:
            logger.warning(f"Failed to parse model signature: {e}")
            # Return a single generic feature as fallback
            return [{
                "name": "input_schema",
                "dataType": "categorical",
                "description": f"Input: {inputs_str}",
            }]

    def _map_mlflow_type_to_feature_type(self, mlflow_type: str) -> str:
        """
        Map MLflow data types to OpenMetadata FeatureType.

        Args:
            mlflow_type: MLflow data type (e.g., "string", "double", "long", "boolean")

        Returns:
            OpenMetadata FeatureType value ("numerical" or "categorical")
        """
        mlflow_type_lower = mlflow_type.lower()

        # Numerical types
        numerical_types = {
            "double", "float", "long", "integer", "int", "int32", "int64",
            "float32", "float64", "number", "numeric", "decimal"
        }

        if mlflow_type_lower in numerical_types:
            return "numerical"

        # Everything else defaults to categorical (strings, booleans, etc.)
        return "categorical"

    def _find_model(self, entity_identifier: str) -> Dict[str, Any]:
        """
        Find model in discovered models.

        Args:
            entity_identifier: Model name

        Returns:
            Model info dictionary

        Raises:
            DataSourceError: If model not found
        """
        if self._discovered_models is None:
            raise DataSourceError("Models not discovered yet")

        for model in self._discovered_models:
            if model["name"] == entity_identifier:
                return model

        raise DataSourceError(f"Model not found: {entity_identifier}")
