"""ML Model entity handler."""

from typing import Any, Dict, List, Optional

from metadata.generated.schema.api.data.createMlModel import CreateMlModelRequest
from metadata.generated.schema.entity.data.mlmodel import (
    FeatureType,
    MlFeature,
    MlHyperParameter,
    MlModel,
    MlStore,
)
from pydantic import BaseModel

from om_ingest.config.schema import EntityType
from om_ingest.entities.base import EntityHandler, EntityValidationError
from om_ingest.entities.registry import EntityRegistry


@EntityRegistry.register(EntityType.ML_MODEL)
class MLModelHandler(EntityHandler):
    """Handler for ML Model entities."""

    entity_type = EntityType.ML_MODEL
    om_entity_class = MlModel
    supports_schema_evolution = False  # ML models don't support schema evolution tracking

    def validate(self) -> None:
        """Validate ML model configuration."""
        super().validate()

        # Validate required properties
        self.get_property("service", required=True)

        # Validate ML features if provided
        ml_features = self.get_property("mlFeatures")
        if ml_features:
            if not isinstance(ml_features, list):
                raise EntityValidationError(
                    f"ML Model '{self.name}': 'mlFeatures' must be a list"
                )

            for idx, feature in enumerate(ml_features):
                if not isinstance(feature, dict):
                    raise EntityValidationError(
                        f"ML Model '{self.name}': Feature at index {idx} must be a dictionary"
                    )

                if "name" not in feature:
                    raise EntityValidationError(
                        f"ML Model '{self.name}': Feature at index {idx} missing 'name'"
                    )

        # Validate hyperparameters if provided
        ml_hyper_parameters = self.get_property("mlHyperParameters")
        if ml_hyper_parameters:
            if not isinstance(ml_hyper_parameters, list):
                raise EntityValidationError(
                    f"ML Model '{self.name}': 'mlHyperParameters' must be a list"
                )

            for idx, param in enumerate(ml_hyper_parameters):
                if not isinstance(param, dict):
                    raise EntityValidationError(
                        f"ML Model '{self.name}': Hyperparameter at index {idx} must be a dictionary"
                    )

                if "name" not in param:
                    raise EntityValidationError(
                        f"ML Model '{self.name}': Hyperparameter at index {idx} missing 'name'"
                    )

    def build_entity(self) -> CreateMlModelRequest:
        """Build ML model entity."""
        service_name = self.get_property("service", required=True)

        # Build ML features from properties
        ml_features = self._build_ml_features()
        ml_hyper_parameters = self._build_hyper_parameters()
        ml_store = self._build_ml_store()

        # Get algorithm (default to "mlmodel")
        algorithm = self.get_property_or_default("algorithm", "mlmodel")

        # Build create request
        create_request = CreateMlModelRequest(
            name=self.name,
            description=self.description,
            algorithm=algorithm,
            mlFeatures=ml_features,
            mlHyperParameters=ml_hyper_parameters,
            mlStore=ml_store,
            service=service_name,
            sourceUrl=self.get_property("sourceUrl"),
        )

        return create_request

    def _build_ml_features(self) -> Optional[List[MlFeature]]:
        """
        Build ML features from configuration.

        Returns:
            List of MlFeature objects or None
        """
        features_config = self.get_property("mlFeatures")

        if not features_config:
            return None

        features = []
        for feature_config in features_config:
            feature = self._build_ml_feature(feature_config)
            features.append(feature)

        return features

    def _build_ml_feature(self, feature_config: Dict[str, Any]) -> MlFeature:
        """
        Build a single ML feature from configuration.

        Args:
            feature_config: Feature configuration dictionary

        Returns:
            MlFeature object
        """
        name = feature_config["name"]

        # dataType in MlFeature is the FeatureType enum (numerical/categorical)
        # Default to numerical if not specified
        data_type = self._parse_feature_type(
            feature_config.get("dataType", "numerical")
        )

        # Build feature
        feature = MlFeature(
            name=name,
            dataType=data_type,
            description=feature_config.get("description"),
            featureAlgorithm=feature_config.get("featureAlgorithm"),
            featureSources=feature_config.get("featureSources"),
        )

        return feature

    def _parse_feature_type(self, feature_type_str: str) -> FeatureType:
        """
        Parse feature type string to FeatureType enum.

        Args:
            feature_type_str: Feature type string

        Returns:
            FeatureType enum value
        """
        try:
            # Normalize to lowercase to match enum values
            normalized = feature_type_str.lower()

            # Map common type names to feature types
            type_mapping = {
                "numerical": FeatureType.numerical,
                "categorical": FeatureType.categorical,
                "numeric": FeatureType.numerical,
                "number": FeatureType.numerical,
                "integer": FeatureType.numerical,
                "float": FeatureType.numerical,
                "string": FeatureType.categorical,
                "text": FeatureType.categorical,
                "category": FeatureType.categorical,
            }

            if normalized in type_mapping:
                return type_mapping[normalized]

            # Try direct enum lookup
            return FeatureType[normalized]
        except KeyError:
            # Default to numerical if unknown
            return FeatureType.numerical

    def _build_hyper_parameters(self) -> Optional[List[MlHyperParameter]]:
        """
        Build hyperparameters from configuration.

        Returns:
            List of MlHyperParameter objects or None
        """
        params_config = self.get_property("mlHyperParameters")

        if not params_config:
            return None

        params = []
        for param_config in params_config:
            param = MlHyperParameter(
                name=param_config["name"],
                value=param_config.get("value", ""),
                description=param_config.get("description"),
            )
            params.append(param)

        return params

    def _build_ml_store(self) -> Optional[MlStore]:
        """
        Build ML store from configuration.

        Returns:
            MlStore object or None
        """
        store_config = self.get_property("mlStore")

        if not store_config:
            return None

        ml_store = MlStore(
            storage=store_config.get("storage"),
            imageRepository=store_config.get("imageRepository"),
        )

        return ml_store

    def get_fqn(self) -> str:
        """Get fully qualified name for ML model."""
        service_name = self.get_property("service", required=True)
        return f"{service_name}.{self.name}"

    def get_dependencies(self) -> List[str]:
        """ML model depends on its service."""
        service_name = self.get_property("service", required=True)
        return [service_name]
