"""Configuration loader for YAML files with validation."""

import os
from pathlib import Path
from typing import Dict, Any

import yaml
from pydantic import ValidationError

from om_ingest.config.schema import IngestionConfig


class ConfigLoadError(Exception):
    """Raised when configuration loading fails."""

    pass


class ConfigLoader:
    """Loads and validates YAML configuration files."""

    @staticmethod
    def load_yaml(file_path: str | Path) -> Dict[str, Any]:
        """
        Load YAML file and return raw dictionary.

        Args:
            file_path: Path to YAML configuration file

        Returns:
            Raw configuration dictionary

        Raises:
            ConfigLoadError: If file cannot be read or YAML is invalid
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise ConfigLoadError(f"Configuration file not found: {file_path}")

        if not file_path.is_file():
            raise ConfigLoadError(f"Path is not a file: {file_path}")

        try:
            with open(file_path, "r") as f:
                config_dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigLoadError(f"Invalid YAML syntax in {file_path}: {e}")
        except Exception as e:
            raise ConfigLoadError(f"Failed to read {file_path}: {e}")

        if not isinstance(config_dict, dict):
            raise ConfigLoadError(
                f"Configuration must be a YAML object/dict, got {type(config_dict)}"
            )

        return config_dict

    @staticmethod
    def substitute_env_vars(value: Any) -> Any:
        """
        Recursively substitute environment variables in configuration values.

        Supports ${VAR_NAME} syntax.

        Args:
            value: Configuration value (can be dict, list, str, etc.)

        Returns:
            Value with environment variables substituted
        """
        if isinstance(value, str):
            # Handle ${VAR_NAME} syntax
            if value.startswith("${") and value.endswith("}"):
                var_name = value[2:-1]
                env_value = os.getenv(var_name)
                if env_value is None:
                    raise ConfigLoadError(
                        f"Environment variable not found: {var_name}"
                    )
                return env_value
            return value
        elif isinstance(value, dict):
            return {k: ConfigLoader.substitute_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [ConfigLoader.substitute_env_vars(item) for item in value]
        else:
            return value

    @staticmethod
    def validate_config(config_dict: Dict[str, Any]) -> IngestionConfig:
        """
        Validate configuration dictionary against Pydantic schema.

        Args:
            config_dict: Raw configuration dictionary

        Returns:
            Validated IngestionConfig object

        Raises:
            ConfigLoadError: If validation fails
        """
        try:
            config = IngestionConfig(**config_dict)
            return config
        except ValidationError as e:
            # Format validation errors in a user-friendly way
            error_messages = []
            for error in e.errors():
                loc = " -> ".join(str(l) for l in error["loc"])
                msg = error["msg"]
                error_messages.append(f"  - {loc}: {msg}")

            raise ConfigLoadError(
                "Configuration validation failed:\n" + "\n".join(error_messages)
            )
        except Exception as e:
            raise ConfigLoadError(f"Unexpected validation error: {e}")

    @classmethod
    def load(cls, file_path: str | Path) -> IngestionConfig:
        """
        Load and validate configuration from YAML file.

        This is the main entry point for loading configurations.

        Args:
            file_path: Path to YAML configuration file

        Returns:
            Validated IngestionConfig object

        Raises:
            ConfigLoadError: If loading or validation fails

        Example:
            >>> config = ConfigLoader.load("config.yaml")
            >>> print(config.metadata.name)
        """
        # Load YAML
        config_dict = cls.load_yaml(file_path)

        # Environment variable substitution is now handled by Pydantic validators
        # in the schema models, but we can do an initial pass here for non-sensitive fields
        # config_dict = cls.substitute_env_vars(config_dict)

        # Validate against schema
        config = cls.validate_config(config_dict)

        return config

    @classmethod
    def load_with_templates(cls, file_path: str | Path) -> IngestionConfig:
        """
        Load configuration with template inheritance support.

        This method will be implemented in conjunction with the template engine.
        For now, it delegates to the standard load method.

        Args:
            file_path: Path to YAML configuration file

        Returns:
            Validated IngestionConfig object

        Raises:
            ConfigLoadError: If loading or validation fails
        """
        # TODO: Integrate with TemplateEngine once implemented
        from om_ingest.config.template_engine import TemplateEngine

        # Load the config with template processing
        config_dict = TemplateEngine.process_file(file_path)

        # Validate
        config = cls.validate_config(config_dict)

        return config
