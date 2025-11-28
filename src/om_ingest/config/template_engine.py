"""Template engine for configuration inheritance and merging."""

import copy
from pathlib import Path
from typing import Any, Dict, List

import yaml

from om_ingest.config.loader import ConfigLoadError


class TemplateEngine:
    """Handles template inheritance and configuration merging."""

    @staticmethod
    def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries, with override taking precedence.

        For dictionaries: recursively merge
        For lists: override replaces base
        For other types: override replaces base

        Args:
            base: Base dictionary
            override: Override dictionary

        Returns:
            Merged dictionary
        """
        result = copy.deepcopy(base)

        for key, value in override.items():
            if key in result:
                # If both are dicts, merge recursively
                if isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = TemplateEngine.deep_merge(result[key], value)
                else:
                    # Otherwise, override replaces base
                    result[key] = copy.deepcopy(value)
            else:
                # Key doesn't exist in base, add it
                result[key] = copy.deepcopy(value)

        return result

    @staticmethod
    def resolve_references(
        config: Dict[str, Any], context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Resolve !ref references in configuration.

        Supports syntax like: !ref connections.postgres.host

        Args:
            config: Configuration dictionary
            context: Context dictionary containing referenceable values

        Returns:
            Configuration with references resolved
        """
        if isinstance(config, dict):
            result = {}
            for key, value in config.items():
                # Check if value is a reference string
                if isinstance(value, str) and value.startswith("!ref "):
                    ref_path = value[5:].strip()  # Remove "!ref "
                    resolved = TemplateEngine._resolve_ref_path(ref_path, context)
                    result[key] = resolved
                else:
                    result[key] = TemplateEngine.resolve_references(value, context)
            return result
        elif isinstance(config, list):
            return [TemplateEngine.resolve_references(item, context) for item in config]
        else:
            return config

    @staticmethod
    def _resolve_ref_path(path: str, context: Dict[str, Any]) -> Any:
        """
        Resolve a dot-separated reference path.

        Args:
            path: Dot-separated path (e.g., "connections.postgres.host")
            context: Context dictionary

        Returns:
            Resolved value

        Raises:
            ConfigLoadError: If reference cannot be resolved
        """
        parts = path.split(".")
        current = context

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise ConfigLoadError(f"Cannot resolve reference: !ref {path}")

        return current

    @staticmethod
    def load_template(template_path: str | Path) -> Dict[str, Any]:
        """
        Load a template file.

        Args:
            template_path: Path to template YAML file

        Returns:
            Template dictionary

        Raises:
            ConfigLoadError: If template cannot be loaded
        """
        template_path = Path(template_path)

        if not template_path.exists():
            raise ConfigLoadError(f"Template file not found: {template_path}")

        try:
            with open(template_path, "r") as f:
                template = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigLoadError(f"Invalid YAML in template {template_path}: {e}")
        except Exception as e:
            raise ConfigLoadError(f"Failed to read template {template_path}: {e}")

        if not isinstance(template, dict):
            raise ConfigLoadError(
                f"Template must be a YAML object/dict: {template_path}"
            )

        return template

    @staticmethod
    def process_file(file_path: str | Path) -> Dict[str, Any]:
        """
        Process a configuration file with template inheritance.

        Supports:
        1. Template inheritance via 'extends' key
        2. Reference resolution via !ref syntax
        3. Deep merging of configurations

        Args:
            file_path: Path to configuration file

        Returns:
            Processed configuration dictionary

        Raises:
            ConfigLoadError: If processing fails

        Example YAML:
            ```yaml
            extends: "./templates/base.yaml"

            openmetadata:
              host: "http://localhost:8585"

            entities:
              - type: database
                name: !ref connections.postgres.database
            ```
        """
        file_path = Path(file_path)

        # Load the main config file
        try:
            with open(file_path, "r") as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigLoadError(f"Invalid YAML in {file_path}: {e}")
        except Exception as e:
            raise ConfigLoadError(f"Failed to read {file_path}: {e}")

        if not isinstance(config, dict):
            raise ConfigLoadError(f"Configuration must be a YAML object: {file_path}")

        # Check if it extends a template
        if "extends" in config:
            extends_path = config.pop("extends")

            # Resolve relative paths relative to the config file's directory
            if not Path(extends_path).is_absolute():
                extends_path = file_path.parent / extends_path

            # Load the template
            template = TemplateEngine.load_template(extends_path)

            # Recursively process the template (it might also extend something)
            template = TemplateEngine.process_file(extends_path)

            # Deep merge: template as base, config as override
            config = TemplateEngine.deep_merge(template, config)

        # Resolve references
        config = TemplateEngine.resolve_references(config, config)

        return config

    @staticmethod
    def apply_defaults(
        config: Dict[str, Any], defaults: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply default values to configuration.

        Defaults are applied where values are not already specified.

        Args:
            config: Configuration dictionary
            defaults: Default values dictionary

        Returns:
            Configuration with defaults applied
        """
        # Create a copy to avoid modifying the original
        result = copy.deepcopy(defaults)

        # Merge config on top of defaults
        result = TemplateEngine.deep_merge(result, config)

        return result

    @staticmethod
    def merge_configs(configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge multiple configurations in order.

        Later configs override earlier ones.

        Args:
            configs: List of configuration dictionaries

        Returns:
            Merged configuration
        """
        if not configs:
            return {}

        result = configs[0]
        for config in configs[1:]:
            result = TemplateEngine.deep_merge(result, config)

        return result
