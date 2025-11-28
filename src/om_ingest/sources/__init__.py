"""Data source connectors and registry.

This module must be imported to register all source connectors.
The connectors use the @SourceRegistry.register() decorator which
executes at import time to register the connectors.
"""

# Import base classes and registry first
from om_ingest.sources.base import DataSource, DataSourceError
from om_ingest.sources.registry import SourceRegistry

# Import all connectors to trigger their @SourceRegistry.register() decorators
# This is CRITICAL - without these imports, the decorators never execute
from om_ingest.sources.s3_hudi import S3HudiConnector

__all__ = [
    "DataSource",
    "DataSourceError",
    "SourceRegistry",
    "S3HudiConnector",
]
