"""Entity handlers and registry.

This module must be imported to register all entity handlers.
The handlers use the @EntityRegistry.register() decorator which
executes at import time to register the handlers.
"""

# Import registry first
from om_ingest.entities.registry import EntityRegistry

# Import all handlers to trigger their @EntityRegistry.register() decorators
# This is CRITICAL - without these imports, the decorators never execute
from om_ingest.entities.database import (
    DatabaseHandler,
    DatabaseSchemaHandler,
    DatabaseServiceHandler,
    TableHandler,
)
from om_ingest.entities.ml import (
    MLModelHandler,
    MLModelServiceHandler,
)

__all__ = [
    "EntityRegistry",
    "DatabaseServiceHandler",
    "DatabaseHandler",
    "DatabaseSchemaHandler",
    "TableHandler",
    "MLModelServiceHandler",
    "MLModelHandler",
]
