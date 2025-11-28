"""Table entity handler."""

from typing import Any, Dict, List, Optional

from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.table import (
    Column,
    ColumnName,
    DataType,
    Table,
    TableType,
)
from pydantic import BaseModel

from om_ingest.config.schema import EntityType
from om_ingest.entities.base import EntityHandler, EntityValidationError
from om_ingest.entities.registry import EntityRegistry


@EntityRegistry.register(EntityType.TABLE)
class TableHandler(EntityHandler):
    """Handler for Table entities."""

    entity_type = EntityType.TABLE
    om_entity_class = Table
    supports_schema_evolution = True  # Tables support schema evolution

    def validate(self) -> None:
        """Validate table configuration."""
        super().validate()

        # Validate required properties
        self.get_property("database", required=True)
        self.get_property("database_schema", required=True)
        self.get_property("service", required=True)

        # Validate columns if provided
        columns = self.get_property("columns")
        if columns:
            if not isinstance(columns, list):
                raise EntityValidationError(
                    f"Table '{self.name}': 'columns' must be a list"
                )

            for idx, col in enumerate(columns):
                if not isinstance(col, dict):
                    raise EntityValidationError(
                        f"Table '{self.name}': Column at index {idx} must be a dictionary"
                    )

                if "name" not in col:
                    raise EntityValidationError(
                        f"Table '{self.name}': Column at index {idx} missing 'name'"
                    )

                if "dataType" not in col:
                    raise EntityValidationError(
                        f"Table '{self.name}': Column '{col['name']}' missing 'dataType'"
                    )

    def build_entity(self) -> CreateTableRequest:
        """Build table entity."""
        database_name = self.get_property("database", required=True)
        schema_name = self.get_property("database_schema", required=True)
        service_name = self.get_property("service", required=True)

        # Construct database schema FQN
        database_schema_fqn = f"{service_name}.{database_name}.{schema_name}"

        # Build columns
        columns = self._build_columns()

        # Get table type (default to Regular)
        table_type_str = self.get_property_or_default("table_type", "Regular")
        table_type = self._parse_table_type(table_type_str)

        # Build create request
        create_request = CreateTableRequest(
            name=self.name,
            description=self.description,
            databaseSchema=database_schema_fqn,
            columns=columns,
            tableType=table_type,
        )

        return create_request

    def _build_columns(self) -> List[Column]:
        """
        Build column definitions from configuration.

        Returns:
            List of Column objects
        """
        columns_config = self.get_property("columns")

        if not columns_config:
            # If no columns specified, return empty list
            # This might happen during discovery phase
            return []

        columns = []
        for col_config in columns_config:
            column = self._build_column(col_config)
            columns.append(column)

        return columns

    def _build_column(self, col_config: Dict[str, Any]) -> Column:
        """
        Build a single column from configuration.

        Args:
            col_config: Column configuration dictionary

        Returns:
            Column object
        """
        name = col_config["name"]
        data_type_str = col_config["dataType"]

        # Parse data type
        data_type = self._parse_data_type(data_type_str)

        # Build column
        column = Column(
            name=ColumnName(name),
            dataType=data_type,
            description=col_config.get("description"),
            dataLength=col_config.get("dataLength"),
            precision=col_config.get("precision"),
            scale=col_config.get("scale"),
            constraint=col_config.get("constraint"),
        )

        return column

    def _parse_data_type(self, data_type_str: str) -> DataType:
        """
        Parse data type string to DataType enum.

        Args:
            data_type_str: Data type string (e.g., "VARCHAR", "INT", "BIGINT")

        Returns:
            DataType enum value

        Raises:
            EntityValidationError: If data type is invalid
        """
        # Normalize to uppercase
        normalized = data_type_str.upper()

        # Try to match to DataType enum
        try:
            # Handle common variations
            type_mapping = {
                "VARCHAR": DataType.VARCHAR,
                "STRING": DataType.STRING,
                "TEXT": DataType.STRING,
                "CHAR": DataType.CHAR,
                "INT": DataType.INT,
                "INTEGER": DataType.INT,
                "BIGINT": DataType.BIGINT,
                "SMALLINT": DataType.SMALLINT,
                "TINYINT": DataType.TINYINT,
                "FLOAT": DataType.FLOAT,
                "DOUBLE": DataType.DOUBLE,
                "DECIMAL": DataType.DECIMAL,
                "NUMERIC": DataType.NUMERIC,
                "BOOLEAN": DataType.BOOLEAN,
                "BOOL": DataType.BOOLEAN,
                "TIMESTAMP": DataType.TIMESTAMP,
                "DATE": DataType.DATE,
                "TIME": DataType.TIME,
                "DATETIME": DataType.DATETIME,
                "BINARY": DataType.BINARY,
                "VARBINARY": DataType.VARBINARY,
                "ARRAY": DataType.ARRAY,
                "STRUCT": DataType.STRUCT,
                "MAP": DataType.MAP,
                "JSON": DataType.JSON,
            }

            if normalized in type_mapping:
                return type_mapping[normalized]

            # Try direct enum lookup
            return DataType[normalized]

        except (KeyError, ValueError):
            raise EntityValidationError(
                f"Table '{self.name}': Invalid data type '{data_type_str}'. "
                f"Must be a valid OpenMetadata DataType."
            )

    def _parse_table_type(self, table_type_str: str) -> TableType:
        """
        Parse table type string to TableType enum.

        Args:
            table_type_str: Table type string

        Returns:
            TableType enum value
        """
        try:
            return TableType[table_type_str]
        except KeyError:
            # Default to Regular if unknown
            return TableType.Regular

    def get_fqn(self) -> str:
        """Get fully qualified name for table."""
        database_name = self.get_property("database", required=True)
        schema_name = self.get_property("database_schema", required=True)
        service_name = self.get_property("service", required=True)
        return f"{service_name}.{database_name}.{schema_name}.{self.name}"

    def get_dependencies(self) -> List[str]:
        """Table depends on its database schema."""
        database_name = self.get_property("database", required=True)
        schema_name = self.get_property("database_schema", required=True)
        service_name = self.get_property("service", required=True)
        database_schema_fqn = f"{service_name}.{database_name}.{schema_name}"
        return [database_schema_fqn]
