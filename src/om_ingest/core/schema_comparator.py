"""Schema comparison and change detection."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class ChangeType(str, Enum):
    """Types of schema changes."""

    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    TYPE_CHANGED = "type_changed"
    NO_CHANGE = "no_change"


@dataclass
class SchemaChange:
    """Represents a schema change."""

    change_type: ChangeType
    field_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None

    def __str__(self) -> str:
        """String representation."""
        if self.change_type == ChangeType.COLUMN_ADDED:
            return f"Added column '{self.field_name}' ({self.new_value})"
        elif self.change_type == ChangeType.COLUMN_REMOVED:
            return f"Removed column '{self.field_name}' ({self.old_value})"
        elif self.change_type == ChangeType.TYPE_CHANGED:
            return f"Changed column '{self.field_name}' type from {self.old_value} to {self.new_value}"
        else:
            return "No change"


@dataclass
class SchemaComparison:
    """Result of schema comparison."""

    has_changes: bool
    changes: List[SchemaChange]
    added_fields: Set[str]
    removed_fields: Set[str]
    type_changes: Dict[str, tuple]

    def is_structural_change(self) -> bool:
        """
        Check if there are structural changes.

        Returns:
            True if there are added/removed columns or type changes
        """
        return self.has_changes

    def summary(self) -> str:
        """
        Get a summary of changes.

        Returns:
            Human-readable summary string
        """
        if not self.has_changes:
            return "No schema changes detected"

        parts = []
        if self.added_fields:
            parts.append(f"{len(self.added_fields)} columns added")
        if self.removed_fields:
            parts.append(f"{len(self.removed_fields)} columns removed")
        if self.type_changes:
            parts.append(f"{len(self.type_changes)} type changes")

        return ", ".join(parts)


class SchemaComparator:
    """
    Compares schemas to detect structural changes.

    Supports comparison of:
    - Tables: columns array
    - Topics: messageSchema.schemaFields
    - SearchIndexes: fields array
    - MLModels: mlFeatures array
    """

    @staticmethod
    def compare_table_schemas(
        old_entity: Any, new_entity: Any
    ) -> SchemaComparison:
        """
        Compare table schemas.

        Args:
            old_entity: Existing table entity from OpenMetadata
            new_entity: New table entity to be created/updated

        Returns:
            SchemaComparison with detected changes
        """
        # Extract columns from both entities
        old_columns = SchemaComparator._extract_table_columns(old_entity)
        new_columns = SchemaComparator._extract_table_columns(new_entity)

        return SchemaComparator._compare_columns(old_columns, new_columns)

    @staticmethod
    def compare_topic_schemas(
        old_entity: Any, new_entity: Any
    ) -> SchemaComparison:
        """
        Compare topic schemas.

        Args:
            old_entity: Existing topic entity from OpenMetadata
            new_entity: New topic entity to be created/updated

        Returns:
            SchemaComparison with detected changes
        """
        # Extract schema fields from both entities
        old_fields = SchemaComparator._extract_topic_fields(old_entity)
        new_fields = SchemaComparator._extract_topic_fields(new_entity)

        return SchemaComparator._compare_columns(old_fields, new_fields)

    @staticmethod
    def compare_search_index_schemas(
        old_entity: Any, new_entity: Any
    ) -> SchemaComparison:
        """
        Compare search index schemas.

        Args:
            old_entity: Existing search index entity from OpenMetadata
            new_entity: New search index entity to be created/updated

        Returns:
            SchemaComparison with detected changes
        """
        # Extract fields from both entities
        old_fields = SchemaComparator._extract_search_index_fields(old_entity)
        new_fields = SchemaComparator._extract_search_index_fields(new_entity)

        return SchemaComparator._compare_columns(old_fields, new_fields)

    @staticmethod
    def _extract_table_columns(entity: Any) -> Dict[str, str]:
        """
        Extract columns from a table entity.

        Args:
            entity: Table entity (either from OM or CreateTableRequest)

        Returns:
            Dict mapping column name to data type
        """
        columns = {}

        if hasattr(entity, "columns") and entity.columns:
            for col in entity.columns:
                # Handle both ColumnName wrapper and string
                col_name = col.name.root if hasattr(col.name, "root") else str(col.name)
                # Handle DataType enum
                col_type = col.dataType.value if hasattr(col.dataType, "value") else str(col.dataType)
                columns[col_name] = col_type

        return columns

    @staticmethod
    def _extract_topic_fields(entity: Any) -> Dict[str, str]:
        """
        Extract schema fields from a topic entity.

        Args:
            entity: Topic entity

        Returns:
            Dict mapping field name to data type
        """
        fields = {}

        if hasattr(entity, "messageSchema") and entity.messageSchema:
            if hasattr(entity.messageSchema, "schemaFields") and entity.messageSchema.schemaFields:
                for field in entity.messageSchema.schemaFields:
                    field_name = field.name.root if hasattr(field.name, "root") else str(field.name)
                    field_type = field.dataType.value if hasattr(field.dataType, "value") else str(field.dataType)
                    fields[field_name] = field_type

        return fields

    @staticmethod
    def _extract_search_index_fields(entity: Any) -> Dict[str, str]:
        """
        Extract fields from a search index entity.

        Args:
            entity: Search index entity

        Returns:
            Dict mapping field name to data type
        """
        fields = {}

        if hasattr(entity, "fields") and entity.fields:
            for field in entity.fields:
                field_name = field.name.root if hasattr(field.name, "root") else str(field.name)
                field_type = field.dataType.value if hasattr(field.dataType, "value") else str(field.dataType)
                fields[field_name] = field_type

        return fields

    @staticmethod
    def _compare_columns(
        old_columns: Dict[str, str], new_columns: Dict[str, str]
    ) -> SchemaComparison:
        """
        Compare column/field dictionaries.

        Args:
            old_columns: Existing columns {name: type}
            new_columns: New columns {name: type}

        Returns:
            SchemaComparison with detected changes
        """
        old_names = set(old_columns.keys())
        new_names = set(new_columns.keys())

        # Detect added and removed columns
        added_fields = new_names - old_names
        removed_fields = old_names - new_names

        # Detect type changes for common columns
        type_changes = {}
        for name in old_names & new_names:
            if old_columns[name] != new_columns[name]:
                type_changes[name] = (old_columns[name], new_columns[name])

        # Build list of changes
        changes = []

        for field in added_fields:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.COLUMN_ADDED,
                    field_name=field,
                    new_value=new_columns[field],
                )
            )

        for field in removed_fields:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.COLUMN_REMOVED,
                    field_name=field,
                    old_value=old_columns[field],
                )
            )

        for field, (old_type, new_type) in type_changes.items():
            changes.append(
                SchemaChange(
                    change_type=ChangeType.TYPE_CHANGED,
                    field_name=field,
                    old_value=old_type,
                    new_value=new_type,
                )
            )

        has_changes = bool(added_fields or removed_fields or type_changes)

        return SchemaComparison(
            has_changes=has_changes,
            changes=changes,
            added_fields=added_fields,
            removed_fields=removed_fields,
            type_changes=type_changes,
        )
