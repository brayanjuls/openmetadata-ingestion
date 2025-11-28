"""Dependency resolution using topological sort (Kahn's algorithm)."""

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

from om_ingest.config.schema import EntityConfig, EntityType


class CircularDependencyError(Exception):
    """Raised when circular dependencies are detected."""

    pass


class DependencyResolver:
    """Resolves entity dependencies and determines execution order."""

    # Entity dependency rules: child -> parent
    # Each entity type lists what it depends on
    DEPENDENCY_RULES: Dict[EntityType, List[EntityType]] = {
        # Database entities
        EntityType.DATABASE_SERVICE: [],  # No dependencies
        EntityType.DATABASE: [EntityType.DATABASE_SERVICE],
        EntityType.DATABASE_SCHEMA: [EntityType.DATABASE],
        EntityType.TABLE: [EntityType.DATABASE_SCHEMA],
        # Pipeline entities
        EntityType.PIPELINE_SERVICE: [],
        EntityType.PIPELINE: [EntityType.PIPELINE_SERVICE],
        EntityType.TASK: [EntityType.PIPELINE],
        # Messaging entities
        EntityType.MESSAGING_SERVICE: [],
        EntityType.TOPIC: [EntityType.MESSAGING_SERVICE],
        # ML entities
        EntityType.ML_MODEL_SERVICE: [],
        EntityType.ML_MODEL: [EntityType.ML_MODEL_SERVICE],
        # Search entities
        EntityType.SEARCH_SERVICE: [],
        EntityType.SEARCH_INDEX: [EntityType.SEARCH_SERVICE],
        # Governance entities (most have no dependencies within themselves)
        EntityType.TAG_CATEGORY: [],
        EntityType.TAG: [EntityType.TAG_CATEGORY],
        EntityType.USER: [],
        EntityType.TEAM: [],
        EntityType.GLOSSARY: [],
        EntityType.GLOSSARY_TERM: [EntityType.GLOSSARY],
    }

    def __init__(self, entities: List[EntityConfig]):
        """
        Initialize dependency resolver.

        Args:
            entities: List of entity configurations to resolve
        """
        self.entities = entities
        self._entity_map: Dict[str, EntityConfig] = {}
        self._build_entity_map()

    def _build_entity_map(self) -> None:
        """Build a map of entity identifiers to entity configs."""
        for entity in self.entities:
            # Use entity name or FQN as identifier
            identifier = self._get_entity_identifier(entity)
            self._entity_map[identifier] = entity

    def _get_entity_identifier(self, entity: EntityConfig) -> str:
        """
        Get a unique identifier for an entity.

        Args:
            entity: Entity configuration

        Returns:
            Unique identifier string
        """
        if entity.fqn:
            return entity.fqn
        elif entity.name:
            return f"{entity.type.value}:{entity.name}"
        else:
            # For discovery-based entities, use type + discovery source
            return f"{entity.type.value}:discovery:{entity.discovery.source}"

    def _get_entity_dependencies(self, entity: EntityConfig) -> List[EntityType]:
        """
        Get the dependency types for an entity based on its type.

        Args:
            entity: Entity configuration

        Returns:
            List of entity types this entity depends on
        """
        return self.DEPENDENCY_RULES.get(entity.type, [])

    def _extract_parent_from_properties(
        self, entity: EntityConfig, parent_type: EntityType
    ) -> Optional[str]:
        """
        Extract parent entity reference from entity properties.

        Different entity types store parent references differently:
        - database: references database_service via 'service' property
        - database_schema: references database via 'database' property
        - table: references database_schema via 'databaseSchema' property

        Args:
            entity: Entity configuration
            parent_type: Type of parent to extract

        Returns:
            Parent entity identifier or None
        """
        props = entity.properties

        # Map parent types to property keys
        parent_property_map = {
            EntityType.DATABASE_SERVICE: "service",
            EntityType.DATABASE: "database",
            EntityType.DATABASE_SCHEMA: "database_schema",  # Use snake_case to match YAML
            EntityType.PIPELINE_SERVICE: "service",
            EntityType.PIPELINE: "pipeline",
            EntityType.MESSAGING_SERVICE: "service",
            EntityType.ML_MODEL_SERVICE: "service",
            EntityType.SEARCH_SERVICE: "service",
            EntityType.TAG_CATEGORY: "category",
            EntityType.GLOSSARY: "glossary",
        }

        property_key = parent_property_map.get(parent_type)
        if not property_key:
            return None

        parent_ref = props.get(property_key)
        if not parent_ref:
            return None

        # Return the parent identifier
        # This could be a direct name or an FQN
        return str(parent_ref)

    def resolve(self) -> List[EntityConfig]:
        """
        Resolve dependencies and return entities in topological order.

        Uses Kahn's algorithm for topological sorting.

        Returns:
            List of entities in dependency order (parents before children)

        Raises:
            CircularDependencyError: If circular dependencies detected
        """
        # Build adjacency list (child -> parents) and in-degree map
        graph: Dict[str, Set[str]] = defaultdict(set)
        in_degree: Dict[str, int] = defaultdict(int)

        # Initialize all entities with 0 in-degree
        for entity in self.entities:
            identifier = self._get_entity_identifier(entity)
            if identifier not in in_degree:
                in_degree[identifier] = 0

        # Build dependency graph
        for entity in self.entities:
            identifier = self._get_entity_identifier(entity)
            dependencies = self._get_entity_dependencies(entity)

            for dep_type in dependencies:
                # Find parent entity of this type
                parent_ref = self._extract_parent_from_properties(entity, dep_type)

                if parent_ref:
                    # Find the actual parent entity config
                    parent_identifier = self._find_parent_identifier(
                        parent_ref, dep_type
                    )

                    if parent_identifier:
                        # Add edge: parent -> child
                        graph[parent_identifier].add(identifier)
                        in_degree[identifier] += 1

        # Kahn's algorithm
        queue = deque()
        result = []

        # Start with entities that have no dependencies
        for identifier, degree in in_degree.items():
            if degree == 0:
                queue.append(identifier)

        while queue:
            current = queue.popleft()
            result.append(current)

            # Process all entities that depend on current
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Check for circular dependencies
        if len(result) != len(self.entities):
            remaining = set(in_degree.keys()) - set(result)
            raise CircularDependencyError(
                f"Circular dependency detected among entities: {remaining}"
            )

        # Convert identifiers back to EntityConfig objects
        ordered_entities = []
        for identifier in result:
            entity = self._entity_map[identifier]
            ordered_entities.append(entity)

        return ordered_entities

    def _find_parent_identifier(
        self, parent_ref: str, parent_type: EntityType
    ) -> Optional[str]:
        """
        Find the identifier of a parent entity by reference and type.

        Args:
            parent_ref: Parent reference (name or FQN)
            parent_type: Expected parent type

        Returns:
            Parent identifier or None
        """
        # Try exact match by identifier
        if parent_ref in self._entity_map:
            return parent_ref

        # Try with type prefix
        typed_ref = f"{parent_type.value}:{parent_ref}"
        if typed_ref in self._entity_map:
            return typed_ref

        # Search through entities
        for identifier, entity in self._entity_map.items():
            if entity.type == parent_type:
                if entity.name == parent_ref or entity.fqn == parent_ref:
                    return identifier

        return None

    def validate_dependencies(self) -> List[str]:
        """
        Validate that all dependencies can be resolved.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        for entity in self.entities:
            dependencies = self._get_entity_dependencies(entity)

            for dep_type in dependencies:
                parent_ref = self._extract_parent_from_properties(entity, dep_type)

                if not parent_ref:
                    errors.append(
                        f"Entity {self._get_entity_identifier(entity)} "
                        f"is missing required parent of type {dep_type.value}"
                    )
                    continue

                parent_identifier = self._find_parent_identifier(parent_ref, dep_type)

                if not parent_identifier:
                    errors.append(
                        f"Entity {self._get_entity_identifier(entity)} "
                        f"references unknown parent '{parent_ref}' of type {dep_type.value}"
                    )

        return errors


from typing import Optional  # Add this import at the top
