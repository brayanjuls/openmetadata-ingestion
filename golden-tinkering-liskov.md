# OpenMetadata YAML-Driven Ingestion Library - Implementation Plan

## Overview

Build a Python library for flexible, YAML-driven OpenMetadata entity ingestion supporting initial bulk loading, incremental updates, schema evolution detection, profiling, and audit logging.

## Core Design Principles

1. **Fail-Fast Dependencies**: Validate parent entities exist before child creation
2. **Optimistic Processing**: Continue on individual entity failures, stop on dependency failures
3. **Idempotency**: Configurable per entity (skip/update/fail), default = skip
4. **Sequential Processing**: One-by-one, no batching
5. **Structural Change Detection**: Only update when columns added/removed/type changed
6. **Comprehensive Audit Trail**: JSON logs of all operations
7. **YAML-Driven**: Template inheritance for connections and defaults
8. **Dry-Run Support**: Preview operations with diffs before execution

## Project Structure

```
openmetadata-ingestion-lib/
├── pyproject.toml                    # Poetry/setuptools config
├── src/
│   └── om_ingest/
│       ├── cli/                      # Click-based CLI
│       │   └── main.py
│       ├── core/                     # Core business logic
│       │   ├── engine.py            # Main orchestration
│       │   ├── executor.py          # Entity execution
│       │   ├── dependency_resolver.py # Topological sorting
│       │   ├── schema_comparator.py  # Schema change detection
│       │   └── context.py           # Execution context
│       ├── config/                   # Configuration management
│       │   ├── loader.py            # YAML loading
│       │   ├── schema.py            # Pydantic models
│       │   ├── template_engine.py   # Template inheritance
│       │   └── validator.py         # Validation
│       ├── entities/                 # Entity type handlers
│       │   ├── base.py              # Abstract base
│       │   ├── registry.py          # Entity registry
│       │   ├── database/            # Database entities
│       │   ├── pipeline/            # Pipeline entities
│       │   ├── ml/                  # ML models
│       │   ├── messaging/           # Kafka topics
│       │   ├── search/              # OpenSearch indexes
│       │   └── governance/          # Tags, users, contracts
│       ├── sources/                  # Data source connectors
│       │   ├── base.py              # Abstract interface
│       │   ├── registry.py          # Plugin registry
│       │   ├── s3_hudi/             # Hudi connector
│       │   ├── opensearch/          # OpenSearch connector
│       │   ├── airflow/             # Airflow connector
│       │   ├── postgres/            # Postgres connector
│       │   ├── mlflow/              # MLflow connector
│       │   └── kafka/               # Kafka connector
│       ├── profiling/                # Data & schema profiling
│       │   ├── data_profiler.py     # Statistics collection
│       │   ├── schema_profiler.py   # Schema tracking
│       │   └── storage.py           # Profile storage
│       ├── audit/                    # Audit logging
│       │   ├── logger.py            # Event logger
│       │   ├── models.py            # Event models
│       │   └── storage.py           # JSON storage
│       ├── strategies/               # Processing strategies
│       │   ├── idempotency.py       # Skip/update/fail
│       │   └── error_handling.py    # Error strategies
│       └── utils/                    # Utilities
│           ├── logging.py           # Logging setup
│           └── formatting.py        # Dry-run formatting
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── examples/
    ├── simple_database.yaml
    ├── complete_stack.yaml
    └── templates/
```

## Implementation Strategy

### MVP Scope (Initial Focus)
The initial implementation will focus on **Phases 1-4** to deliver a working MVP that can:
- Ingest database entities (Database, Schema, Table) from S3 Hudi tables
- Resolve dependencies and execute in correct order
- Detect schema changes and apply idempotency rules
- Profile data and schema
- Generate audit logs
- Support dry-run mode

**Critical Components for MVP**:
1. ✅ Core engine + dependency resolution
2. ✅ Database entities (Database, Schema, Table, Column)
3. ✅ S3 Hudi connector (highest priority source)
4. ✅ Profiling system (data + schema)
5. ✅ Basic CLI for ingestion
6. ✅ Minimal smoke tests (not comprehensive coverage initially)

**Deferred to Post-MVP** (Phases 5-10):
- Additional source connectors (Postgres, OpenSearch, Airflow, MLflow, Kafka)
- Additional entity types (Pipeline, Topic, MLModel, SearchIndex, Tags, Users)
- Comprehensive test suite (will add iteratively)
- Advanced CLI commands (discover, list-sources, etc.)
- Rich documentation

### Testing Approach
- **Minimal smoke tests** for MVP to move quickly
- Test against **live OpenMetadata instance** (already running)
- Focus on critical paths: dependency resolution, entity creation, schema detection
- Expand test coverage iteratively post-MVP

---

## Implementation Phases (MVP: Phases 1-4)

### Phase 1: Foundation & Configuration (Core Infrastructure)

**Goal**: Establish configuration system and basic structure

#### 1.1 Project Setup
- Initialize Python project with Poetry/setuptools
- Configure `pyproject.toml` with dependencies:
  - `openmetadata-ingestion` (OpenMetadata Python SDK)
  - `pydantic>=2.0` (config validation)
  - `click>=8.0` (CLI)
  - `pyyaml` (YAML parsing)
  - `rich` (CLI formatting)
  - `boto3` (S3 connector)
  - `opensearch-py` (OpenSearch connector)
  - `apache-airflow-client` (Airflow API)
  - `mlflow` (MLflow connector)
  - `kafka-python` (Kafka connector)
  - `sqlalchemy` (database connectors)
  - `pandas` (data profiling)
  - `pytest` (testing)
- Set up directory structure
- Configure linting (ruff/black) and type checking (mypy)

#### 1.2 Configuration Schema (`config/schema.py`)
- Define Pydantic models for YAML config structure:
  - `IngestionConfig` (root model)
  - `OpenMetadataConfig` (connection config)
  - `SourceConfig` (data source config)
  - `EntityConfig` (entity definition)
  - `DefaultsConfig`, `ProfilingConfig`, `AuditConfig`, `ExecutionConfig`
  - Enum types: `IdempotencyMode`, `AuthType`, etc.
- Add validators for required fields and relationships
- Support environment variable substitution (`${VAR}`)

#### 1.3 Configuration Loader (`config/loader.py`)
- Implement YAML loading with error handling
- Environment variable substitution
- Schema validation using Pydantic
- Clear error messages for config issues

#### 1.4 Template Engine (`config/template_engine.py`)
- Load template files
- Deep merge algorithm for config inheritance
- Reference resolution (`!ref` syntax)
- Support for connection and entity-level defaults

**Deliverables**:
- Working config loading and validation
- Example YAML configurations
- Unit tests for config system

---

### Phase 2: Core Orchestration Engine

**Goal**: Build the main ingestion engine and dependency resolution

#### 2.1 Dependency Resolver (`core/dependency_resolver.py`)
- Implement entity dependency graph construction
- Entity-specific dependency rules:
  - `database_schema` depends on `database`
  - `table` depends on `database_schema`
  - `task` depends on `pipeline`
  - Service entities have no dependencies
- Topological sort using Kahn's algorithm
- Circular dependency detection
- Clear error messages for missing dependencies

#### 2.2 Execution Context (`core/context.py`)
- Manage execution state:
  - OpenMetadata client instance
  - Processed entity cache (for dependency validation)
  - Configuration reference
  - Dry-run mode flag
- Provide entity lookup methods
- Track execution statistics

#### 2.3 OpenMetadata Client Wrapper (`core/client.py`)
- Wrapper around OpenMetadata SDK
- Connection management and authentication
- Entity creation/update operations
- Entity lookup by FQN
- Error handling and retries
- Support for dry-run mode (skip API calls)

#### 2.4 Ingestion Engine (`core/engine.py`)
- Main orchestration class:
  1. Load and validate config
  2. Initialize OpenMetadata client
  3. Resolve entity dependencies
  4. Execute entities in topological order
  5. Coordinate audit logging
  6. Return execution summary
- Handle dry-run mode
- Error handling coordination

**Deliverables**:
- Working dependency resolution with tests
- Functional ingestion engine skeleton
- Integration with OpenMetadata SDK

---

### Phase 3: Entity System & Registry

**Goal**: Implement entity handlers and registration

#### 3.1 Base Entity Interface (`entities/base.py`)
- Abstract base class `EntityHandler`:
  - `build_entity()`: Convert config to OpenMetadata entity
  - `get_dependencies()`: Extract parent dependencies
  - `supports_schema_evolution`: Flag for schema tracking
  - `om_entity_class`: Reference to OM SDK class
  - `validate()`: Config validation
- Standard entity lifecycle methods

#### 3.2 Entity Registry (`entities/registry.py`)
- Registry pattern for entity type registration
- Decorator-based registration: `@EntityRegistry.register('table')`
- Entity type lookup
- List available entity types

#### 3.3 Database Entity Handlers (MVP Focus)
Implement handlers for database entities only:

**Database Entities** (`entities/database/`):
- `DatabaseServiceHandler` - Database service configuration
  - Map to OpenMetadata `DatabaseService` entity
  - Handle service connection config
  - Support Datalake service type (for S3 Hudi)

- `DatabaseHandler` - Database metadata
  - Map to OpenMetadata `Database` entity
  - Dependency: requires `DatabaseService`
  - Extract database-level properties

- `DatabaseSchemaHandler` - Schema metadata
  - Map to OpenMetadata `DatabaseSchema` entity
  - Dependency: requires `Database`
  - Handle schema-level metadata

- `TableHandler` - Table with column definitions
  - Map to OpenMetadata `Table` entity
  - Dependency: requires `Database` and `DatabaseSchema`
  - **Support schema evolution detection** (critical for MVP)
  - Column-level metadata with data types
  - Table properties (table type, storage location for Hudi)

**Deliverables**:
- Database entity handler system working end-to-end
- All 4 database entity types functional
- Registry pattern implemented
- Schema evolution support for tables

**Note**: Other entity types (Pipeline, ML, Messaging, Search, Governance) are deferred to post-MVP phases.

---

### Phase 4: Entity Executor & Processing Logic

**Goal**: Implement entity creation/update logic with idempotency

#### 4.1 Schema Comparator (`core/schema_comparator.py`)
- Structural schema comparison algorithm:
  - Extract schema from entities (columns/fields)
  - Detect added columns
  - Detect removed columns
  - Detect type changes
- Support multiple entity types:
  - Tables: `columns` array
  - Topics: `messageSchema.schemaFields`
  - SearchIndexes: `fields` array
  - MLModels: `mlFeatures` array
- Return structured change information

#### 4.2 Idempotency Strategies (`strategies/idempotency.py`)
- Strategy pattern for idempotency modes:
  - `SkipStrategy`: Skip if exists (default)
  - `UpdateStrategy`: Update if exists
  - `FailStrategy`: Fail if exists
- `IdempotencyAction` result type
- Strategy selection based on config

#### 4.3 Error Handling Strategies (`strategies/error_handling.py`)
- `DependencyValidationError`: Always fail-fast
- `EntityProcessingError`: Optimistic handling
- Retry logic with exponential backoff
- Error categorization and logging

#### 4.4 Entity Executor (`core/executor.py`)
- Main execution logic for individual entities:
  1. Validate parent dependencies (fail-fast)
  2. Get entity handler from registry
  3. Build OpenMetadata entity object
  4. Check if entity exists
  5. Apply idempotency strategy
  6. Detect schema changes (if applicable)
  7. Handle dry-run mode
  8. Execute operation (create/update/skip)
  9. Run profiling if enabled
  10. Log audit event
  11. Register in context
- Error handling with fail-fast vs optimistic
- Integration with all strategies

**Deliverables**:
- Complete entity execution pipeline
- Schema change detection working
- Idempotency modes functional
- Comprehensive error handling

---

### Phase 5: S3 Hudi Connector (MVP Priority)

**Goal**: Implement S3 Hudi source connector (highest priority)

#### 5.1 Source Base Interface (`sources/base.py`)
- Abstract `DataSource` class:
  - `connect()`: Establish connection
  - `discover_entities()`: Yield discovered entities
  - `extract_schema()`: Get schema metadata
  - `fetch_sample_data()`: Get data for profiling
  - `validate_connection()`: Test connectivity
  - `disconnect()`: Cleanup
- Source metadata: `source_type`, `supported_entities`

#### 5.2 Source Registry (`sources/registry.py`)
- Registry pattern for source plugins
- Decorator registration: `@SourceRegistry.register('s3_hudi')`
- Source lookup and instantiation

#### 5.3 S3 Hudi Connector (MVP Focus)

**S3 Hudi Connector** (`sources/s3_hudi/`):
- **Connection**:
  - Connect to S3 using boto3
  - Support AWS credentials (access key/secret or IAM role)
  - Configure S3 client with region

- **Discovery**:
  - Scan S3 bucket/prefix for Hudi tables
  - Identify tables by `.hoodie` metadata directories
  - Extract table names from S3 paths

- **Schema Extraction**:
  - Parse Hudi metadata files (`.hoodie/metadata`)
  - Extract column definitions and data types
  - Map Hudi types to OpenMetadata types
  - Extract table properties (partition keys, file format, etc.)

- **Data Sampling for Profiling**:
  - Read sample parquet files from Hudi table
  - Convert to pandas DataFrame for profiling
  - Support configurable sample size

- **Yield EntityConfig**:
  - Generate `EntityConfig` for:
    - DatabaseService (Datalake type)
    - Database (from config)
    - DatabaseSchema (from config)
    - Table (discovered from S3)
  - Include all metadata needed for entity creation

**Deliverables**:
- Working S3 Hudi connector
- Discovery of Hudi tables functional
- Schema extraction working
- Data sampling for profiling
- Integration with entity handlers

**Note**: Other connectors (Postgres, OpenSearch, Airflow, MLflow, Kafka) are deferred to post-MVP phases.

---

### Phase 6: Profiling System

**Goal**: Implement data and schema profiling

#### 6.1 Schema Profiler (`profiling/schema_profiler.py`)
- Extract schema metadata from entities
- Compute schema fingerprint (hash)
- Track schema snapshots over time
- Store in local files organized by entity
- Schema history and evolution timeline

#### 6.2 Data Profiler (`profiling/data_profiler.py`)
- Sample data collection via source connectors
- Column-level statistics:
  - Null count and percentage
  - Distinct value count
  - Min/max values
  - Mean, median, std dev (numeric columns)
  - Value distributions
- Table-level metrics:
  - Row count
  - Column count
  - Sample percentage used
- `TableProfile` and `ColumnProfile` data models

#### 6.3 Profile Storage (`profiling/storage.py`)
- Store profiles as JSON files
- Organization: `{base_path}/{profile_type}/{date}/{entity_fqn}_{timestamp}.json`
- Load latest profile for comparison
- Profile retention and cleanup

#### 6.4 Integration with Executor
- Call profilers after entity creation/update
- Conditional profiling based on config
- Store profiles locally
- Optionally push to OpenMetadata

**Deliverables**:
- Both data and schema profiling working
- Profile storage and retrieval
- Integration with main execution flow

---

### Phase 7: Audit Logging System

**Goal**: Comprehensive operation tracking

#### 7.1 Audit Models (`audit/models.py`)
- `AuditEvent` dataclass:
  - Event type (success/failed/skipped/dry_run/validation_error)
  - Entity type and FQN
  - Timestamp
  - Operation (create/update/skip/delete)
  - Error details if failed
  - Schema changes if applicable
  - Duration
- Enum types for event classification

#### 7.2 Audit Logger (`audit/logger.py`)
- Initialize timestamped log file per run
- Real-time event logging to JSON
- File organization: `{output_dir}/{date}/ingestion_{timestamp}.json`
- Execution summary generation:
  - Total entities processed
  - Success/failure/skip counts
  - Error details
  - Timing information

#### 7.3 Audit Storage (`audit/storage.py`)
- JSON file writing with proper formatting
- Atomic writes to prevent corruption
- File rotation and retention
- Query helpers for audit history

**Deliverables**:
- Complete audit logging system
- JSON logs for every operation
- Summary statistics
- Error tracking

---

### Phase 8: Dry-Run Mode & Visualization

**Goal**: Preview operations without execution

#### 8.1 Dry-Run Formatter (`utils/formatting.py`)
- Rich console formatting for dry-run output
- Display operations by type:
  - **CREATE**: Show new entity JSON
  - **UPDATE**: Show diff (before/after)
  - **SKIP**: Show skip reason
- Schema change visualization in table format
- Validation error highlighting
- Summary statistics table

#### 8.2 Dry-Run Execution Flow
- Skip all OpenMetadata API calls
- Simulate entity lookups from context cache
- Execute all validation logic
- Show all planned operations
- Generate audit log in dry-run mode

**Deliverables**:
- Functional dry-run mode
- Rich CLI output with diffs
- Complete operation preview

---

### Phase 9: Basic CLI (MVP)

**Goal**: Simple command-line interface for ingestion

#### 9.1 Main Ingest Command (`cli/main.py`)
Implement Click-based CLI with core `ingest` command:

**`ingest`** - Main ingestion command:
```bash
om-ingest ingest config.yaml [--dry-run] [--audit-dir PATH] [--log-level LEVEL]
```

**Features**:
- Load YAML configuration
- Initialize OpenMetadata client
- Execute ingestion engine
- Display progress and results
- Return exit code (0=success, 1=failure)

#### 9.2 CLI Output
- Basic progress messages during execution
- Colored output for success/errors using rich
- Summary statistics after execution:
  - Total entities processed
  - Success/failure/skip counts
  - Audit log location
- Verbose mode with `--log-level DEBUG`

**Deliverables**:
- Working `ingest` command
- Basic help documentation
- Error handling and exit codes

**Note**: Additional commands (`discover`, `validate`, `list-sources`, `list-entities`) are deferred to post-MVP.

---

### Phase 10: Minimal Testing & Documentation (MVP)

**Goal**: Basic validation and usability

#### 10.1 Smoke Tests
Focus on critical paths only:

**Unit Tests**:
- Config loading (happy path + basic error cases)
- Dependency resolver (simple ordering + circular dependency detection)
- Schema comparator (column add/remove/change detection)
- Idempotency strategies (skip/update/fail behavior)

**Integration Tests**:
- S3 Hudi connector with mocked S3 (using moto)
- Database entity creation against live OpenMetadata instance
- End-to-end flow: YAML → entities in OpenMetadata

**Manual Testing Checklist**:
- [ ] Load YAML config successfully
- [ ] Connect to OpenMetadata instance
- [ ] Create database service
- [ ] Create database, schema, table in correct order
- [ ] Detect schema changes on re-run
- [ ] Idempotency works (skip existing entities)
- [ ] Profiling generates statistics
- [ ] Audit log created with correct events
- [ ] Dry-run mode shows operations without executing

#### 10.2 Basic Documentation
**README.md**:
- Installation instructions
- Quick start example (S3 Hudi → OpenMetadata)
- Basic YAML configuration example
- CLI usage

**Example Configurations**:
- `examples/s3_hudi_simple.yaml` - Minimal working example
- `examples/s3_hudi_profiling.yaml` - With profiling enabled
- `examples/templates/` - Template inheritance example

**Deliverables**:
- Smoke tests for critical paths
- Basic README with examples
- Manual testing checklist completed
- Example YAML configurations

**Note**: Comprehensive test coverage (80%+), detailed API docs, and architectural documentation are deferred to post-MVP iterations.

---

## Critical Design Decisions

### 1. Dependency Resolution: Topological Sort (Kahn's Algorithm)
- **Rationale**: Deterministic, efficient O(V+E), built-in cycle detection
- **Trade-off**: Requires upfront graph construction

### 2. Schema Evolution: Structural Comparison
- **Rationale**: Detects actual changes, no version management needed
- **Trade-off**: Cannot detect non-structural changes (descriptions)

### 3. Plugin Architecture: Registry Pattern
- **Rationale**: Easy extensibility, auto-discovery, type-safe
- **Trade-off**: All plugins loaded at startup

### 4. Configuration: YAML + Pydantic
- **Rationale**: Human-readable YAML, runtime validation, type safety
- **Trade-off**: YAML parsing overhead

### 5. Error Handling: Hybrid Fail-Fast/Optimistic
- **Rationale**: Maintains data integrity (fail on dependencies) while maximizing progress (optimistic on entity errors)
- **Trade-off**: More complex error handling logic

### 6. Audit Logging: Timestamped JSON Files
- **Rationale**: Simple, portable, no database dependency
- **Trade-off**: Not as queryable as database

### 7. Dry-Run: In-Memory Simulation
- **Rationale**: Fast, safe, deterministic
- **Trade-off**: Cannot validate actual OpenMetadata API responses

### 8. Profiling: Separate Data vs Schema
- **Rationale**: Different use cases, can disable data profiling for performance
- **Trade-off**: Two systems to maintain

### 9. Template Inheritance: Deep Merge
- **Rationale**: Flexible, DRY, clear precedence
- **Trade-off**: Complex merge logic

### 10. Sequential Processing: No Batching
- **Rationale**: Simple, clear failure points, guaranteed ordering
- **Trade-off**: Slower than parallel processing

---

## Example YAML Configurations

### Simple Database Ingestion
```yaml
metadata:
  name: "postgres-ingestion"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585"

sources:
  - name: "local-postgres"
    type: "postgres"
    properties:
      host: "localhost"
      database: "mydb"

entities:
  - type: "database_service"
    name: "LocalPostgres"

  - type: "database"
    name: "mydb"
    properties:
      service: "LocalPostgres"

  - type: "table"
    discovery:
      source: "local-postgres"
      filter:
        schema: "public"
```

### Multi-Source with Profiling
```yaml
metadata:
  name: "production-stack"
  version: "1.0"

defaults:
  idempotency: "update"
  profiling:
    enabled: true
    sample_percentage: 5

sources:
  - name: "data-lake"
    type: "s3_hudi"
    properties:
      bucket: "company-datalake"

  - name: "airflow-prod"
    type: "airflow"
    properties:
      host: "airflow.company.com"

entities:
  - type: "table"
    discovery:
      source: "data-lake"
    profiling:
      metrics:
        - "row_count"
        - "null_percentage"

  - type: "pipeline"
    discovery:
      source: "airflow-prod"
```

---

## Dependencies

### Core Dependencies
- `openmetadata-ingestion>=1.3.0` - OpenMetadata Python SDK
- `pydantic>=2.0` - Data validation
- `click>=8.0` - CLI framework
- `pyyaml>=6.0` - YAML parsing
- `rich>=13.0` - Terminal formatting

### Source Connectors
- `boto3` - AWS S3 for Hudi tables
- `opensearch-py` - OpenSearch connector
- `apache-airflow-client` - Airflow API
- `mlflow` - MLflow connector
- `kafka-python` - Kafka connector
- `sqlalchemy` - Database connectors
- `psycopg2-binary` - PostgreSQL driver

### Profiling
- `pandas>=2.0` - Data manipulation
- `numpy` - Numerical operations

### Development
- `pytest>=7.0` - Testing framework
- `pytest-cov` - Coverage reporting
- `pytest-mock` - Mocking
- `moto` - AWS mocking
- `ruff` - Linting
- `black` - Code formatting
- `mypy` - Type checking

---

## MVP Success Criteria

### Functional Requirements (MVP Only)
- ✅ Load and validate YAML configurations
- ✅ Resolve entity dependencies with topological sort
- ✅ Create/update database entities in OpenMetadata
- ✅ Detect schema changes (structural only) for tables
- ✅ Support database entities (Service, Database, Schema, Table)
- ✅ Support S3 Hudi data source
- ✅ Profile data (statistics) and schema (structure)
- ✅ Log all operations to JSON audit files
- ✅ Dry-run mode with diffs
- ✅ Template inheritance for configs
- ✅ Idempotency (skip/update/fail)
- ✅ Fail-fast on dependency errors
- ✅ Optimistic on entity errors

### Non-Functional Requirements (MVP Only)
- ✅ Basic smoke tests
- ✅ Type hints on public APIs
- ✅ README with examples
- ✅ Extensible architecture (easy to add sources/entities later)
- ✅ Clear error messages
- ✅ Working CLI

### Deferred to Post-MVP
- Additional entity types (Pipeline, Topic, MLModel, SearchIndex, Tags, Users)
- Additional source connectors (Postgres, OpenSearch, Airflow, MLflow, Kafka)
- 80%+ test coverage
- Comprehensive API documentation
- Advanced CLI commands (discover, validate, list)

---

## MVP Build Order (Recommended)

### Iteration 1: Core Foundation (Week 1)
**Goal**: Get basic infrastructure working

1. **Project Setup**
   - Initialize project with Poetry
   - Add core dependencies
   - Create directory structure

2. **Configuration System**
   - `src/om_ingest/config/schema.py` - Pydantic models
   - `src/om_ingest/config/loader.py` - YAML loading
   - `src/om_ingest/config/template_engine.py` - Template inheritance
   - Basic unit tests

3. **OpenMetadata Client**
   - `src/om_ingest/core/client.py` - Wrapper around OM SDK
   - Test connection to live OpenMetadata instance

**Validation**: Can load YAML config and connect to OpenMetadata

---

### Iteration 2: Dependency Resolution (Week 1)
**Goal**: Entity ordering system working

1. **Dependency Resolver**
   - `src/om_ingest/core/dependency_resolver.py` - Topological sort
   - Unit tests for ordering and cycle detection

2. **Execution Context**
   - `src/om_ingest/core/context.py` - State management
   - Entity cache for dependency validation

**Validation**: Can resolve entity dependencies and determine execution order

---

### Iteration 3: Entity System (Week 2)
**Goal**: Database entities can be created

1. **Entity Base & Registry**
   - `src/om_ingest/entities/base.py` - Abstract handler
   - `src/om_ingest/entities/registry.py` - Registration system

2. **Database Entity Handlers**
   - `src/om_ingest/entities/database/database_service.py` - DatabaseServiceHandler
   - `src/om_ingest/entities/database/database.py` - DatabaseHandler
   - `src/om_ingest/entities/database/schema.py` - DatabaseSchemaHandler
   - `src/om_ingest/entities/database/table.py` - TableHandler

**Validation**: Can manually create database entities via handlers

---

### Iteration 4: Execution Engine (Week 2)
**Goal**: End-to-end entity creation working

1. **Schema Comparator**
   - `src/om_ingest/core/schema_comparator.py` - Change detection

2. **Idempotency Strategies**
   - `src/om_ingest/strategies/idempotency.py` - Skip/update/fail logic

3. **Error Handling**
   - `src/om_ingest/strategies/error_handling.py` - Error strategies

4. **Entity Executor**
   - `src/om_ingest/core/executor.py` - Entity execution logic

5. **Ingestion Engine**
   - `src/om_ingest/core/engine.py` - Main orchestration

**Validation**: Can ingest entities from YAML to OpenMetadata

---

### Iteration 5: S3 Hudi Connector (Week 3)
**Goal**: Discover tables from S3

1. **Source Base & Registry**
   - `src/om_ingest/sources/base.py` - Abstract interface
   - `src/om_ingest/sources/registry.py` - Plugin system

2. **S3 Hudi Connector**
   - `src/om_ingest/sources/s3_hudi/connector.py` - Main connector
   - `src/om_ingest/sources/s3_hudi/parser.py` - Schema parsing
   - Integration test with mocked S3

**Validation**: Can discover Hudi tables and extract schemas

---

### Iteration 6: Profiling (Week 3)
**Goal**: Data and schema profiling working

1. **Schema Profiler**
   - `src/om_ingest/profiling/schema_profiler.py` - Schema tracking
   - `src/om_ingest/profiling/storage.py` - Profile storage

2. **Data Profiler**
   - `src/om_ingest/profiling/data_profiler.py` - Statistics collection

**Validation**: Profiles generated and stored

---

### Iteration 7: Audit Logging (Week 4)
**Goal**: All operations tracked

1. **Audit System**
   - `src/om_ingest/audit/models.py` - Event models
   - `src/om_ingest/audit/logger.py` - Event logging
   - `src/om_ingest/audit/storage.py` - JSON file storage

**Validation**: Audit logs created with all events

---

### Iteration 8: Dry-Run & CLI (Week 4)
**Goal**: User-facing interface complete

1. **Dry-Run Formatter**
   - `src/om_ingest/utils/formatting.py` - Output formatting
   - `src/om_ingest/utils/logging.py` - Logging setup

2. **CLI**
   - `src/om_ingest/cli/main.py` - Click-based CLI
   - Basic `ingest` command

**Validation**: CLI works, dry-run shows operations

---

### Iteration 9: Testing & Examples (Week 4)
**Goal**: MVP ready to use

1. **Tests**
   - Smoke tests for critical paths
   - Integration test: YAML → OpenMetadata

2. **Documentation**
   - README.md with quick start
   - Example YAML configurations
   - Manual testing checklist

**Validation**: All manual tests pass, examples work

---

## Critical Files to Create (In Order)

### Phase 1: Foundation
1. `pyproject.toml` - Project configuration
2. `src/om_ingest/__init__.py` - Package init
3. `src/om_ingest/config/schema.py` - Pydantic models **[MOST CRITICAL]**
4. `src/om_ingest/config/loader.py` - YAML loading
5. `src/om_ingest/config/template_engine.py` - Template system
6. `src/om_ingest/core/client.py` - OM client wrapper

### Phase 2: Core Logic
7. `src/om_ingest/core/dependency_resolver.py` - Topological sort **[CRITICAL]**
8. `src/om_ingest/core/context.py` - Execution state
9. `src/om_ingest/entities/base.py` - Entity abstraction **[CRITICAL]**
10. `src/om_ingest/entities/registry.py` - Entity registry

### Phase 3: Database Entities
11. `src/om_ingest/entities/database/database_service.py`
12. `src/om_ingest/entities/database/database.py`
13. `src/om_ingest/entities/database/schema.py`
14. `src/om_ingest/entities/database/table.py`

### Phase 4: Execution
15. `src/om_ingest/core/schema_comparator.py` - Change detection
16. `src/om_ingest/strategies/idempotency.py` - Strategies
17. `src/om_ingest/strategies/error_handling.py` - Error handling
18. `src/om_ingest/core/executor.py` - Entity execution **[CRITICAL]**
19. `src/om_ingest/core/engine.py` - Main orchestration **[CRITICAL]**

### Phase 5: Data Source
20. `src/om_ingest/sources/base.py` - Source interface **[CRITICAL]**
21. `src/om_ingest/sources/registry.py` - Source registry
22. `src/om_ingest/sources/s3_hudi/connector.py` - S3 connector

### Phase 6-9: Supporting Systems
23. `src/om_ingest/profiling/` - Profiling modules
24. `src/om_ingest/audit/` - Audit logging
25. `src/om_ingest/utils/` - Utilities
26. `src/om_ingest/cli/main.py` - CLI entry point

---

## Next Steps After Implementation

### Future Enhancements (Not in Initial Scope)
1. **Parallel Processing**: Option to process independent entities in parallel
2. **Incremental Discovery**: Only discover new/changed entities
3. **Schema Registry Integration**: Native Avro/Protobuf schema support
4. **Lineage Tracking**: Automatic lineage detection from queries/configs
5. **Notifications**: Slack/email alerts on schema changes or failures
6. **Web UI**: Optional web interface for config management
7. **Scheduling**: Built-in scheduler for periodic ingestion
8. **Metrics Export**: Prometheus metrics for monitoring
9. **Delta Detection**: Track data changes beyond schema
10. **Advanced Profiling**: Data quality rules, anomaly detection
