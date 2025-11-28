# Configuration Guide

Complete guide to configuring YAML files for OpenMetadata ingestion.

---

## Table of Contents

1. [Overview](#overview)
2. [Configuration Structure](#configuration-structure)
3. [Section Reference](#section-reference)
   - [Metadata](#metadata)
   - [OpenMetadata](#openmetadata)
   - [Sources](#sources)
   - [Defaults](#defaults)
   - [Entities](#entities)
   - [Audit](#audit)
   - [Execution](#execution)
4. [Source Connectors](#source-connectors)
5. [Discovery vs Static Configuration](#discovery-vs-static-configuration)
6. [Complete Examples](#complete-examples)
7. [Best Practices](#best-practices)

---

## Overview

The ingestion library uses YAML configuration files to define:
- What data sources to connect to
- What entities to discover or create
- How to handle conflicts and errors
- Where to send the data (OpenMetadata instance)

Every configuration file follows the same basic structure with required and optional sections.

---

## Configuration Structure

```yaml
# Required: Metadata about this ingestion configuration
metadata:
  name: "my-ingestion-job"
  version: "1.0"
  description: "Optional description"

# Required: OpenMetadata connection details
openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: true
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

# Optional: Data source connectors
sources:
  - name: "my-source"
    type: "s3_hudi"
    properties:
      # Source-specific properties

# Optional: Default behaviors
defaults:
  idempotency: "update"

# Required: Entities to process
entities:
  - type: "table"
    # Entity configuration

# Optional: Audit logging
audit:
  enabled: true
  output_dir: "./audit_logs"

# Optional: Execution settings
execution:
  dry_run: false
  continue_on_error: true
  fail_fast_on_dependency: true
```

---

## Section Reference

### Metadata

**Purpose:** Identifies and describes the ingestion configuration.

**Required:** Yes

**Properties:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | Yes | Unique name for this ingestion job |
| `version` | string | Yes | Version of the configuration (e.g., "1.0") |
| `description` | string | No | Human-readable description |

**Example:**

```yaml
metadata:
  name: "production-datalake-ingestion"
  version: "2.1"
  description: "Daily ingestion of Hudi tables from production S3 bucket"
```

---

### OpenMetadata

**Purpose:** Configure connection to your OpenMetadata instance.

**Required:** Yes

**Properties:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `host` | string | Yes | OpenMetadata API URL (must end with `/api`) |
| `verify_ssl` | boolean | No | Enable SSL certificate verification (default: `true`) |
| `auth` | object | Yes | Authentication configuration |
| `auth.type` | string | Yes | Authentication type: `"openmetadata"`, `"no-auth"`, `"google"`, `"okta"`, `"auth0"`, `"azure"` |
| `auth.jwt_token` | string | Yes* | JWT token for OpenMetadata auth (*required for `openmetadata` type) |

**Environment Variable Substitution:**

You can use `${VAR_NAME}` to reference environment variables:

```yaml
openmetadata:
  host: "${OPENMETADATA_HOST}"
  auth:
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"
```

**Examples:**

```yaml
# Local development
openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

# Production with SSL
openmetadata:
  host: "https://metadata.company.com/api"
  verify_ssl: true
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

# No authentication (not recommended)
openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "no-auth"
```

**Getting Your JWT Token:**

1. Go to OpenMetadata UI
2. Navigate to: **Settings** → **Bots** → **ingestion-bot**
3. Copy the JWT token
4. Set environment variable: `export OPENMETADATA_JWT_TOKEN="your-token"`

---

### Sources

**Purpose:** Define data source connectors for discovering entities.

**Required:** No (only needed for discovery-based ingestion)

**Structure:**

```yaml
sources:
  - name: "unique-source-name"
    type: "source_type"
    properties:
      # Source-specific properties
```

**Common Properties:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | Yes | Unique identifier for this source |
| `type` | string | Yes | Source connector type (see [Source Connectors](#source-connectors)) |
| `properties` | object | Yes | Source-specific configuration |

**Multiple Sources:**

You can define multiple sources in one configuration:

```yaml
sources:
  - name: "production-datalake"
    type: "s3_hudi"
    properties:
      bucket: "prod-data"
      # ...

  - name: "staging-datalake"
    type: "s3_hudi"
    properties:
      bucket: "staging-data"
      # ...
```

---

### Defaults

**Purpose:** Set default behaviors for entity processing.

**Required:** No

**Properties:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `idempotency` | string | `"skip"` | How to handle existing entities: `"skip"`, `"update"`, or `"fail"` |

**Idempotency Modes:**

- **`skip`**: If entity exists, skip it without making changes
- **`update`**: If entity exists, update it with new properties
- **`fail`**: If entity exists, fail with an error

**Example:**

```yaml
defaults:
  idempotency: "update"  # Always update existing entities
```

**Note:** Individual entities can override this with their own `idempotency` setting.

---

### Entities

**Purpose:** Define which entities to process.

**Required:** Yes

**Two Approaches:**

1. **Discovery-based**: Automatically discover entities from a data source
2. **Static**: Manually define entities with explicit properties

#### Discovery-Based Entities

**Structure:**

```yaml
entities:
  - type: "entity_type"
    discovery:
      source: "source-name"
      filter:
        key: "value"
      include_pattern: "regex"
      exclude_pattern: "regex"
    idempotency: "update"  # Optional override
```

**Properties:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `type` | string | Yes | Entity type: `database_service`, `database`, `database_schema`, `table` |
| `discovery` | object | Yes* | Discovery configuration (*for discovery-based) |
| `discovery.source` | string | Yes | Name of source to discover from (must match a `sources` entry) |
| `discovery.filter` | object | No | Key-value filters for discovery |
| `discovery.include_pattern` | string | No | Regex pattern for entities to include |
| `discovery.exclude_pattern` | string | No | Regex pattern for entities to exclude |
| `idempotency` | string | No | Override default idempotency mode for this entity |

**Example:**

```yaml
entities:
  # Discover all tables
  - type: "table"
    discovery:
      source: "my-datalake"

  # Discover tables matching pattern
  - type: "table"
    discovery:
      source: "my-datalake"
      include_pattern: "^prod_.*"  # Only tables starting with "prod_"
      exclude_pattern: ".*_temp$"   # Exclude tables ending with "_temp"
```

#### Static Entities

**Structure:**

```yaml
entities:
  - type: "entity_type"
    name: "entity-name"
    properties:
      # Entity-specific properties
    idempotency: "skip"  # Optional override
```

**Properties:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `type` | string | Yes | Entity type |
| `name` | string | Yes | Entity name |
| `properties` | object | Yes | Entity-specific properties (see entity types below) |
| `idempotency` | string | No | Override default idempotency mode |

**Entity Types and Required Properties:**

##### Database Service

```yaml
- type: "database_service"
  name: "my-datalake-service"
  properties:
    service_type: "Datalake"  # Required
    description: "My datalake service"
```

##### Database

```yaml
- type: "database"
  name: "my-database"
  properties:
    service: "my-datalake-service"  # Required: parent service name
    description: "My database"
```

##### Database Schema

```yaml
- type: "database_schema"
  name: "my-schema"
  properties:
    service: "my-datalake-service"  # Required: parent service name
    database: "my-database"         # Required: parent database name
    description: "My schema"
```

##### Table

```yaml
- type: "table"
  name: "my-table"
  properties:
    service: "my-datalake-service"     # Required
    database: "my-database"            # Required
    database_schema: "my-schema"       # Required
    columns:                           # Required
      - name: "id"
        dataType: "BIGINT"
        dataTypeDisplay: "bigint"
      - name: "name"
        dataType: "STRING"
        dataTypeDisplay: "string"
    tableType: "External"              # Optional: "Regular", "External", "View"
    description: "My table"
    sourceUrl: "s3://bucket/path"      # Optional
```

---

### Audit

**Purpose:** Configure audit logging for tracking ingestion operations.

**Required:** No

**Properties:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable audit logging |
| `output_dir` | string | `"./audit_logs"` | Directory to write audit logs |

**Example:**

```yaml
audit:
  enabled: true
  output_dir: "/var/log/openmetadata/ingestion"
```

**Audit Log Format:**

Audit logs are written as JSON files with timestamps:
- Filename: `audit_YYYYMMDD_HHMMSS.json`
- Contains: All processed entities, operations, errors, and summary

---

### Execution

**Purpose:** Control how the ingestion process executes.

**Required:** No

**Properties:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `dry_run` | boolean | `false` | Preview changes without writing to OpenMetadata |
| `continue_on_error` | boolean | `true` | Continue processing if an entity fails |
| `fail_fast_on_dependency` | boolean | `true` | Stop immediately if dependency validation fails |

**Example:**

```yaml
execution:
  dry_run: true                      # Preview mode
  continue_on_error: false           # Stop on first error
  fail_fast_on_dependency: true      # Stop on missing dependencies
```

**Dry Run Mode:**

When `dry_run: true`:
- ✓ Connects to data sources
- ✓ Discovers entities
- ✓ Validates dependencies
- ✓ Checks for existing entities
- ✗ Does NOT create/update entities in OpenMetadata

Use dry run to:
- Test configurations
- Preview what will be created/updated
- Validate connections

---

## Source Connectors

### S3 Hudi Connector

**Type:** `s3_hudi`

**Purpose:** Discover and ingest Apache Hudi tables from S3-compatible storage.

**Properties:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `bucket` | string | Yes | S3 bucket name |
| `prefix` | string | No | Prefix/path within bucket (e.g., `"hudi-tables"`) |
| `endpoint_url` | string | No | Custom S3 endpoint (for MinIO, LocalStack) |
| `region` | string | No | AWS region (default: `"us-east-1"`) |
| `aws_access_key_id` | string | No | AWS access key (or use default credentials) |
| `aws_secret_access_key` | string | No | AWS secret key (or use default credentials) |
| `database_service_name` | string | Yes | Name of database service to use/create |
| `database_name` | string | Yes | Name of database to use/create |
| `schema_name` | string | Yes | Name of schema to use/create |

**AWS Credentials:**

The connector supports multiple authentication methods (in order of precedence):
1. Explicit `aws_access_key_id` and `aws_secret_access_key` in config
2. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. AWS credentials file (`~/.aws/credentials`)
4. IAM role (when running on EC2/ECS)

**Example: AWS S3**

```yaml
sources:
  - name: "production-datalake"
    type: "s3_hudi"
    properties:
      bucket: "company-datalake"
      prefix: "hudi/tables"
      region: "us-west-2"
      database_service_name: "prod_datalake"
      database_name: "analytics"
      schema_name: "default"
      # Uses default AWS credentials
```

**Example: MinIO (Local)**

```yaml
sources:
  - name: "local-datalake"
    type: "s3_hudi"
    properties:
      bucket: "test-datalake"
      prefix: "hudi-tables"
      endpoint_url: "http://localhost:9000"
      region: "us-east-1"
      aws_access_key_id: "minioadmin"
      aws_secret_access_key: "minioadmin"
      database_service_name: "local_test_datalake"
      database_name: "test_database"
      schema_name: "default"
```

---

## Discovery vs Static Configuration

### When to Use Discovery

**Use discovery when:**
- You have many entities to ingest
- Entities are dynamically created/updated in the source
- You want automated discovery of new entities
- Schema information is available in the source

**Example:**

```yaml
# Automatically discover all Hudi tables from S3
sources:
  - name: "my-datalake"
    type: "s3_hudi"
    properties:
      bucket: "data-lake"
      # ...

entities:
  - type: "database_service"
    discovery:
      source: "my-datalake"

  - type: "database"
    discovery:
      source: "my-datalake"

  - type: "database_schema"
    discovery:
      source: "my-datalake"

  - type: "table"
    discovery:
      source: "my-datalake"
```

### When to Use Static Configuration

**Use static when:**
- You have a small, fixed set of entities
- You need precise control over properties
- The source doesn't support discovery
- You're manually defining metadata

**Example:**

```yaml
# Manually define entities
entities:
  - type: "database_service"
    name: "manual-service"
    properties:
      service_type: "Datalake"

  - type: "database"
    name: "manual-database"
    properties:
      service: "manual-service"

  - type: "database_schema"
    name: "manual-schema"
    properties:
      service: "manual-service"
      database: "manual-database"

  - type: "table"
    name: "manual-table"
    properties:
      service: "manual-service"
      database: "manual-database"
      database_schema: "manual-schema"
      columns:
        - name: "id"
          dataType: "BIGINT"
```

### Hybrid Approach

You can mix static and discovery:

```yaml
# Create service and database manually, discover tables
entities:
  # Static
  - type: "database_service"
    name: "my-service"
    properties:
      service_type: "Datalake"

  # Static
  - type: "database"
    name: "my-database"
    properties:
      service: "my-service"

  # Static
  - type: "database_schema"
    name: "my-schema"
    properties:
      service: "my-service"
      database: "my-database"

  # Discovery
  - type: "table"
    discovery:
      source: "my-source"
```

---

## Complete Examples

### Example 1: Full Discovery (New Setup)

**Use Case:** First-time setup, discover everything from S3.

```yaml
metadata:
  name: "initial-datalake-setup"
  version: "1.0"
  description: "Initial discovery of all Hudi tables from production S3"

openmetadata:
  host: "https://metadata.company.com/api"
  verify_ssl: true
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "production-s3"
    type: "s3_hudi"
    properties:
      bucket: "company-datalake-prod"
      prefix: "hudi/tables"
      region: "us-west-2"
      database_service_name: "prod_datalake"
      database_name: "analytics"
      schema_name: "default"

defaults:
  idempotency: "skip"  # Don't overwrite if exists

entities:
  # Discover service
  - type: "database_service"
    discovery:
      source: "production-s3"

  # Discover database
  - type: "database"
    discovery:
      source: "production-s3"

  # Discover schema
  - type: "database_schema"
    discovery:
      source: "production-s3"

  # Discover all tables
  - type: "table"
    discovery:
      source: "production-s3"

audit:
  enabled: true
  output_dir: "./logs"

execution:
  dry_run: false
  continue_on_error: true
  fail_fast_on_dependency: true
```

### Example 2: Incremental Updates (Existing Database)

**Use Case:** Database already exists, only discover new tables.

```yaml
metadata:
  name: "daily-table-discovery"
  version: "1.0"
  description: "Daily discovery of new tables in existing database"

openmetadata:
  host: "https://metadata.company.com/api"
  verify_ssl: true
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "production-s3"
    type: "s3_hudi"
    properties:
      bucket: "company-datalake-prod"
      prefix: "hudi/tables"
      region: "us-west-2"
      # Reference EXISTING entities
      database_service_name: "prod_datalake"  # Must exist!
      database_name: "analytics"              # Must exist!
      schema_name: "default"                  # Must exist!

defaults:
  idempotency: "update"  # Update existing tables

entities:
  # Only discover tables (parents must exist!)
  - type: "table"
    discovery:
      source: "production-s3"

execution:
  dry_run: false
  continue_on_error: true
  fail_fast_on_dependency: true
```

### Example 3: Filtered Discovery

**Use Case:** Only discover specific tables matching patterns.

```yaml
metadata:
  name: "filtered-discovery"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "datalake"
    type: "s3_hudi"
    properties:
      bucket: "data-lake"
      prefix: "tables"
      database_service_name: "my_datalake"
      database_name: "analytics"
      schema_name: "default"

entities:
  - type: "table"
    discovery:
      source: "datalake"
      # Only production tables, exclude temp tables
      include_pattern: "^prod_.*"
      exclude_pattern: ".*_(temp|staging)$"
```

### Example 4: Multiple Environments

**Use Case:** Discover from multiple S3 buckets (dev, staging, prod).

```yaml
metadata:
  name: "multi-environment-discovery"
  version: "1.0"

openmetadata:
  host: "https://metadata.company.com/api"
  verify_ssl: true
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  # Development
  - name: "dev-datalake"
    type: "s3_hudi"
    properties:
      bucket: "datalake-dev"
      prefix: "hudi/tables"
      region: "us-west-2"
      database_service_name: "dev_datalake"
      database_name: "dev_analytics"
      schema_name: "default"

  # Staging
  - name: "staging-datalake"
    type: "s3_hudi"
    properties:
      bucket: "datalake-staging"
      prefix: "hudi/tables"
      region: "us-west-2"
      database_service_name: "staging_datalake"
      database_name: "staging_analytics"
      schema_name: "default"

  # Production
  - name: "prod-datalake"
    type: "s3_hudi"
    properties:
      bucket: "datalake-prod"
      prefix: "hudi/tables"
      region: "us-west-2"
      database_service_name: "prod_datalake"
      database_name: "prod_analytics"
      schema_name: "default"

defaults:
  idempotency: "update"

entities:
  # Discover from all environments
  - type: "database_service"
    discovery:
      source: "dev-datalake"

  - type: "database_service"
    discovery:
      source: "staging-datalake"

  - type: "database_service"
    discovery:
      source: "prod-datalake"

  # ... repeat for database, schema, table for each source
```

### Example 5: Static Manual Configuration

**Use Case:** Manually define a small set of tables.

```yaml
metadata:
  name: "manual-tables"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

defaults:
  idempotency: "update"

entities:
  # Define service
  - type: "database_service"
    name: "my_manual_service"
    properties:
      service_type: "Datalake"
      description: "Manually configured datalake service"

  # Define database
  - type: "database"
    name: "my_database"
    properties:
      service: "my_manual_service"
      description: "My database"

  # Define schema
  - type: "database_schema"
    name: "public"
    properties:
      service: "my_manual_service"
      database: "my_database"
      description: "Public schema"

  # Define table
  - type: "table"
    name: "users"
    properties:
      service: "my_manual_service"
      database: "my_database"
      database_schema: "public"
      tableType: "Regular"
      description: "Users table"
      columns:
        - name: "id"
          dataType: "BIGINT"
          dataTypeDisplay: "bigint"
          description: "User ID"
        - name: "username"
          dataType: "STRING"
          dataTypeDisplay: "string"
          description: "Username"
        - name: "email"
          dataType: "STRING"
          dataTypeDisplay: "string"
          description: "Email address"
        - name: "created_at"
          dataType: "TIMESTAMP"
          dataTypeDisplay: "timestamp"
          description: "Account creation timestamp"
```

### Example 6: Dry Run Testing

**Use Case:** Test configuration without making changes.

```yaml
metadata:
  name: "test-configuration"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "test-source"
    type: "s3_hudi"
    properties:
      bucket: "test-bucket"
      endpoint_url: "http://localhost:9000"
      aws_access_key_id: "minioadmin"
      aws_secret_access_key: "minioadmin"
      database_service_name: "test_service"
      database_name: "test_db"
      schema_name: "default"

entities:
  - type: "table"
    discovery:
      source: "test-source"

execution:
  dry_run: true  # PREVIEW ONLY - no changes will be made
  continue_on_error: true
  fail_fast_on_dependency: true

audit:
  enabled: true
  output_dir: "./test_logs"
```

---

## Best Practices

### 1. Use Environment Variables for Secrets

**❌ Bad:**
```yaml
openmetadata:
  auth:
    jwt_token: "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpz..."  # Hardcoded secret!
```

**✅ Good:**
```yaml
openmetadata:
  auth:
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"
```

### 2. Start with Dry Run

Always test new configurations in dry-run mode first:

```yaml
execution:
  dry_run: true  # Test first!
```

Then switch to production:

```yaml
execution:
  dry_run: false  # After validation
```

### 3. Enable Audit Logging

Track all operations for debugging and compliance:

```yaml
audit:
  enabled: true
  output_dir: "/var/log/ingestion"
```

### 4. Use Descriptive Names

**❌ Bad:**
```yaml
metadata:
  name: "config1"
```

**✅ Good:**
```yaml
metadata:
  name: "production-daily-table-discovery"
  version: "2.1"
  description: "Daily discovery of new tables from production S3 bucket"
```

### 5. Separate Environments

Use different configuration files for different environments:

```
configs/
  ├── dev.yaml
  ├── staging.yaml
  └── production.yaml
```

### 6. Use Filters Wisely

Avoid ingesting temporary or test data:

```yaml
entities:
  - type: "table"
    discovery:
      source: "my-source"
      exclude_pattern: ".*(temp|test|staging|backup).*"
```

### 7. Choose Idempotency Mode Carefully

- **Development:** `idempotency: "update"` (always sync latest)
- **Production:** `idempotency: "skip"` or `"update"` based on needs
- **One-time import:** `idempotency: "skip"`

### 8. Organize Large Configurations

Break complex configurations into multiple files:

```yaml
# 1. setup_hierarchy.yaml - Create service/database/schema
metadata:
  name: "setup-database-hierarchy"
entities:
  - type: "database_service"
    # ...

# 2. ingest_tables.yaml - Discover tables
metadata:
  name: "discover-tables"
entities:
  - type: "table"
    discovery:
      source: "my-source"
```

Run them in sequence:
```bash
python -m om_ingest.cli run --config setup_hierarchy.yaml
python -m om_ingest.cli run --config ingest_tables.yaml
```

### 9. Document Your Configuration

Add comments to explain non-obvious settings:

```yaml
sources:
  - name: "legacy-datalake"
    type: "s3_hudi"
    properties:
      bucket: "old-data-lake"
      # Note: This bucket uses legacy naming convention
      # Tables are prefixed with "v1_" instead of "prod_"
      database_service_name: "legacy_service"
```

### 10. Version Your Configurations

Track configuration changes in version control:

```yaml
metadata:
  name: "production-ingestion"
  version: "2.3"  # Increment on changes
  description: "v2.3: Added filtering to exclude temp tables"
```

---

## Troubleshooting

### Common Errors

#### Error: "Missing required property 'database'"

**Cause:** Trying to create tables without parent entities existing.

**Solution:** Either:
1. Use full discovery (discover service, database, schema, table)
2. Ensure parent entities exist before discovering tables

#### Error: "Missing dependency: service.database.schema"

**Cause:** Parent entities don't exist in OpenMetadata.

**Solutions:**
1. Check entity names match exactly (case-sensitive)
2. Create parent entities first
3. Use full discovery mode

#### Error: "JWT token is required"

**Cause:** Environment variable not set.

**Solution:**
```bash
export OPENMETADATA_JWT_TOKEN="your-token-here"
```

#### Error: "Failed to connect to S3"

**Causes & Solutions:**
- **Wrong endpoint:** Check `endpoint_url` for MinIO/LocalStack
- **Invalid credentials:** Verify `aws_access_key_id` and `aws_secret_access_key`
- **Network issues:** Check firewall, VPN, security groups

---

## Additional Resources

- [Entity Discovery Modes](./ENTITY_DISCOVERY_MODES.md)
- [Source Connector Development](./SOURCE_CONNECTOR_DEV.md)
- [OpenMetadata API Documentation](https://docs.open-metadata.org)

---

**Last Updated:** 2025-11-28
