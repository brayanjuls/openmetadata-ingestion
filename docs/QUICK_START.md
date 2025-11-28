# Quick Start Guide

Get started with OpenMetadata ingestion in 5 minutes.

---

## Prerequisites

1. **OpenMetadata instance running** (local or remote)
2. **JWT token** from OpenMetadata UI:
   - Go to **Settings** → **Bots** → **ingestion-bot**
   - Copy the token
3. **Set environment variable:**
   ```bash
   export OPENMETADATA_JWT_TOKEN="your-token-here"
   ```

---

## Common Scenarios

### Scenario 1: First-Time Setup - Discover Everything

**When to use:** Brand new setup, empty OpenMetadata instance.

**Create `config.yaml`:**

```yaml
metadata:
  name: "initial-setup"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "my-datalake"
    type: "s3_hudi"
    properties:
      bucket: "my-bucket"
      prefix: "hudi-tables"
      region: "us-east-1"
      database_service_name: "my_datalake"
      database_name: "analytics"
      schema_name: "default"

defaults:
  idempotency: "skip"

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

execution:
  dry_run: false
```

**Run:**
```bash
python -m om_ingest.cli run --config config.yaml
```

---

### Scenario 2: Daily Updates - Add New Tables Only

**When to use:** Database already exists, only discover new tables.

**Create `daily-update.yaml`:**

```yaml
metadata:
  name: "daily-table-discovery"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "my-datalake"
    type: "s3_hudi"
    properties:
      bucket: "my-bucket"
      prefix: "hudi-tables"
      region: "us-east-1"
      # IMPORTANT: These must match existing entities!
      database_service_name: "my_datalake"  # Must exist
      database_name: "analytics"            # Must exist
      schema_name: "default"                # Must exist

defaults:
  idempotency: "update"

entities:
  # Only discover tables - parents must exist!
  - type: "table"
    discovery:
      source: "my-datalake"

execution:
  dry_run: false
```

**Run:**
```bash
python -m om_ingest.cli run --config daily-update.yaml
```

---

### Scenario 3: Local Testing with MinIO

**When to use:** Testing locally before production.

**Start MinIO:**
```bash
docker-compose -f docker-compose-minio.yml up -d
```

**Create `test-local.yaml`:**

```yaml
metadata:
  name: "local-test"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "local-minio"
    type: "s3_hudi"
    properties:
      bucket: "test-datalake"
      prefix: "hudi-tables"
      endpoint_url: "http://localhost:9000"  # MinIO endpoint
      region: "us-east-1"
      aws_access_key_id: "minioadmin"
      aws_secret_access_key: "minioadmin"
      database_service_name: "local_datalake"
      database_name: "test_db"
      schema_name: "default"

defaults:
  idempotency: "update"

entities:
  - type: "database_service"
    discovery:
      source: "local-minio"

  - type: "database"
    discovery:
      source: "local-minio"

  - type: "database_schema"
    discovery:
      source: "local-minio"

  - type: "table"
    discovery:
      source: "local-minio"

execution:
  dry_run: false
```

---

### Scenario 4: Dry Run - Test Before Running

**When to use:** Testing configuration, preview changes.

**Any config with dry run enabled:**

```yaml
# ... your configuration ...

execution:
  dry_run: true  # ← PREVIEW ONLY, no changes made
  continue_on_error: true
  fail_fast_on_dependency: true

audit:
  enabled: true
  output_dir: "./test_logs"
```

**Run:**
```bash
python -m om_ingest.cli run --config your-config.yaml
```

**Review logs to see what would be created/updated.**

---

## Configuration Templates

### Minimal Template

```yaml
metadata:
  name: "my-job"
  version: "1.0"

openmetadata:
  host: "http://localhost:8585/api"
  verify_ssl: false
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

# Add your configuration here
entities: []
```

### S3 Hudi Template (AWS)

```yaml
metadata:
  name: "s3-hudi-ingestion"
  version: "1.0"

openmetadata:
  host: "${OPENMETADATA_HOST}"
  verify_ssl: true
  auth:
    type: "openmetadata"
    jwt_token: "${OPENMETADATA_JWT_TOKEN}"

sources:
  - name: "production-s3"
    type: "s3_hudi"
    properties:
      bucket: "YOUR-BUCKET-NAME"
      prefix: "YOUR-PREFIX"  # e.g., "hudi/tables"
      region: "YOUR-REGION"  # e.g., "us-west-2"
      database_service_name: "YOUR-SERVICE-NAME"
      database_name: "YOUR-DATABASE-NAME"
      schema_name: "YOUR-SCHEMA-NAME"

defaults:
  idempotency: "update"

entities:
  - type: "database_service"
    discovery:
      source: "production-s3"

  - type: "database"
    discovery:
      source: "production-s3"

  - type: "database_schema"
    discovery:
      source: "production-s3"

  - type: "table"
    discovery:
      source: "production-s3"

execution:
  dry_run: false
  continue_on_error: true
```

---

## Command Line Usage

### Basic Run

```bash
python -m om_ingest.cli run --config config.yaml
```

### Run with Dry Run Override

```bash
python -m om_ingest.cli run --config config.yaml --dry-run
```

### Using Example Scripts

```bash
# Using the test script
python examples/test_ingestion.py --config examples/test_local_minio.yaml

# With dry run
python examples/test_ingestion.py --config examples/test_local_minio.yaml --dry-run
```

---

## Quick Checklist

Before running your configuration:

- [ ] OpenMetadata is running and accessible
- [ ] JWT token is set: `echo $OPENMETADATA_JWT_TOKEN`
- [ ] Data source is accessible (S3 bucket, credentials)
- [ ] Configuration file has correct entity names
- [ ] Tested with `dry_run: true` first
- [ ] Audit logging enabled (optional but recommended)

---

## Common Issues

### "JWT token is required"

**Fix:**
```bash
export OPENMETADATA_JWT_TOKEN="your-token"
```

### "Missing dependency: service.database.schema"

**Fix:** Make sure parent entities exist or discover them first:
```yaml
entities:
  # Add these before tables
  - type: "database_service"
    discovery:
      source: "your-source"
  - type: "database"
    discovery:
      source: "your-source"
  - type: "database_schema"
    discovery:
      source: "your-source"
```

### "Failed to connect to S3"

**For MinIO:**
```yaml
properties:
  endpoint_url: "http://localhost:9000"  # ← Add this
  aws_access_key_id: "minioadmin"
  aws_secret_access_key: "minioadmin"
```

**For AWS:**
- Check credentials
- Verify region is correct
- Ensure bucket name is correct

---

## Next Steps

- Read the [Configuration Guide](./CONFIGURATION_GUIDE.md) for detailed options
- See [Entity Discovery Modes](./ENTITY_DISCOVERY_MODES.md) for different approaches
- Check example configurations in `/examples` directory

---

**Need Help?** Check the logs in `audit_logs/` directory for detailed execution information.
