# S3 Hudi Connector

The S3 Hudi connector discovers Apache Hudi tables from S3 buckets and extracts their metadata for ingestion into OpenMetadata.

## Features

- **Automatic Discovery**: Scans S3 buckets for Hudi tables (identified by `.hoodie` directories)
- **Schema Extraction**: Reads parquet files to extract column names and data types
- **Data Sampling**: Supports fetching sample data for profiling
- **Flexible Configuration**: Supports AWS credentials, regions, and path filtering

## Configuration

### Required Properties

- `bucket`: S3 bucket name containing Hudi tables

### Optional Properties

- `prefix`: S3 prefix/path to scan (default: `""`)
- `region`: AWS region (default: from environment)
- `aws_access_key_id`: AWS access key (default: from environment/IAM)
- `aws_secret_access_key`: AWS secret key (default: from environment/IAM)
- `database_service_name`: DatabaseService name (default: `{bucket}_datalake`)
- `database_name`: Database name (default: `{bucket}`)
- `schema_name`: Schema name (default: `"default"`)

## Example Configuration

```yaml
sources:
  - name: "my-datalake"
    type: "s3_hudi"
    properties:
      bucket: "my-data-bucket"
      prefix: "hudi-tables/"
      region: "us-east-1"
      # Optional: Use environment variables for credentials
      # aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      # aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"

entities:
  - type: "table"
    discovery:
      source: "my-datalake"
      # Optional: filter tables
      include_pattern: "^prod_.*"
      exclude_pattern: "^test_.*"
```

## How It Works

1. **Connection**: Establishes connection to S3 using boto3
2. **Discovery**: Scans the specified bucket/prefix for directories containing `.hoodie` subdirectories
3. **Schema Extraction**: Reads the first parquet file from each discovered table to extract schema
4. **Entity Generation**: Creates EntityConfig objects for:
   - DatabaseService (Datalake type)
   - Database
   - DatabaseSchema
   - Tables (one per discovered Hudi table)

## Data Type Mapping

The connector maps pandas data types (read from parquet) to OpenMetadata types:

| Pandas Type | OpenMetadata Type |
|-------------|-------------------|
| int*        | INT               |
| float*      | DOUBLE            |
| bool        | BOOLEAN           |
| object      | STRING            |
| datetime*   | TIMESTAMP         |
| timedelta*  | INTERVAL          |

## Limitations

- **Schema Extraction**: Currently uses the first parquet file's schema. For production use, consider using PyHudi or reading Hudi commit metadata for more accurate schema information.
- **Data Sampling**: Limited to the first 5 parquet files. For large tables, this may not be representative.
- **Hudi Metadata Columns**: Columns starting with `_hoodie_` are automatically excluded from the schema.

## Dependencies

- `boto3`: AWS SDK for Python
- `pandas`: For reading parquet files and data sampling (optional for basic schema extraction)

Install with:
```bash
pip install boto3 pandas
```

## Future Enhancements

- Use PyHudi for proper Hudi metadata parsing
- Support for Hudi evolution history tracking
- Optimize data sampling for large tables
- Support for partitioned table discovery
- Cache discovered tables to avoid repeated S3 scans
