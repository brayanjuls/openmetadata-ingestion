"""S3 Hudi data source connector."""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

try:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None
    Config = None

try:
    import pandas as pd
except ImportError:
    pd = None

from om_ingest.config.schema import EntityConfig, EntityType, SourceType
from om_ingest.sources.base import DataSource, DataSourceError
from om_ingest.sources.registry import SourceRegistry

logger = logging.getLogger(__name__)


@SourceRegistry.register(SourceType.S3_HUDI)
class S3HudiConnector(DataSource):
    """
    S3 Hudi data lake connector.

    Discovers Apache Hudi tables from S3 buckets and extracts their metadata.

    Configuration properties:
    - bucket: S3 bucket name (required)
    - prefix: S3 prefix/path (optional, default: "")
    - region: AWS region (optional, default: from environment)
    - endpoint_url: Custom S3 endpoint URL (optional, for MinIO/localstack)
    - aws_access_key_id: AWS access key (optional, default: from environment)
    - aws_secret_access_key: AWS secret key (optional, default: from environment)
    - database_service_name: DatabaseService name (optional, default: "{bucket}_datalake")
    - database_name: Database name (optional, default: "{bucket}")
    - schema_name: Schema name (optional, default: "default")
    """

    def __init__(self, config):
        """Initialize S3 Hudi connector."""
        super().__init__(config)

        if boto3 is None:
            raise DataSourceError(
                "boto3 is required for S3 Hudi connector. Install with: pip install boto3"
            )

        # Extract configuration
        self.bucket = self.properties.get("bucket")
        if not self.bucket:
            raise DataSourceError("S3 bucket name is required in source configuration")

        self.prefix = self.properties.get("prefix", "")
        self.region = self.properties.get("region")
        self.endpoint_url = self.properties.get("endpoint_url")
        self.aws_access_key_id = self.properties.get("aws_access_key_id")
        self.aws_secret_access_key = self.properties.get("aws_secret_access_key")

        # Entity naming configuration
        self.database_service_name = self.properties.get(
            "database_service_name", f"{self.bucket}_datalake"
        )
        self.database_name = self.properties.get("database_name", self.bucket)
        self.schema_name = self.properties.get("schema_name", "default")

        self.s3_client = None
        self._discovered_tables: Optional[List[Dict[str, Any]]] = None

    @property
    def source_type(self) -> str:
        """Get source type identifier."""
        return "s3_hudi"

    @property
    def supported_entity_types(self) -> List[EntityType]:
        """Get supported entity types."""
        return [
            EntityType.DATABASE_SERVICE,
            EntityType.DATABASE,
            EntityType.DATABASE_SCHEMA,
            EntityType.TABLE,
        ]

    def connect(self) -> None:
        """
        Establish connection to S3.

        Raises:
            DataSourceError: If connection fails
        """
        try:
            # Build S3 client configuration
            client_config = {}

            # Region configuration
            if self.region:
                client_config["region_name"] = self.region

            # Custom endpoint for MinIO/localstack
            if self.endpoint_url:
                client_config["endpoint_url"] = self.endpoint_url
                # Use signature v4 for MinIO compatibility
                if Config:
                    client_config["config"] = Config(signature_version="s3v4")

            # AWS credentials
            if self.aws_access_key_id and self.aws_secret_access_key:
                client_config["aws_access_key_id"] = self.aws_access_key_id
                client_config["aws_secret_access_key"] = self.aws_secret_access_key

            self.s3_client = boto3.client("s3", **client_config)

            # Test connection by checking bucket access
            self.s3_client.head_bucket(Bucket=self.bucket)
            self._connected = True

            endpoint_info = f" at {self.endpoint_url}" if self.endpoint_url else ""
            logger.info(f"Connected to S3 bucket: {self.bucket}{endpoint_info}")

        except (BotoCoreError, ClientError) as e:
            raise DataSourceError(f"Failed to connect to S3: {e}")

    def disconnect(self) -> None:
        """Close S3 connection."""
        self.s3_client = None
        self._connected = False
        self._discovered_tables = None
        logger.info(f"Disconnected from S3 bucket: {self.bucket}")

    def validate_connection(self) -> bool:
        """
        Test that connection is working.

        Returns:
            True if connection is valid, False otherwise
        """
        if not self._connected or not self.s3_client:
            return False

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            return True
        except (BotoCoreError, ClientError):
            return False

    def discover_entities(
        self,
        entity_type: EntityType,
        filters: Optional[Dict[str, Any]] = None,
        include_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
    ) -> Iterator[EntityConfig]:
        """
        Discover entities from S3 Hudi tables.

        Args:
            entity_type: Type of entities to discover
            filters: Optional filters (not used for Hudi)
            include_pattern: Regex pattern for table names to include
            exclude_pattern: Regex pattern for table names to exclude

        Yields:
            EntityConfig objects for discovered entities
        """
        if not self._connected:
            raise DataSourceError("Not connected. Call connect() first.")

        # Discover tables lazily on first call
        if self._discovered_tables is None:
            self._discovered_tables = self._discover_hudi_tables()

        # Generate entities based on type
        if entity_type == EntityType.DATABASE_SERVICE:
            yield self._create_database_service_config()

        elif entity_type == EntityType.DATABASE:
            yield self._create_database_config()

        elif entity_type == EntityType.DATABASE_SCHEMA:
            yield self._create_schema_config()

        elif entity_type == EntityType.TABLE:
            # Apply filters
            for table_info in self._discovered_tables:
                table_name = table_info["name"]

                # Apply include pattern
                if include_pattern and not re.match(include_pattern, table_name):
                    continue

                # Apply exclude pattern
                if exclude_pattern and re.match(exclude_pattern, table_name):
                    continue

                yield self._create_table_config(table_info)

        else:
            logger.warning(f"Entity type {entity_type.value} not supported by S3 Hudi connector")

    def extract_schema(
        self, entity_type: EntityType, entity_identifier: str
    ) -> Dict[str, Any]:
        """
        Extract schema metadata for a Hudi table.

        Args:
            entity_type: Type of entity (must be TABLE)
            entity_identifier: Table name or S3 path

        Returns:
            Dictionary containing schema metadata
        """
        if entity_type != EntityType.TABLE:
            raise DataSourceError(
                f"Schema extraction only supported for tables, not {entity_type.value}"
            )

        # Find table info
        if self._discovered_tables is None:
            self._discovered_tables = self._discover_hudi_tables()

        table_info = None
        for table in self._discovered_tables:
            if table["name"] == entity_identifier or table["path"] == entity_identifier:
                table_info = table
                break

        if not table_info:
            raise DataSourceError(f"Table not found: {entity_identifier}")

        return table_info["schema"]

    def fetch_sample_data(
        self,
        entity_type: EntityType,
        entity_identifier: str,
        sample_size: Optional[int] = None,
    ) -> Any:
        """
        Fetch sample data from a Hudi table.

        Args:
            entity_type: Type of entity (must be TABLE)
            entity_identifier: Table name or S3 path
            sample_size: Number of rows to sample (default: 1000)

        Returns:
            pandas DataFrame with sample data
        """
        if pd is None:
            raise DataSourceError(
                "pandas is required for data sampling. Install with: pip install pandas"
            )

        if entity_type != EntityType.TABLE:
            raise DataSourceError(
                f"Data sampling only supported for tables, not {entity_type.value}"
            )

        sample_size = sample_size or 1000

        # Find table info
        if self._discovered_tables is None:
            self._discovered_tables = self._discover_hudi_tables()

        table_info = None
        for table in self._discovered_tables:
            if table["name"] == entity_identifier or table["path"] == entity_identifier:
                table_info = table
                break

        if not table_info:
            raise DataSourceError(f"Table not found: {entity_identifier}")

        # Read sample parquet files
        # For Hudi tables, data is stored in parquet files
        # We'll read the first few parquet files up to sample_size rows
        try:
            # List parquet files in the table path
            table_path = table_info["path"]
            parquet_files = self._list_parquet_files(table_path)

            if not parquet_files:
                logger.warning(f"No parquet files found for table: {entity_identifier}")
                return pd.DataFrame()

            # Read first parquet file(s) to get sample
            # Note: This is a simplified implementation
            # In production, you'd want to use PyArrow or similar for better Hudi support
            dfs = []
            total_rows = 0

            for parquet_file in parquet_files[:5]:  # Limit to first 5 files
                if total_rows >= sample_size:
                    break

                # Download parquet file to temp location
                local_path = f"/tmp/hudi_sample_{Path(parquet_file).name}"
                self.s3_client.download_file(self.bucket, parquet_file, local_path)

                # Read with pandas
                df = pd.read_parquet(local_path)
                dfs.append(df)
                total_rows += len(df)

                # Clean up temp file
                Path(local_path).unlink()

            # Combine and sample
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                return combined_df.head(sample_size)
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to fetch sample data for {entity_identifier}: {e}")
            raise DataSourceError(f"Failed to fetch sample data: {e}")

    def _discover_hudi_tables(self) -> List[Dict[str, Any]]:
        """
        Discover Hudi tables by scanning S3 for .hoodie directories.

        Returns:
            List of table information dictionaries
        """
        # Ensure prefix ends with / for proper directory listing
        search_prefix = self.prefix
        if search_prefix and not search_prefix.endswith("/"):
            search_prefix = search_prefix + "/"

        logger.info(f"Discovering Hudi tables in s3://{self.bucket}/{search_prefix}")
        tables = []

        try:
            # List all objects with the prefix
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=search_prefix, Delimiter="/")

            # Look for .hoodie directories which indicate Hudi tables
            for page in pages:
                # Check common prefixes (directories)
                common_prefixes = page.get("CommonPrefixes", [])
                logger.debug(f"Found {len(common_prefixes)} directories at {search_prefix}")

                for prefix_obj in common_prefixes:
                    prefix_path = prefix_obj["Prefix"]
                    logger.debug(f"Checking directory: {prefix_path}")

                    # Check if this directory contains a .hoodie subdirectory
                    hoodie_path = f"{prefix_path}.hoodie/"
                    logger.debug(f"Looking for .hoodie at: {hoodie_path}")

                    if self._check_path_exists(hoodie_path):
                        # Found a Hudi table
                        table_name = self._extract_table_name(prefix_path)
                        logger.info(f"Found Hudi table: {table_name} at {prefix_path}")

                        # Extract schema
                        schema = self._extract_hudi_schema(prefix_path)

                        tables.append(
                            {
                                "name": table_name,
                                "path": prefix_path,
                                "schema": schema,
                                "s3_location": f"s3://{self.bucket}/{prefix_path}",
                            }
                        )
                    else:
                        logger.debug(f"No .hoodie directory found at {hoodie_path}")

            logger.info(f"Discovered {len(tables)} Hudi tables")
            return tables

        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error during discovery: {e}")
            raise DataSourceError(f"Failed to discover Hudi tables: {e}")

    def _check_path_exists(self, path: str) -> bool:
        """Check if an S3 path exists."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=path, MaxKeys=1
            )
            return "Contents" in response and len(response["Contents"]) > 0
        except (BotoCoreError, ClientError):
            return False

    def _extract_table_name(self, path: str) -> str:
        """Extract table name from S3 path."""
        # Remove trailing slash
        path = path.rstrip("/")
        # Get last component
        return Path(path).name

    def _extract_hudi_schema(self, table_path: str) -> Dict[str, Any]:
        """
        Extract schema from Hudi metadata.

        This is a simplified implementation. In production, you'd want to:
        1. Read the Hudi commit metadata
        2. Parse Avro schema from .hoodie/metadata
        3. Use PyHudi or similar for proper schema extraction

        For now, we'll try to infer from parquet files.
        """
        try:
            # List parquet files
            parquet_files = self._list_parquet_files(table_path)

            if not parquet_files:
                logger.warning(f"No parquet files found for table at {table_path}")
                return {"columns": []}

            # Download first parquet file to inspect schema
            first_parquet = parquet_files[0]
            local_path = f"/tmp/hudi_schema_{Path(first_parquet).name}"

            try:
                self.s3_client.download_file(self.bucket, first_parquet, local_path)

                # Read schema using pandas
                if pd is not None:
                    # Read just the first row to get schema (parquet is columnar, so this is fast)
                    df = pd.read_parquet(local_path)
                    columns = []

                    for col_name, dtype in df.dtypes.items():
                        # Skip Hudi metadata columns
                        if col_name.startswith("_hoodie_"):
                            continue

                        columns.append(
                            {
                                "name": col_name,
                                "dataType": self._map_pandas_type_to_om(dtype),
                                "dataTypeDisplay": str(dtype),
                            }
                        )

                    return {"columns": columns}
                else:
                    logger.warning("pandas not available, cannot extract schema")
                    return {"columns": []}

            finally:
                # Clean up temp file
                if Path(local_path).exists():
                    Path(local_path).unlink()

        except Exception as e:
            logger.error(f"Failed to extract schema from {table_path}: {e}")
            return {"columns": []}

    def _list_parquet_files(self, table_path: str) -> List[str]:
        """List all parquet files in a Hudi table path."""
        parquet_files = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=table_path)

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Include parquet files, exclude Hudi metadata
                    if key.endswith(".parquet") and "/.hoodie/" not in key:
                        parquet_files.append(key)

            return parquet_files

        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to list parquet files: {e}")
            return []

    def _map_pandas_type_to_om(self, dtype) -> str:
        """
        Map pandas dtype to OpenMetadata data type.

        Returns:
            OpenMetadata data type string
        """
        dtype_str = str(dtype)

        # Numeric types
        if dtype_str.startswith("int"):
            return "INT"
        elif dtype_str.startswith("float"):
            return "DOUBLE"
        elif dtype_str == "bool":
            return "BOOLEAN"

        # String types
        elif dtype_str == "object":
            return "STRING"

        # Temporal types
        elif dtype_str.startswith("datetime"):
            return "TIMESTAMP"
        elif dtype_str.startswith("timedelta"):
            return "INTERVAL"

        # Default
        else:
            return "STRING"

    def _create_database_service_config(self) -> EntityConfig:
        """Create DatabaseService entity configuration."""
        return EntityConfig(
            type=EntityType.DATABASE_SERVICE,
            name=self.database_service_name,
            properties={
                "service_type": "Datalake",
                "description": f"S3 Datalake service for bucket: {self.bucket}",
            },
        )

    def _create_database_config(self) -> EntityConfig:
        """Create Database entity configuration."""
        return EntityConfig(
            type=EntityType.DATABASE,
            name=self.database_name,
            properties={
                "service": self.database_service_name,
                "description": f"Database for S3 bucket: {self.bucket}",
            },
        )

    def _create_schema_config(self) -> EntityConfig:
        """Create DatabaseSchema entity configuration."""
        return EntityConfig(
            type=EntityType.DATABASE_SCHEMA,
            name=self.schema_name,
            properties={
                "service": self.database_service_name,
                "database": self.database_name,
                "description": f"Schema for Hudi tables in {self.bucket}",
            },
        )

    def _create_table_config(self, table_info: Dict[str, Any]) -> EntityConfig:
        """Create Table entity configuration from discovered table."""
        # Build columns list
        columns = []
        for col in table_info["schema"].get("columns", []):
            columns.append(
                {
                    "name": col["name"],
                    "dataType": col["dataType"],
                    "dataTypeDisplay": col.get("dataTypeDisplay", col["dataType"]),
                }
            )

        return EntityConfig(
            type=EntityType.TABLE,
            name=table_info["name"],
            properties={
                "service": self.database_service_name,
                "database": self.database_name,
                "database_schema": self.schema_name,
                "columns": columns,
                "tableType": "External",
                "description": f"Hudi table at {table_info['s3_location']}",
                "sourceUrl": table_info["s3_location"],
            },
        )
