"""Error handling strategies and exceptions."""

import time
from typing import Callable, Optional, TypeVar

from om_ingest.config.schema import EntityType

T = TypeVar("T")


class IngestionError(Exception):
    """Base exception for ingestion errors."""

    def __init__(self, message: str, entity_type: Optional[EntityType] = None, entity_name: Optional[str] = None):
        """
        Initialize ingestion error.

        Args:
            message: Error message
            entity_type: Type of entity that failed
            entity_name: Name of entity that failed
        """
        self.entity_type = entity_type
        self.entity_name = entity_name
        super().__init__(message)

    def __str__(self) -> str:
        """String representation."""
        if self.entity_type and self.entity_name:
            return f"{self.entity_type.value} '{self.entity_name}': {super().__str__()}"
        elif self.entity_type:
            return f"{self.entity_type.value}: {super().__str__()}"
        else:
            return super().__str__()


class DependencyValidationError(IngestionError):
    """
    Raised when dependency validation fails.

    These errors should always fail-fast as they indicate
    broken dependency chains.
    """

    def __init__(
        self,
        message: str,
        entity_type: Optional[EntityType] = None,
        entity_name: Optional[str] = None,
        missing_dependency: Optional[str] = None,
    ):
        """
        Initialize dependency validation error.

        Args:
            message: Error message
            entity_type: Type of entity that failed
            entity_name: Name of entity that failed
            missing_dependency: FQN of missing dependency
        """
        self.missing_dependency = missing_dependency
        super().__init__(message, entity_type, entity_name)


class EntityProcessingError(IngestionError):
    """
    Raised when entity processing fails.

    These errors can be handled optimistically (continue processing)
    or fail-fast depending on configuration.
    """

    def __init__(
        self,
        message: str,
        entity_type: Optional[EntityType] = None,
        entity_name: Optional[str] = None,
        original_exception: Optional[Exception] = None,
    ):
        """
        Initialize entity processing error.

        Args:
            message: Error message
            entity_type: Type of entity that failed
            entity_name: Name of entity that failed
            original_exception: Original exception that caused this error
        """
        self.original_exception = original_exception
        super().__init__(message, entity_type, entity_name)


class EntityValidationError(IngestionError):
    """
    Raised when entity validation fails.

    These errors indicate configuration problems.
    """

    pass


class ConfigurationError(IngestionError):
    """
    Raised when configuration is invalid.

    These errors should always fail-fast.
    """

    pass


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
    ):
        """
        Initialize retry configuration.

        Args:
            max_attempts: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential backoff
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base

    def get_delay(self, attempt: int) -> float:
        """
        Get delay for the given attempt number.

        Args:
            attempt: Attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = self.initial_delay * (self.exponential_base**attempt)
        return min(delay, self.max_delay)


def retry_with_backoff(
    func: Callable[..., T],
    config: Optional[RetryConfig] = None,
    retryable_exceptions: tuple = (Exception,),
) -> T:
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        config: Retry configuration
        retryable_exceptions: Tuple of exception types to retry

    Returns:
        Result of function call

    Raises:
        Last exception if all retries fail
    """
    if config is None:
        config = RetryConfig()

    last_exception = None

    for attempt in range(config.max_attempts):
        try:
            return func()
        except retryable_exceptions as e:
            last_exception = e

            if attempt < config.max_attempts - 1:
                delay = config.get_delay(attempt)
                time.sleep(delay)
            else:
                # Last attempt failed, raise
                raise

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    else:
        raise RuntimeError("Retry failed with no exception")


class ErrorHandler:
    """
    Handles errors during entity processing.

    Implements different strategies based on error type.
    """

    def __init__(
        self,
        fail_fast_on_dependency: bool = True,
        continue_on_error: bool = True,
        retry_config: Optional[RetryConfig] = None,
    ):
        """
        Initialize error handler.

        Args:
            fail_fast_on_dependency: Fail immediately on dependency errors
            continue_on_error: Continue processing on entity errors
            retry_config: Retry configuration
        """
        self.fail_fast_on_dependency = fail_fast_on_dependency
        self.continue_on_error = continue_on_error
        self.retry_config = retry_config or RetryConfig()

    def should_fail_fast(self, error: Exception) -> bool:
        """
        Determine if we should fail fast for this error.

        Args:
            error: Exception that occurred

        Returns:
            True if should fail fast
        """
        # Always fail fast on dependency errors
        if isinstance(error, DependencyValidationError):
            return True

        # Always fail fast on configuration errors
        if isinstance(error, ConfigurationError):
            return True

        # For entity processing errors, check configuration
        if isinstance(error, EntityProcessingError):
            return not self.continue_on_error

        # For other errors, fail fast
        return True

    def handle_error(
        self,
        error: Exception,
        entity_type: Optional[EntityType] = None,
        entity_name: Optional[str] = None,
    ) -> None:
        """
        Handle an error according to strategy.

        Args:
            error: Exception that occurred
            entity_type: Type of entity being processed
            entity_name: Name of entity being processed

        Raises:
            Exception if should fail fast
        """
        # Wrap error if not already an IngestionError
        if not isinstance(error, IngestionError):
            error = EntityProcessingError(
                message=str(error),
                entity_type=entity_type,
                entity_name=entity_name,
                original_exception=error,
            )

        # Check if we should fail fast
        if self.should_fail_fast(error):
            raise error
        # Otherwise, error is logged but processing continues
