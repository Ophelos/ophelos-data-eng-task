"""Custom exceptions for the pipeline.

These provide structured error handling for different failure modes
in the ingestion process.
"""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


class ValidationError(PipelineError):
    """Raised when data fails schema or quality validation."""

    def __init__(self, message: str, column: str | None = None, **kwargs):
        super().__init__(message, kwargs)
        self.column = column


class IngestionError(PipelineError):
    """Raised when file reading or parsing fails."""

    def __init__(self, message: str, file_path: str | None = None, **kwargs):
        super().__init__(message, kwargs)
        self.file_path = file_path


class LoadError(PipelineError):
    """Raised when database operations fail."""

    def __init__(self, message: str, table: str | None = None, **kwargs):
        super().__init__(message, kwargs)
        self.table = table
