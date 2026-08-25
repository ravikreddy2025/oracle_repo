"""Structured JSON logging with bound run context.

Every line carries the run/table/env/stage it belongs to, so a failure in a
50-table run can be filtered to one table without grepping for a table name
that might also appear in an unrelated message. Context is bound once and
inherited by child loggers rather than repeated at each call site.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Mapping

LOGGER_NAME = "ingestion_framework"

# Keys the stdlib puts on every record; anything else the caller passed via
# `extra` is ours and belongs in the JSON payload.
_STANDARD_FIELDS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "message", "module",
        "msecs", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "taskName", "thread", "threadName",
    }
)


# Names the stdlib record already owns, or that the JSON payload uses for the
# log's own metadata. A caller's field by these names is renamed, not dropped.
_RESERVED_FIELD_NAMES = frozenset({"level", "message", "logger", "ts", "name", "args", "msg"})


class JsonFormatter(logging.Formatter):
    """Render a log record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_FIELDS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


class StructuredLogger:
    """A logger with immutable bound context.

    ``bind()`` returns a new logger rather than mutating this one, so a stage
    logger cannot leak its context back into the run logger.
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(LOGGER_NAME)
        self._context: dict[str, Any] = dict(context or {})

    @property
    def context(self) -> dict[str, Any]:
        return dict(self._context)

    def bind(self, **context: Any) -> "StructuredLogger":
        merged = {**self._context, **{k: v for k, v in context.items() if v is not None}}
        return StructuredLogger(self._logger, merged)

    # Positional-only parameters: a caller passing a structured field named
    # 'level' or 'message' is ordinary (a dependency level, a source message)
    # and must not collide with this method's own arguments.
    def _log(self, level: int, message: str, /, exc_info: Any = None, **fields: Any) -> None:
        # 'message' and 'level' are meaningful to the stdlib record, so user
        # fields are namespaced away from them rather than silently dropped.
        safe = {(f"field_{k}" if k in _RESERVED_FIELD_NAMES else k): v for k, v in fields.items()}
        self._logger.log(
            level, message, exc_info=exc_info, extra={**self._context, **safe}
        )

    def debug(self, message: str, /, **fields: Any) -> None:
        self._log(logging.DEBUG, message, **fields)

    def info(self, message: str, /, **fields: Any) -> None:
        self._log(logging.INFO, message, **fields)

    def warning(self, message: str, /, **fields: Any) -> None:
        self._log(logging.WARNING, message, **fields)

    def error(self, message: str, /, exc_info: Any = None, **fields: Any) -> None:
        self._log(logging.ERROR, message, exc_info=exc_info, **fields)


def configure_logging(level: str = "INFO", stream: Any = None) -> logging.Logger:
    """Attach the JSON formatter once. Safe to call repeatedly."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    # Databricks captures driver stdout; a second handler would double every line.
    for existing in list(logger.handlers):
        logger.removeHandler(existing)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_logger(level: str = "INFO", **context: Any) -> StructuredLogger:
    return StructuredLogger(configure_logging(level), context)
