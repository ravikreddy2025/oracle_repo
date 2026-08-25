"""Alert dispatch over pluggable channels.

Channels are configured as ``kind:target`` strings (``email:oncall@example.com``,
``slack:#data-alerts``, ``webhook:https://...``) so a table can declare where it
shouts without the framework depending on any particular notifier.

An unroutable channel is logged loudly rather than raised: an alert that cannot
be delivered must not become a second failure that masks the first one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Protocol, Sequence

from .logger import StructuredLogger


class AlertEvent(str, Enum):
    TASK_FAILED = "task_failed"
    RECONCILIATION_MISMATCH = "reconciliation_mismatch"
    FRESHNESS_BREACH = "freshness_breach"
    EXPECTATION_FAILED = "expectation_failed"


@dataclass(frozen=True)
class Alert:
    event: AlertEvent
    subject: str
    body: str
    table_fqn: str | None = None
    env: str | None = None
    run_id: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "event": self.event.value,
            "subject": self.subject,
            "body": self.body,
            "table_fqn": self.table_fqn,
            "env": self.env,
            "run_id": self.run_id,
            **dict(self.details),
        }


@dataclass(frozen=True)
class Channel:
    kind: str
    target: str

    def __str__(self) -> str:
        return f"{self.kind}:{self.target}"


class ChannelParseError(ValueError):
    pass


def parse_channel(spec: str) -> Channel:
    """Parse ``kind:target``. The target may itself contain colons (URLs do)."""
    text = str(spec).strip()
    if ":" not in text:
        raise ChannelParseError(
            f"{spec!r} is not a channel; expected 'kind:target', e.g. 'email:oncall@example.com'"
        )
    kind, target = text.split(":", 1)
    kind, target = kind.strip().lower(), target.strip()
    if not kind or not target:
        raise ChannelParseError(f"{spec!r} is missing a kind or a target")
    return Channel(kind=kind, target=target)


class AlertSender(Protocol):
    """Delivers one alert to one target."""

    def send(self, channel: Channel, alert: Alert) -> None: ...


class LoggingSender:
    """Default sender: records the alert in the structured log.

    Databricks Workflows already delivers job-level email/webhook notifications,
    so the framework's own default is to make the alert *visible* rather than to
    reimplement delivery. Swap in a real sender per channel kind where the
    workspace does not already cover it.
    """

    def __init__(self, logger: StructuredLogger) -> None:
        self._logger = logger

    def send(self, channel: Channel, alert: Alert) -> None:
        self._logger.warning(
            f"ALERT {alert.event.value}: {alert.subject}",
            alert_channel=str(channel),
            **alert.to_payload(),
        )


@dataclass
class DispatchResult:
    delivered: list[Channel] = field(default_factory=list)
    failed: list[tuple[Channel, str]] = field(default_factory=list)
    unroutable: list[str] = field(default_factory=list)

    @property
    def attempted(self) -> int:
        return len(self.delivered) + len(self.failed)


class AlertDispatcher:
    """Routes alerts to senders by channel kind."""

    def __init__(
        self,
        logger: StructuredLogger,
        senders: Mapping[str, AlertSender] | None = None,
        *,
        default_sender: AlertSender | None = None,
    ) -> None:
        self._logger = logger
        self._senders: dict[str, AlertSender] = dict(senders or {})
        self._default = default_sender or LoggingSender(logger)

    def register(self, kind: str, sender: AlertSender) -> None:
        self._senders[kind.lower()] = sender

    def dispatch(self, alert: Alert, channels: Sequence[str]) -> DispatchResult:
        """Send to every configured channel. Never raises."""
        result = DispatchResult()
        for spec in channels:
            try:
                channel = parse_channel(spec)
            except ChannelParseError as exc:
                # Config problem, not a delivery problem -- say so plainly.
                self._logger.error(f"unroutable alert channel: {exc}", alert_channel=str(spec))
                result.unroutable.append(str(spec))
                continue
            sender = self._senders.get(channel.kind, self._default)
            try:
                sender.send(channel, alert)
                result.delivered.append(channel)
            except Exception as exc:  # a failed alert must not mask the failure it reports
                self._logger.error(
                    f"alert delivery failed on {channel}: {exc}",
                    alert_channel=str(channel),
                    error_type=type(exc).__name__,
                )
                result.failed.append((channel, str(exc)))
        return result


def build_failure_alert(
    *, table_fqn: str, env: str, run_id: str, error: BaseException, attempt: int
) -> Alert:
    return Alert(
        event=AlertEvent.TASK_FAILED,
        subject=f"[{env}] ingestion failed: {table_fqn}",
        body=f"{type(error).__name__}: {error}",
        table_fqn=table_fqn,
        env=env,
        run_id=run_id,
        details={"attempt": attempt, "error_type": type(error).__name__},
    )


def build_reconciliation_alert(
    *, table_fqn: str, env: str, run_id: str, failures: Sequence[Any]
) -> Alert:
    names = ", ".join(str(getattr(f, "check_name", f)) for f in failures)
    return Alert(
        event=AlertEvent.RECONCILIATION_MISMATCH,
        subject=f"[{env}] reconciliation failed: {table_fqn}",
        body=f"{len(failures)} check(s) failed: {names}",
        table_fqn=table_fqn,
        env=env,
        run_id=run_id,
        details={"failed_checks": [str(getattr(f, "check_name", f)) for f in failures]},
    )
