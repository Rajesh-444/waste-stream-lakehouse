from __future__ import annotations

from dataclasses import dataclass


EXPECTED_FIELDS = {
    "event_id",
    "sensor_id",
    "event_time",
    "fill_percent",
    "temperature_c",
    "latitude",
    "longitude",
}


@dataclass(frozen=True)
class DataSafetyPolicy:
    """Minimize stored telemetry and block fields outside the data contract."""

    allow_precise_location: bool = False

    def prepare(self, row: dict[str, str]) -> tuple[dict[str, str], list[str]]:
        unexpected = sorted(key for key in row if key not in EXPECTED_FIELDS and row.get(key))
        errors = [f"unexpected field blocked: {key}" for key in unexpected]
        safe = {key: row.get(key, "") for key in EXPECTED_FIELDS}
        if not self.allow_precise_location:
            for field in ("latitude", "longitude"):
                try:
                    safe[field] = f"{float(safe[field]):.2f}"
                except (TypeError, ValueError):
                    pass
        return safe, errors

