from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Iterable


@dataclass(frozen=True)
class SensorEvent:
    event_id: str
    sensor_id: str
    event_time: str
    fill_percent: float
    temperature_c: float
    latitude: float
    longitude: float

    @property
    def date(self) -> str:
        return datetime.fromisoformat(self.event_time).date().isoformat()


def validate(row: dict[str, str]) -> tuple[SensorEvent | None, list[str]]:
    errors: list[str] = []
    for field in ("event_id", "sensor_id", "event_time", "fill_percent", "temperature_c"):
        if not row.get(field, "").strip():
            errors.append(f"{field} is required")

    try:
        datetime.fromisoformat(row.get("event_time", ""))
    except ValueError:
        errors.append("event_time must be ISO 8601")

    numeric: dict[str, float] = {}
    for field in ("fill_percent", "temperature_c", "latitude", "longitude"):
        try:
            numeric[field] = float(row.get(field, ""))
        except ValueError:
            errors.append(f"{field} must be numeric")

    if "fill_percent" in numeric and not 0 <= numeric["fill_percent"] <= 100:
        errors.append("fill_percent must be between 0 and 100")
    if "temperature_c" in numeric and not -30 <= numeric["temperature_c"] <= 100:
        errors.append("temperature_c must be between -30 and 100")
    if "latitude" in numeric and not -90 <= numeric["latitude"] <= 90:
        errors.append("latitude must be between -90 and 90")
    if "longitude" in numeric and not -180 <= numeric["longitude"] <= 180:
        errors.append("longitude must be between -180 and 180")

    if errors:
        return None, errors
    return SensorEvent(
        event_id=row["event_id"],
        sensor_id=row["sensor_id"],
        event_time=row["event_time"],
        fill_percent=numeric["fill_percent"],
        temperature_c=numeric["temperature_c"],
        latitude=numeric["latitude"],
        longitude=numeric["longitude"],
    ), []


def transform(rows: Iterable[dict[str, str]]) -> tuple[list[SensorEvent], list[dict]]:
    accepted: list[SensorEvent] = []
    rejected: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        event, errors = validate(row)
        event_id = row.get("event_id", "")
        if event and event_id in seen:
            errors = ["duplicate event_id"]
            event = None
        if event:
            accepted.append(event)
            seen.add(event.event_id)
        else:
            rejected.append({"row": row, "errors": errors})
    return accepted, rejected


def aggregate(events: Iterable[SensorEvent]) -> list[dict[str, str | int | float]]:
    groups: dict[tuple[str, str], list[SensorEvent]] = defaultdict(list)
    for event in events:
        groups[(event.date, event.sensor_id)].append(event)
    result = []
    for (date, sensor_id), values in sorted(groups.items()):
        result.append(
            {
                "date": date,
                "sensor_id": sensor_id,
                "event_count": len(values),
                "average_fill_percent": round(mean(v.fill_percent for v in values), 2),
                "max_fill_percent": max(v.fill_percent for v in values),
                "average_temperature_c": round(mean(v.temperature_c for v in values), 2),
                "collection_alerts": sum(v.fill_percent >= 85 for v in values),
            }
        )
    return result


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def run(input_path: Path, output_path: Path) -> dict[str, int]:
    with input_path.open(newline="", encoding="utf-8") as stream:
        raw = list(csv.DictReader(stream))
    accepted, rejected = transform(raw)

    write_jsonl(output_path / "bronze" / "events.jsonl", raw)
    write_jsonl(output_path / "silver" / "events.jsonl", (asdict(event) for event in accepted))
    write_jsonl(output_path / "quarantine" / "rejected.jsonl", rejected)

    gold = aggregate(accepted)
    gold_path = output_path / "gold" / "daily_metrics.csv"
    gold_path.parent.mkdir(parents=True, exist_ok=True)
    with gold_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(gold[0]) if gold else ["date", "sensor_id"])
        writer.writeheader()
        writer.writerows(gold)
    return {"raw": len(raw), "accepted": len(accepted), "rejected": len(rejected), "gold": len(gold)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a local medallion lakehouse")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    summary = run(args.input, args.output)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

