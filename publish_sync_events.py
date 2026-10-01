"""Publish compact GitHub Actions sync outcomes to the site's Analytics Engine.

This does not read or write D1. GitHub retains the full reports; the analytics
event holds only status, counts, a short reason, and the workflow run ID.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import requests


EVENT_URL = "https://nhidrug.mingtc.com/api/sync/event"


def event_from_report(source: str, path: Path, run_id: str) -> dict:
    if path.exists():
        report = json.loads(path.read_text(encoding="utf-8"))
        status = report.get("status", "failure")
        if status not in {
            "success", "no_changes", "deferred_quota", "deferred_data_anomaly",
            "failure", "dry_run", "skipped",
        }:
            status = "failure"
        return {
            "source": source,
            "status": status,
            "run_id": run_id,
            "reason": str(report.get("reason", ""))[:200],
            "total_records": int(report.get("total_records") or 0),
            "changed": int(report.get("changed") or 0),
            "inserted": int(report.get("inserted") or 0),
            "updated": int(report.get("updated") or 0),
            "deleted": int(report.get("deleted") or 0),
        }
    return {
        "source": source, "status": "failure", "run_id": run_id,
        "reason": "工作步驟未產生報告，請查看 GitHub Actions 日誌",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nhi", action="store_true")
    parser.add_argument("--ingredients", action="store_true")
    args = parser.parse_args()
    token = os.environ.get("TFDA_SYNC_TOKEN")
    if not token:
        print("::warning::TFDA_SYNC_TOKEN unavailable; update events were not published")
        return 0
    run_id = "-".join(filter(None, [os.environ.get("GITHUB_RUN_ID", ""), os.environ.get("GITHUB_RUN_ATTEMPT", "")]))
    targets = []
    if args.nhi:
        targets.append(("nhi", Path("upload_report.json")))
    if args.ingredients:
        targets.append(("tfda_ingredients", Path("ingredient_profile_report.json")))
    for source, path in targets:
        event = event_from_report(source, path, run_id)
        try:
            response = requests.post(
                EVENT_URL, headers={"Authorization": f"Bearer {token}"},
                json=event, timeout=15,
            )
            response.raise_for_status()
            print(f"Update event published: {source} {event['status']}")
        except requests.RequestException as exc:
            print(f"::warning::Update event publishing failed for {source}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
