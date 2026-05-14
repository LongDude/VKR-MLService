from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ingestion.openalex_bootstrap.dto import OpenAlexBootstrapReportDTO


def report_to_dict(report: OpenAlexBootstrapReportDTO) -> dict[str, Any]:
    """Convert bootstrap report to a JSON-ready dictionary."""
    return report.model_dump(mode="json")


def write_report(path: str | Path, report: OpenAlexBootstrapReportDTO) -> None:
    """Write bootstrap report as UTF-8 JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report_to_dict(report), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


__all__ = ["report_to_dict", "write_report"]
