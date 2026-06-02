from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class CliModule:
    module: str
    command: str
    description: str


CLI_MODULES = [
    CliModule(
        module="cli.data",
        command="python -m cli.data",
        description="Data validation, repair, Qdrant initialization, and synchronization.",
    ),
    CliModule(
        module="cli.worker",
        command="python -m cli.worker",
        description="Redis ML worker runner.",
    ),
    CliModule(
        module="cli.tasks",
        command="python -m cli.tasks",
        description="Redis task enqueueing and queue maintenance utilities.",
    ),
    CliModule(
        module="cli.ml",
        command="python -m cli.ml",
        description="Local ML pipeline execution utilities.",
    ),
]


def main(_argv: list[str] | None = None) -> int:
    """Print available CLI modules and their entry commands."""
    print(
        json.dumps(
            {
                "command": "cli",
                "modules": [asdict(item) for item in CLI_MODULES],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
