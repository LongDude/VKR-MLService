from __future__ import annotations

import sys
from pathlib import Path

# _SRC_DIR = Path(__file__).resolve().parent
# _SRC_DIR_STR = str(_SRC_DIR)

# if _SRC_DIR_STR not in sys.path:
#     sys.path.insert(0, _SRC_DIR_STR)

__all__: list[str] = [
    "adapters",
    "cli",
    "core",
    "dto",
    "ml",
    "models",
    "repositories",
    "utils",
]
