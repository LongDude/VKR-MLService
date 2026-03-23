from __future__ import annotations

import os

from dotenv import load_dotenv

from pipeline_core import PipelineManager


def main() -> None:
    load_dotenv("./core/.env")
    config_path = os.getenv("PIPELINE_CONFIG_PATH", "./core/pipeline.yaml")
    profile = os.getenv("PIPELINE_PROFILE") or None
    manager = PipelineManager(config_path=config_path, profile=profile)
    manager.run_continuous()


if __name__ == "__main__":
    main()

