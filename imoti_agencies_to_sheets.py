from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    repo_dir = Path(__file__).resolve().parent
    scraper_path = repo_dir / "scraper.py"

    cmd = [sys.executable, str(scraper_path), "--update-agencies"]
    if "--dry-run" in sys.argv[1:]:
        cmd.append("--dry-run")

    print("[DEPRECATED] imoti_agencies_to_sheets.py перенаправляет в scraper.py --update-agencies")
    completed = subprocess.run(cmd, cwd=str(repo_dir), check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
