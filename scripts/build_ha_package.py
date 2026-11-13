#!/usr/bin/env python3
"""Build a Home Assistant-installable zip of custom_components/komeco_connect."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "komeco_connect"
DIST = ROOT / "dist"


def main() -> None:
    manifest = json.loads((COMPONENT / "manifest.json").read_text(encoding="utf-8"))
    version = manifest["version"]
    DIST.mkdir(parents=True, exist_ok=True)
    out = DIST / f"komeco_connect-{version}.zip"
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(COMPONENT.rglob("*")):
            if path.is_dir():
                continue
            if path.name == "__pycache__" or path.suffix == ".pyc":
                continue
            arc = Path("custom_components") / "komeco_connect" / path.relative_to(COMPONENT)
            zf.write(path, arcname=str(arc).replace("\\", "/"))
    print(out)


if __name__ == "__main__":
    main()
