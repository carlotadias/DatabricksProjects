"""Test harness: make `hhportal_data` (src/) and the app's `hhportal` package (app/) importable."""

import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "app"))
