"""Energy portal (demo) - Databricks App entry point (`uvicorn app:app`). Reads only through the Lakebase Data API."""

import logging

from hhportal.server import create_app

logging.basicConfig(level=logging.INFO)
app = create_app()
