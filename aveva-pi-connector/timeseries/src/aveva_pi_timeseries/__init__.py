"""aveva-pi-timeseries — the AVEVA PI **time-series connector** for Spark.

A self-contained Spark DataSource that reads point values for a set of WebIDs and
returns `(web_id, timestamp, value)`. It does NOT resolve names or walk Asset
Framework — get WebIDs from the separate `aveva-pi-assetframework` library first.

    from aveva_pi_timeseries import PITimeSeriesSource
    spark.dataSource.register(PITimeSeriesSource)
    df = spark.read.format("aveva_pi_timeseries").options(web_ids=..., ...).load()
"""

from __future__ import annotations

from .reader import (
    TIMESERIES_SCHEMA,
    PITimeSeriesBatchReader,
    PITimeSeriesSource,
    PITimeSeriesStreamReader,
)

__version__ = "2.0.3"

__all__ = [
    "PITimeSeriesSource",
    "PITimeSeriesBatchReader",
    "PITimeSeriesStreamReader",
    "TIMESERIES_SCHEMA",
    "__version__",
]
