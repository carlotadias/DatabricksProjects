"""Data side of the HH portal Lakebase demo: synthetic data, UC silver, Lakebase gold and the daily refresh.

Everything this package produces is SYNTHETIC. Pure functions (SQL builders, date logic) are unit-tested; the
Spark and Postgres calls live in jobs/ and scripts/.
"""
