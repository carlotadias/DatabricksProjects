"""aveva-pi-assetframework — a thin PI Web API client library.

One function per real PI Web API call, returning the API's JSON verbatim. No
orchestration, no paging loops, no tree-walking, no tag/asset model — you compose
those from these primitives, then feed the resulting WebIDs to the
`aveva-pi-timeseries` connector.

    from aveva_pi_assetframework import get_point, batch, get_asset_database, \
        get_database_elements, get_child_elements, get_element_attributes

    pt = get_point("https://host/piwebapi", "PISRV", "Plant.Area.Unit1.Temp",
                   basic_user=user, basic_password=pw)
    web_id = pt["WebId"]
"""

from __future__ import annotations

from .client import (
    batch,
    get_asset_database,
    get_child_elements,
    get_database_elements,
    get_element_attributes,
    get_point,
)

__version__ = "3.0.2"

__all__ = [
    "get_point",
    "batch",
    "get_asset_database",
    "get_database_elements",
    "get_child_elements",
    "get_element_attributes",
    "__version__",
]
