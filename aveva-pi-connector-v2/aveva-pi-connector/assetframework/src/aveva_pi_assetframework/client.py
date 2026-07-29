"""Thin PI Web API client — one function per real API call, nothing more.

This library does **only what the PI Web API itself does**: each public function is
a single HTTP request that returns the API's JSON verbatim. It contains no
multi-step orchestration, no paging loops, no tree-walking, and no opinions about
how tags map to assets — the caller composes those from these primitives.

Typical composition the *caller* writes (not us):

    AUTH = dict(basic_user=user, basic_password=pw)   # HTTP Basic against PI

    # tag name -> WebID (one call per tag, or bundle via batch())
    pt      = get_point(base, "PISRV", "Plant.Area.Unit1.Temp", **AUTH)
    web_id  = pt["WebId"]

    # ...or resolve many tags in ONE request with the Batch controller:
    reqs = {str(i): {"Method": "GET",
                     "Resource": f"{base}/points?path=" + quote(rf'\\PISRV\{t}', safe='')}
            for i, t in enumerate(tags)}
    resp = batch(base, reqs, **AUTH)          # {id: {Status, Content}}

    # ...walk an AF database yourself, mapping to YOUR model:
    db   = get_asset_database(base, "PISRV", "MyDB", **AUTH)
    els  = get_database_elements(base, db["WebId"], **AUTH)["Items"]
    for el in els:
        attrs = get_element_attributes(base, el["WebId"], **AUTH)["Items"]
        ...   # you decide asset_id/tag, recursion (get_child_elements), paging

Each function accepts `basic_user`+`basic_password` (HTTP Basic against PI) and an
optional `sess` (reuse one pooled session across calls). `base` is the PI Web API
root, e.g. `https://host/piwebapi`. (A `bearer`/`api_key` path also exists but is
reserved for the demo's OAuth-gated mock App — use Basic against real PI.)
"""

from __future__ import annotations

from ._http import request_json, session


def _s(sess, bearer, api_key, basic_user=None, basic_password=None, verify_tls=True):
    return sess if sess is not None else session(
        bearer, api_key, basic_user=basic_user, basic_password=basic_password,
        verify_tls=verify_tls)


def get_point(base: str, server: str, tag: str, *, bearer: str | None = None,
              api_key: str | None = None, basic_user: str | None = None,
              basic_password: str | None = None,
              verify_tls: bool = True, sess=None, timeout: int = 60) -> dict:
    """`GET /points?path=\\\\{server}\\{tag}` — the point object for one tag
    (incl. its `WebId`, `Path`, `EngineeringUnits`, …). One API call."""
    return request_json(_s(sess, bearer, api_key, basic_user, basic_password, verify_tls), "GET", f"{base}/points",
                        params={"path": rf"\\{server}\{tag}"}, timeout=timeout)


def batch(base: str, requests: dict, *, bearer: str | None = None,
          api_key: str | None = None, basic_user: str | None = None,
              basic_password: str | None = None,
              verify_tls: bool = True, sess=None, timeout: int = 60) -> dict:
    """`POST /batch` — run many sub-requests in one HTTP call (Batch controller).

    `requests` is the PI batch body: a dict keyed by string ids, each value
    `{"Method", "Resource", ...}`. Returns PI's response dict keyed by the same
    ids, each `{"Status", "Headers", "Content"}`. Passed through verbatim — the
    caller builds the sub-requests and interprets the results."""
    return request_json(_s(sess, bearer, api_key, basic_user, basic_password, verify_tls), "POST", f"{base}/batch",
                        json_body=requests, timeout=timeout)


def get_asset_database(base: str, server: str, database: str, *, bearer: str | None = None,
                       api_key: str | None = None, basic_user: str | None = None,
              basic_password: str | None = None,
              verify_tls: bool = True, sess=None, timeout: int = 60) -> dict:
    """`GET /assetdatabases?path=\\\\{server}\\{database}` — one AF database object."""
    return request_json(_s(sess, bearer, api_key, basic_user, basic_password, verify_tls), "GET", f"{base}/assetdatabases",
                        params={"path": rf"\\{server}\{database}"}, timeout=timeout)


def get_database_elements(base: str, database_web_id: str, *, bearer: str | None = None,
                          api_key: str | None = None, basic_user: str | None = None,
              basic_password: str | None = None,
              verify_tls: bool = True, sess=None, timeout: int = 60,
                          start_index: int = 0, max_count: int = 1000) -> dict:
    """`GET /assetdatabases/{web_id}/elements` — a database's top-level elements
    (one page). Pass `start_index`/`max_count` to page; the caller loops."""
    return request_json(_s(sess, bearer, api_key, basic_user, basic_password, verify_tls), "GET",
                        f"{base}/assetdatabases/{database_web_id}/elements",
                        params={"startIndex": start_index, "maxCount": max_count}, timeout=timeout)


def get_child_elements(base: str, element_web_id: str, *, bearer: str | None = None,
                       api_key: str | None = None, basic_user: str | None = None,
              basic_password: str | None = None,
              verify_tls: bool = True, sess=None, timeout: int = 60,
                       start_index: int = 0, max_count: int = 1000) -> dict:
    """`GET /elements/{web_id}/elements` — an element's child elements (one page)."""
    return request_json(_s(sess, bearer, api_key, basic_user, basic_password, verify_tls), "GET",
                        f"{base}/elements/{element_web_id}/elements",
                        params={"startIndex": start_index, "maxCount": max_count}, timeout=timeout)


def get_element_attributes(base: str, element_web_id: str, *, bearer: str | None = None,
                           api_key: str | None = None, basic_user: str | None = None,
              basic_password: str | None = None,
              verify_tls: bool = True, sess=None, timeout: int = 60,
                           start_index: int = 0, max_count: int = 1000) -> dict:
    """`GET /elements/{web_id}/attributes` — an element's attributes (one page)."""
    return request_json(_s(sess, bearer, api_key, basic_user, basic_password, verify_tls), "GET",
                        f"{base}/elements/{element_web_id}/attributes",
                        params={"startIndex": start_index, "maxCount": max_count}, timeout=timeout)
