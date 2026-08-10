"""Shared HTTP layer (VENDORED — private copy).

Pooled keep-alive sessions + a retrying JSON request helper for the PI Web API.
`requests` is imported inside functions so the module pickles cleanly to Spark
executors without a hard top-level dependency.

NOTE: this file is intentionally duplicated between the `aveva-pi-timeseries`
(connector) and `aveva-pi-assetframework` (library) wheels so the two install
independently with zero cross-dependency. Keep the two copies in sync — a change
here (e.g. retry logic) must be applied in both.
"""

from __future__ import annotations

import datetime as dt

# --------------------------------------------------------------------------- #
# AVEVA PI Web API documented limits (PI Web API 2023 SP2 reference).
# --------------------------------------------------------------------------- #
PI_MAX_RETURNED_ITEMS = 150_000
PI_DEFAULT_MAX_COUNT = 10_000
PI_DEFAULT_PARTITION_CONCURRENCY = 8
PI_DEFAULT_WEBIDS_PER_CALL = 50
# How many times a window may be halved when PI truncates at maxCount. Each split doubles
# the call count, so this bounds the blow-up: 12 splits = up to 4,096 sub-windows, enough
# to take a 3,000 s window down to under a second.
PI_MAX_WINDOW_SPLITS = 12
# `recorded` has no interval to size a window from, so the span is derived from an assumed
# archive rate. 1 value/sec/tag is a deliberately CONSERVATIVE default: too-wide windows
# are recovered by truncation detection (a re-read), whereas too-narrow ones only cost
# extra calls. Override with the `assumed_values_per_second` option when the real rate is
# known — § 1 of notebooks/benchmark_fanout.py measures it.
PI_ASSUMED_VALUES_PER_SECOND = 1.0
# Retriable signals. 429 = documented per-IP rate limit (default 1,000 req/s;
# the search controller has a *tighter* 50/s limit). 5xx = transient.
PI_RETRY_STATUS = (429, 500, 502, 503, 504)


def session(bearer_token: str | None, api_key: str | None, pool_maxsize: int = 32,
            *, basic_user: str | None = None, basic_password: str | None = None,
            verify_tls: bool = True):
    """A pooled keep-alive session. Connection reuse matters: the docs warn
    against 'creating new HTTP connections per request'.

    Auth (choose one): `bearer_token` -> `Authorization: Bearer`; `basic_user` +
    `basic_password` -> HTTP Basic (sent pre-emptively, so it works even if the
    server advertises only Negotiate in its WWW-Authenticate challenge — see
    RFC 9110 §11.6.2: Authorization is used "usually, but not necessarily, after
    receiving a 401"); `api_key` -> `X-API-Key`.

    `verify_tls` — validate the server certificate (default True). Set False ONLY as
    a diagnostic against an internal-CA / self-signed PI when the CA isn't yet in the
    cluster trust store: it disables cert validation for this session and skips the
    hostname check. This exposes the connection (and the reusable Basic credential
    it carries) to MITM — prefer importing the CA and keeping verification on.

    Security: pass credentials from a secret store — never a literal. Basic sends a
    reusable AD credential on every request, so use TLS (verified) and a dedicated
    least-privilege account.
    """
    import requests
    from requests.adapters import HTTPAdapter

    s = requests.Session()
    if basic_user is not None:
        # requests.auth.HTTPBasicAuth attaches the Authorization: Basic header to
        # every request pre-emptively (no need to be offered Basic in a challenge).
        s.auth = requests.auth.HTTPBasicAuth(basic_user, basic_password or "")
    elif bearer_token:
        s.headers["Authorization"] = f"Bearer {bearer_token}"
    if api_key:
        s.headers["X-API-Key"] = api_key
    s.headers["Accept"] = "application/json"
    s.verify = verify_tls
    if not verify_tls:
        # silence the per-request InsecureRequestWarning spam when validation is off
        try:
            from urllib3.exceptions import InsecureRequestWarning
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        except Exception:
            pass
    adapter = HTTPAdapter(pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def request_json(session, method: str, url: str, *, params=None, json_body=None,
                 timeout: int = 60, retries: int = 4):
    """GET/POST returning parsed JSON, with retry + exponential backoff.

    Retries on (a) the documented throttling / transient HTTP statuses
    (429/5xx) and (b) transient *network* errors — ConnectionError, ReadTimeout,
    ChunkedEncodingError. Honours integer-seconds `Retry-After`; the HTTP-date
    form is not parsed and falls back to exponential backoff. Any other 4xx
    (400/413/404) is raised immediately.
    """
    import time

    import requests

    net_errors = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    )

    for attempt in range(retries):
        try:
            resp = session.request(method, url, params=params or None,
                                   json=json_body, timeout=timeout)
        except net_errors:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        if resp.status_code in PI_RETRY_STATUS and attempt < retries - 1:
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
            time.sleep(delay)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()  # pragma: no cover


def raise_on_web_exception(body: dict) -> dict:
    """Guard against a WebException carried on an HTTP 200.

    PI Web API can return HTTP 200 with a top-level `WebException` when the
    response stream failed mid-transfer. Raise so a truncated payload fails (and
    is retried) instead of being silently ingested/committed.
    """
    exc = body.get("WebException")
    if exc:
        status = exc.get("StatusCode", "?")
        errors = "; ".join(exc.get("Errors", []) or []) or "WebException on 200 response"
        raise RuntimeError(f"PI Web API WebException (StatusCode={status}): {errors}")
    return body


def iso(epoch_s: int) -> str:
    """UTC ISO-8601 with a trailing Z, as PI expects."""
    return dt.datetime.fromtimestamp(epoch_s, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def parse_watermark_epoch(s: str) -> int:
    """Parse an ISO watermark to a UTC epoch. A naive timestamp (no offset) is
    interpreted as UTC, so the window doesn't shift on a non-UTC driver."""
    d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return int(d.timestamp())


def now_epoch() -> int:
    return int(dt.datetime.now(tz=dt.timezone.utc).timestamp())


def chunk(items: list, size: int) -> list[list]:
    """Split a list into consecutive chunks of at most `size`."""
    size = max(1, size)
    return [items[i:i + size] for i in range(0, len(items), size)]
