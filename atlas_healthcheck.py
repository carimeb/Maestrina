#!/usr/bin/env python3
# =============================================================================
# atlas_healthcheck.py — Maestrina post-go-live health check, atlas_api mode
# Version: 2.0.0
# Output contract: maestrina_output.schema.json v2.1.0 (JSON Schema 2020-12)
#   https://github.com/carimeb/Maestrina
#
# WHAT THIS SCRIPT DOES
#   Collects operational metadata for ONE Atlas cluster through the MongoDB
#   Atlas Administration API v2 and writes a single JSON file that validates
#   against the committed Maestrina output schema. It is the "full" collection
#   mode: it fills the sections that the low-friction mongosh collector
#   (maestrina_collect.js) must declare unavailable (financial, alert_hygiene)
#   and derives workload rates from the Atlas 30-day metrics series.
#
# READ-ONLY BY CONSTRUCTION
#   Every request issued by this script is an HTTP GET. There is no code path
#   that performs POST/PUT/PATCH/DELETE — this is verifiable by inspecting
#   http_get_json(), the single point through which all requests flow.
#   The API key used should carry the "Project Read Only" role.
#
# COMPLIANCE
#   Only operational metadata and statistics are collected: counters, sizes,
#   rates, configuration flags, index names, alert configuration counts and
#   billing totals by SKU category. Never: document contents, query predicate
#   values, query shapes (Performance Advisor is consulted for COUNTS and
#   index names only), user accounts/credentials, or PII. The output embeds a
#   machine-readable compliance manifest.
#
# CONFIGURATION — environment variables only; this file is never edited, so
# the executed script remains hash-verifiable against the published copy.
#   MAESTRINA_ATLAS_PUBLIC_KEY    Atlas API public key  (prompted if absent)
#   MAESTRINA_ATLAS_PRIVATE_KEY   Atlas API private key (prompted if absent,
#                                 hidden input; never echoed, never persisted)
#   MAESTRINA_ATLAS_PROJECT       Atlas project (group) ID — required
#   MAESTRINA_ATLAS_CLUSTER       Cluster name; if unset and the project has
#                                 exactly one cluster it is auto-selected,
#                                 otherwise an interactive prompt lists them
#   MAESTRINA_WINDOW_DAYS         Metrics window in days (default 30)
#   MAESTRINA_PSEUDO=1            Pseudonymize hosts/namespaces/databases/
#                                 index names (sha256_trunc8, deterministic —
#                                 the same real name always maps to the same
#                                 pseudonym, so checkpoints remain joinable).
#                                 The cluster name is NEVER pseudonymized: it
#                                 is the navigation key in multi-cluster
#                                 engagements. The name map is written to a
#                                 separate local file that must never be sent
#                                 along with the output.
#   MAESTRINA_BASELINE_FILE       Path to the sizing baseline JSON produced by
#                                 the workload-fingerprinting engagement
#   MAESTRINA_OUT                 Output path override
#   MAESTRINA_DEBUG=1             Also write maestrina_debug_<ts>.json with
#                                 the full execution trace (no collected data)
#
# USAGE
#   export MAESTRINA_ATLAS_PROJECT=<projectId>
#   python3 atlas_healthcheck.py
#
# API VERSION PIN
#   All requests send Accept: application/vnd.atlas.2025-03-12+json
#   (a stable resource version of the versioned Atlas Administration API v2).
#   Authentication: HTTP Digest with a programmatic API key pair.
#
# THRESHOLDS / HEURISTICS
#   backup_to_compute_ratio_percent and the recommended-alerts checklist in
#   alert_hygiene are Maestrina's OWN heuristics — no public MongoDB document
#   defines them. Everything else reads values straight from the API.
#
# CREDIT
#   The Maestrina project's metadata-only approach to sharded-cluster
#   collection is credited to Felipe Scabral's msizer
#   (https://github.com/felipesscabral/msizer-mongodb). In atlas_api mode the
#   sharding internals (balancer/chunks/orphans) are not exposed by the Admin
#   API; run maestrina_collect.js through a mongos for the full block.
#
# DIAGNOSTICS (three layers, same design as maestrina_collect.js)
#   1) errors[] inside the output       — individual calls that failed
#   2) maestrina_debug_<ts>.json        — full trace, opt-in via MAESTRINA_DEBUG=1
#   3) maestrina_crash_<ts>.json        — automatic on fatal error, redacted
# =============================================================================

import base64
import getpass
import hashlib
import json
import os
import platform
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

COLLECTOR_NAME = "atlas_healthcheck.py"
COLLECTOR_VERSION = "2.0.0"
SCHEMA_VERSION = "2.1.0"
API_BASE = "https://cloud.mongodb.com"
API_ROOT = "/api/atlas/v2"
API_ACCEPT = "application/vnd.atlas.2025-03-12+json"
HTTP_TIMEOUT_SECONDS = 45

PHASES = ["init", "auth_check", "cluster_discovery", "measurements",
          "workload", "availability", "efficiency", "financial",
          "alert_hygiene", "sharding", "baseline", "assemble_output",
          "write_output", "done"]

# ---------------------------------------------------------------------------
# Runtime state (reset by run(); module-level so the crash handler sees it)
# ---------------------------------------------------------------------------
STATE = {}


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_compact():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# Redaction — strips credentials from ANY string before it can reach a file.
# ---------------------------------------------------------------------------
def redact(text):
    if text is None:
        return None
    s = str(text)
    for secret in STATE.get("secrets", []):
        if secret:
            s = s.replace(secret, "<redacted>")
    # connection strings and embedded userinfo
    s = re.sub(r"mongodb(\+srv)?://\S+", "mongodb<redacted-uri>", s)
    s = re.sub(r"://[^/\s:@]+:[^/\s@]+@", "://<redacted>@", s)
    # digest / api key material that may surface in library error text
    s = re.sub(r"(?i)(private[_\s-]?key\s*[=:]\s*)\S+", r"\1<redacted>", s)
    s = re.sub(r"(?i)(authorization:\s*)\S.*", r"\1<redacted>", s)
    return s


# ---------------------------------------------------------------------------
# Pseudonymization (sha256_trunc8, deterministic). Cluster name: NEVER.
# ---------------------------------------------------------------------------
def pseudo(name, kind):
    if name is None:
        return None
    if not STATE["opts"]["pseudo"]:
        return name
    key = str(name)
    m = STATE["pseudo_map"]
    if key not in m:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]
        m[key] = "%s_%s" % (kind, digest)
    return m[key]


def pseudo_ns(namespace):
    """Pseudonymize db.collection keeping the dot structure."""
    if namespace is None:
        return None
    if not STATE["opts"]["pseudo"]:
        return namespace
    parts = str(namespace).split(".", 1)
    if len(parts) == 2:
        return "%s.%s" % (pseudo(parts[0], "db"), pseudo(parts[1], "coll"))
    return pseudo(namespace, "ns")


# ---------------------------------------------------------------------------
# Trace / attempt — collection never aborts because one call failed.
# ---------------------------------------------------------------------------
def set_phase(phase):
    STATE["last_phase"] = phase


def attempt(source, fn, default=None):
    """Run fn(); on failure, record in errors[] (redacted) and return default.
    `source` must already use pseudonymized names where applicable."""
    t0 = time.time()
    try:
        result = fn()
        STATE["trace"].append({"source": source, "ok": True,
                               "ms": int((time.time() - t0) * 1000)})
        return result
    except Exception as exc:  # noqa: BLE001 — by design: absorb and record
        STATE["trace"].append({"source": source, "ok": False,
                               "ms": int((time.time() - t0) * 1000)})
        STATE["errors"].append({"source": source,
                                "detail": redact("%s: %s" % (type(exc).__name__, exc)),
                                "at": _now_iso()})
        return default


# ---------------------------------------------------------------------------
# HTTP layer — the single seam. All requests flow through here (GET only).
# The test harness replaces this function with stubbed responses.
# ---------------------------------------------------------------------------
_OPENER = None


def _get_opener():
    global _OPENER
    if _OPENER is None:
        mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        mgr.add_password(None, API_BASE,
                         STATE["opts"]["public_key"], STATE["opts"]["private_key"])
        _OPENER = urllib.request.build_opener(
            urllib.request.HTTPDigestAuthHandler(mgr))
    return _OPENER


def http_get_json(path, params=None):
    """HTTP GET against the Atlas Admin API v2. Returns parsed JSON.
    This is intentionally the ONLY function that touches the network."""
    query = ("?" + urllib.parse.urlencode(params, doseq=True)) if params else ""
    url = API_BASE + API_ROOT + path + query
    req = urllib.request.Request(url, method="GET",
                                 headers={"Accept": API_ACCEPT})
    with _get_opener().open(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_get_all(path, params=None):
    """Follow the {results, totalCount} envelope through all pages."""
    items = []
    page = 1
    per_page = 500
    while True:
        p = dict(params or {})
        p.update({"itemsPerPage": per_page, "pageNum": page})
        body = http_get_json(path, p)
        results = body.get("results", [])
        items.extend(results)
        if len(results) < per_page:
            return items
        page += 1


# ---------------------------------------------------------------------------
# Measurements helpers
# ---------------------------------------------------------------------------
def fetch_measurements(process_id, names, period, granularity="PT1H",
                       disk_partition=None):
    """Fetch a batch of measurements for one process. Returns
    {measurement_name: [(timestamp, value|None), ...]}. Batches of <=10 names;
    if a batch fails (e.g. one invalid name rejects the whole request), falls
    back to per-name requests so a single bad name never sinks its siblings."""
    if disk_partition:
        path = "/groups/%s/processes/%s/disks/%s/measurements" % (
            STATE["project_id"], process_id, disk_partition)
    else:
        path = "/groups/%s/processes/%s/measurements" % (
            STATE["project_id"], process_id)

    out = {}

    def _one_call(name_list):
        params = [("granularity", granularity), ("period", period)]
        for n in name_list:
            params.append(("m", n))
        body = http_get_json(path, params)
        for m in body.get("measurements", []):
            pts = [(dp.get("timestamp"), dp.get("value"))
                   for dp in (m.get("dataPoints") or [])]
            out[m.get("name")] = pts

    label_host = pseudo(process_id, "host")
    for i in range(0, len(names), 10):
        batch = list(names[i:i + 10])
        try:
            _one_call(batch)
        except Exception:  # noqa: BLE001 — fall back to per-name attempts
            for name in batch:
                attempt("measurements:%s:%s" % (label_host, name),
                        lambda n=name: _one_call([n]))
    return out


def series_avg(points):
    vals = [v for (_t, v) in (points or []) if isinstance(v, (int, float))]
    return (sum(vals) / len(vals)) if vals else None


def series_max(points):
    vals = [v for (_t, v) in (points or []) if isinstance(v, (int, float))]
    return max(vals) if vals else None


def series_min(points):
    vals = [v for (_t, v) in (points or []) if isinstance(v, (int, float))]
    return min(vals) if vals else None


def series_growth_percent(points):
    """Percent growth between the first and last non-null points."""
    vals = [v for (_t, v) in (points or []) if isinstance(v, (int, float))]
    if len(vals) < 2 or vals[0] in (0, None):
        return None
    return round((vals[-1] - vals[0]) / vals[0] * 100.0, 2)


def first_available(measurements, aliases):
    """Return the series for the first alias present (server naming varies
    across resource versions — same spirit as the mongosh collector's
    version-compatibility fallbacks)."""
    for name in aliases:
        if measurements.get(name):
            return measurements[name]
    return None


# ---------------------------------------------------------------------------
# Section builders — each returns a schema-exact dict.
# ---------------------------------------------------------------------------
def section(available, data, reason=None):
    return {"available": available,
            "unavailable_reason": reason,
            "data": data}


def build_workload(meas, top_namespaces):
    ops = {}
    for field, mname in [("insert", "OPCOUNTER_INSERT"), ("query", "OPCOUNTER_QUERY"),
                         ("update", "OPCOUNTER_UPDATE"), ("delete", "OPCOUNTER_DELETE"),
                         ("getmore", "OPCOUNTER_GETMORE"), ("command", "OPCOUNTER_CMD")]:
        avg = series_avg(meas.get(mname))
        ops[field] = round(avg, 3) if avg is not None else None

    reads = sum(v for v in [ops["query"], ops["getmore"]] if v is not None)
    writes = sum(v for v in [ops["insert"], ops["update"], ops["delete"]]
                 if v is not None)
    ratio = round(reads / writes, 2) if writes else None

    data_size = series_avg(meas.get("DB_DATA_SIZE_TOTAL"))
    storage_size = series_avg(meas.get("DB_STORAGE_TOTAL"))
    index_size = series_avg(first_available(
        meas, ["DB_INDEX_SIZE_TOTAL", "INDEX_SIZE_TOTAL"]))
    growth = series_growth_percent(meas.get("DB_DATA_SIZE_TOTAL"))
    working_set = series_avg(meas.get("CACHE_USED_BYTES"))

    return section(True, {
        "rates": {
            # Averages over the metrics window — the value the baseline band
            # is compared against at each checkpoint.
            "method": "atlas_30d_series",
            "ops_per_sec": ops,
            "read_write_ratio": ratio,
        },
        "storage": {
            "data_size_bytes": int(data_size) if data_size is not None else None,
            "storage_size_bytes": int(storage_size) if storage_size is not None else None,
            "index_size_bytes": int(index_size) if index_size is not None else None,
            # Unlike mongosh mode (single snapshot), the API window makes
            # growth directly measurable: first vs. last point of the series.
            "growth_30d_percent": growth,
        },
        "working_set_estimate_bytes": int(working_set) if working_set is not None else None,
        "top_namespaces": top_namespaces,
    })


def build_availability(meas, members, secondary_lags, elections, restarts):
    lags = [lag for (_m, lag) in secondary_lags if lag is not None]
    max_lag = round(max(lags), 2) if lags else None

    oplog_window_seconds = series_avg(first_available(
        meas, ["OPLOG_MASTER_TIME", "OPLOG_REPLICATION_LAG_TIME"]))
    oplog_window_hours = (round(oplog_window_seconds / 3600.0, 2)
                          if oplog_window_seconds is not None else None)

    conn_avg = series_avg(meas.get("CONNECTIONS"))
    tickets_r = series_min(meas.get("TICKETS_AVAILABLE_READS"))
    tickets_w = series_min(first_available(
        meas, ["TICKETS_AVAILABLE_WRITE", "TICKETS_AVAILABLE_WRITES"]))
    page_faults = series_avg(meas.get("EXTRA_INFO_PAGE_FAULTS"))

    disk_used = series_avg(first_available(
        meas, ["DISK_PARTITION_SPACE_PERCENT_USED",
               "DISK_PARTITION_SPACE_USED_PERCENT"]))
    disk_util_max = series_max(first_available(
        meas, ["DISK_PARTITION_UTILIZATION", "MAX_DISK_PARTITION_UTILIZATION"]))

    member_entries = []
    for m in members:
        entry = {"member": pseudo(m["id"], "host"), "state": m["state"],
                 "lag_seconds": None}
        for (mid, lag) in secondary_lags:
            if mid == m["id"]:
                entry["lag_seconds"] = round(lag, 2) if lag is not None else None
        member_entries.append(entry)

    return section(True, {
        "replication": {"max_lag_seconds": max_lag, "members": member_entries},
        "oplog_window_hours": oplog_window_hours,
        "elections_last_30d": elections,
        "restarts_last_30d": restarts,
        "tickets": {
            # Minimum over the window: the worst moment is what matters for
            # saturation analysis, not the average.
            "read_available": int(tickets_r) if tickets_r is not None else None,
            "write_available": int(tickets_w) if tickets_w is not None else None,
        },
        "connections": {
            "current": int(conn_avg) if conn_avg is not None else None,
            # Per-tier connection limits are not exposed by the Admin API.
            "available": None,
            "utilization_percent": None,
        },
        "disk": {
            "space_used_percent": round(disk_used, 2) if disk_used is not None else None,
            "util_percent_max_30d": round(disk_util_max, 2) if disk_util_max is not None else None,
        },
        "page_faults_per_sec": round(page_faults, 3) if page_faults is not None else None,
    })


def build_efficiency(meas, unused_indexes, pa_counts):
    keys_ratio = series_avg(meas.get("QUERY_TARGETING_SCANNED_PER_RETURNED"))
    objs_ratio = series_avg(meas.get("QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED"))
    scan_and_order = series_avg(meas.get("OPERATIONS_SCAN_AND_ORDER"))
    in_cache = series_avg(meas.get("CACHE_USED_BYTES"))

    return section(True, {
        "query_targeting": {
            "keys_scanned_per_returned": round(keys_ratio, 2) if keys_ratio is not None else None,
            "objects_scanned_per_returned": round(objs_ratio, 2) if objs_ratio is not None else None,
            "source": "atlas_metrics_30d",
        },
        "scan_and_order_per_sec": round(scan_and_order, 3) if scan_and_order is not None else None,
        "cache": {
            # WiredTiger configured cache size is not an Atlas measurement;
            # available in mongosh mode only.
            "configured_bytes": None,
            "in_cache_bytes": int(in_cache) if in_cache is not None else None,
            "dirty_percent": None,
            "fill_percent": None,
        },
        # In atlas_api mode, unused-index detection comes from the
        # Performance Advisor drop-index suggestions (index NAMES only —
        # never query shapes). accesses_* are mongosh-mode fields ($indexStats).
        "unused_indexes": unused_indexes,
        "performance_advisor": pa_counts,
    })


# Maestrina's own SKU categorization heuristic (public invoice SKUs vary;
# unmatched SKUs are counted as "other" rather than guessed).
_SKU_RULES = [
    ("backup_usd", ("BACKUP", "SNAPSHOT", "PIT_RESTORE")),
    ("data_transfer_usd", ("DATA_TRANSFER",)),
    ("storage_usd", ("STORAGE", "DISK", "IOPS")),
    ("compute_usd", ("INSTANCE", "SERVERLESS", "FLEX", "SEARCH")),
]


def categorize_sku(sku):
    s = (sku or "").upper()
    for category, tokens in _SKU_RULES:
        if any(tok in s for tok in tokens):
            return category
    return "other_usd"


def build_financial(line_items):
    cats = {"compute_usd": 0.0, "storage_usd": 0.0, "backup_usd": 0.0,
            "data_transfer_usd": 0.0, "other_usd": 0.0}
    daily = {}
    total_cents = 0
    for li in line_items:
        cents = li.get("totalPriceCents") or 0
        total_cents += cents
        cats[categorize_sku(li.get("sku"))] += cents / 100.0
        day = (li.get("startDate") or "")[:10]
        if day:
            daily[day] = daily.get(day, 0.0) + cents / 100.0

    compute = cats["compute_usd"]
    backup = cats["backup_usd"]
    # Maestrina's own heuristic ratio — no public MongoDB document defines it.
    ratio = round(backup / compute * 100.0, 2) if compute > 0 else None

    points = [["%sT00:00:00Z" % day, round(usd, 2)]
              for day, usd in sorted(daily.items())]
    series = []
    if points:
        series.append({"metric": "cost_daily_usd", "unit": "USD",
                       "granularity": "P1D", "points": points})

    return section(True, {
        "billing_month_to_date": {
            "total_usd": round(total_cents / 100.0, 2),
            "by_sku_category": {k: round(v, 2) for k, v in cats.items()},
        },
        "backup_to_compute_ratio_percent": ratio,
        "series": series,
    })


# Maestrina's own recommended-alerts checklist (own heuristic): the alert
# coverage a calibration-phase cluster should have. Matching is by metric /
# event type of the configured alerts.
_RECOMMENDED_ALERTS = [
    ("query_targeting_threshold", ("QUERY_TARGETING",)),
    ("replication_lag_threshold", ("OPLOG_SLAVE_LAG", "REPLICATION_LAG")),
    ("oplog_window_threshold", ("OPLOG_MASTER_TIME", "REPLICATION_OPLOG_WINDOW")),
    ("disk_space_used_threshold", ("DISK_PARTITION_SPACE",)),
    ("connections_threshold", ("CONNECTIONS",)),
    ("system_cpu_threshold", ("SYSTEM_NORMALIZED_CPU", "NORMALIZED_SYSTEM_CPU", "CPU")),
    ("pending_invoice_over_threshold", ("PENDING_INVOICE_OVER_THRESHOLD",)),
    ("backup_failure_alert", ("CPS_SNAPSHOT", "BACKUP",)),
    ("host_down_alert", ("HOST_DOWN", "NO_PRIMARY")),
]


def build_alert_hygiene(alert_configs):
    enabled = [a for a in alert_configs if a.get("enabled")]
    haystacks = []
    for a in enabled:
        haystacks.append("%s %s" % (a.get("eventTypeName", ""),
                                    a.get("metricThreshold", {}).get("metricName", "")
                                    if isinstance(a.get("metricThreshold"), dict) else ""))
    blob = " ".join(haystacks).upper()
    missing = [label for (label, tokens) in _RECOMMENDED_ALERTS
               if not any(tok in blob for tok in tokens)]
    return section(True, {
        "configured_count": len(enabled),
        "recommended_missing": missing,
    })


# ---------------------------------------------------------------------------
# Cluster / process discovery
# ---------------------------------------------------------------------------
def pick_cluster(clusters):
    wanted = STATE["opts"]["cluster_name"]
    names = [c.get("name") for c in clusters]
    if wanted:
        for c in clusters:
            if c.get("name") == wanted:
                return c
        raise RuntimeError("cluster '%s' not found in project; available: %s"
                           % (wanted, ", ".join(names)))
    if len(clusters) == 1:
        return clusters[0]
    if sys.stdin.isatty():
        print("Clusters in project:")
        for i, n in enumerate(names, 1):
            print("  %d) %s" % (i, n))
        choice = input("Select cluster [1-%d]: " % len(names)).strip()
        idx = int(choice) - 1
        if 0 <= idx < len(clusters):
            return clusters[idx]
        raise RuntimeError("invalid selection")
    raise RuntimeError("multiple clusters in project and no "
                       "MAESTRINA_ATLAS_CLUSTER set: %s" % ", ".join(names))


def extract_cluster_facts(cluster):
    """Tier/provider/region/autoscaling across old and new API shapes."""
    tier = provider = region = None
    auto_compute = auto_disk = None
    specs = cluster.get("replicationSpecs") or []
    if specs:
        region_cfgs = (specs[0].get("regionConfigs") or [{}])
        rc = region_cfgs[0]
        electable = rc.get("electableSpecs") or {}
        tier = electable.get("instanceSize")
        provider = rc.get("providerName") or rc.get("backingProviderName")
        region = rc.get("regionName")
        auto = rc.get("autoScaling") or {}
        if isinstance(auto.get("compute"), dict):
            auto_compute = auto["compute"].get("enabled")
        if isinstance(auto.get("diskGB"), dict):
            auto_disk = auto["diskGB"].get("enabled")
    ps = cluster.get("providerSettings") or {}
    tier = tier or ps.get("instanceSizeName")
    provider = provider or ps.get("providerName")
    region = region or ps.get("regionName")
    if auto_compute is None:
        auto = cluster.get("autoScaling") or {}
        if isinstance(auto.get("compute"), dict):
            auto_compute = auto["compute"].get("enabled")
        if auto_disk is None:
            auto_disk = auto.get("diskGBEnabled")
    return tier, provider, region, auto_compute, auto_disk


def processes_for_cluster(all_processes, cluster_name):
    """Match project processes to the selected cluster. Atlas exposes
    userAlias like '<clustername>-shard-00-01' (lowercased)."""
    prefix = cluster_name.lower()
    mine = []
    for p in all_processes:
        alias = (p.get("userAlias") or p.get("hostname") or "").lower()
        if alias.startswith(prefix + "-") or alias.startswith(prefix + "."):
            mine.append(p)
    return mine


def count_events(event_type, min_date, process_hostnames, cluster_name):
    events = api_get_all("/groups/%s/events" % STATE["project_id"],
                         {"eventTypeName": event_type, "minDate": min_date})
    count = 0
    hostset = {h.lower() for h in process_hostnames}
    for ev in events:
        host = (ev.get("hostname") or "").lower()
        cl = (ev.get("clusterName") or "")
        rs = (ev.get("replicaSetName") or "").lower()
        if (host in hostset
                or cl == cluster_name
                or rs.startswith(cluster_name.lower())):
            count += 1
    return count


# ---------------------------------------------------------------------------
# Diagnostics files
# ---------------------------------------------------------------------------
def diagnostic_payload():
    return {
        "collector": {"name": COLLECTOR_NAME, "version": COLLECTOR_VERSION},
        "generated_at": _now_iso(),
        "last_phase": STATE.get("last_phase"),
        "api_version_pin": API_ACCEPT,
        "python_version": platform.python_version(),
        "options": {
            "project_id": STATE.get("project_id"),
            "cluster_name": STATE.get("cluster_real_name"),
            "window_days": STATE["opts"]["window_days"],
            "pseudo": STATE["opts"]["pseudo"],
            "baseline_file_provided": bool(STATE["opts"]["baseline_file"]),
            "debug": STATE["opts"]["debug"],
        },
        "trace": STATE.get("trace", []),
        "errors": STATE.get("errors", []),
    }


def write_debug_file():
    path = os.path.join(STATE["opts"]["out_dir"],
                        "maestrina_debug_%s.json" % STATE["run_ts"])
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(diagnostic_payload(), fh, indent=2)
    print("[maestrina] debug trace written: %s" % path)


def write_crash_file(exc):
    payload = diagnostic_payload()
    payload["fatal"] = {
        "message": redact("%s: %s" % (type(exc).__name__, exc)),
        "stack": redact(traceback.format_exc()),
    }
    path = os.path.join(STATE["opts"]["out_dir"],
                        "maestrina_crash_%s.json" % STATE["run_ts"])
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print("[maestrina] FATAL at phase '%s' — crash report: %s"
              % (STATE.get("last_phase"), path), file=sys.stderr)
    except Exception:  # noqa: BLE001 — last resort: print redacted to stderr
        print(json.dumps(payload), file=sys.stderr)


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------
def read_options():
    env = os.environ
    public_key = env.get("MAESTRINA_ATLAS_PUBLIC_KEY", "").strip()
    private_key = env.get("MAESTRINA_ATLAS_PRIVATE_KEY", "").strip()
    if not public_key and sys.stdin.isatty():
        public_key = input("Atlas API public key: ").strip()
    if not private_key and sys.stdin.isatty():
        private_key = getpass.getpass("Atlas API private key (hidden): ").strip()
    if not public_key or not private_key:
        raise RuntimeError("API key pair missing: set MAESTRINA_ATLAS_PUBLIC_KEY "
                           "and MAESTRINA_ATLAS_PRIVATE_KEY (or run interactively)")
    project = env.get("MAESTRINA_ATLAS_PROJECT", "").strip()
    if not project:
        raise RuntimeError("MAESTRINA_ATLAS_PROJECT is required")
    out_override = env.get("MAESTRINA_OUT", "").strip() or None
    return {
        "public_key": public_key,
        "private_key": private_key,
        "project_id": project,
        "cluster_name": env.get("MAESTRINA_ATLAS_CLUSTER", "").strip() or None,
        "window_days": int(env.get("MAESTRINA_WINDOW_DAYS", "30")),
        "pseudo": env.get("MAESTRINA_PSEUDO", "") == "1",
        "debug": env.get("MAESTRINA_DEBUG", "") == "1",
        "baseline_file": env.get("MAESTRINA_BASELINE_FILE", "").strip() or None,
        "out_override": out_override,
        "out_dir": os.path.dirname(os.path.abspath(out_override)) if out_override else os.getcwd(),
    }


def run():
    global _OPENER
    _OPENER = None
    STATE.clear()
    STATE.update({"trace": [], "errors": [], "pseudo_map": {},
                  "run_ts": _ts_compact(), "last_phase": "init",
                  "opts": None, "secrets": []})
    set_phase("init")
    opts = read_options()
    STATE["opts"] = opts
    STATE["secrets"] = [opts["private_key"], opts["public_key"]]
    STATE["project_id"] = opts["project_id"]
    window_days = opts["window_days"]
    period = "P%dD" % window_days
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(days=window_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        # ---- auth_check -----------------------------------------------------
        set_phase("auth_check")
        group = attempt("auth_check:get_group",
                        lambda: http_get_json("/groups/%s" % opts["project_id"]))
        if group is None:
            raise RuntimeError("cannot read project %s — check API key pair, "
                               "its Project Read Only role and the API key "
                               "IP access list" % opts["project_id"])
        org_id = group.get("orgId")

        # ---- cluster_discovery ----------------------------------------------
        set_phase("cluster_discovery")
        clusters = attempt("cluster_discovery:list_clusters",
                           lambda: api_get_all("/groups/%s/clusters" % opts["project_id"]),
                           default=[])
        if not clusters:
            raise RuntimeError("no clusters visible in project %s" % opts["project_id"])
        cluster = pick_cluster(clusters)
        cluster_name = cluster.get("name")
        STATE["cluster_real_name"] = cluster_name
        mongodb_version = cluster.get("mongoDBVersion") or "unknown"
        cluster_type = (cluster.get("clusterType") or "REPLICASET").upper()
        topology = "sharded_cluster" if cluster_type == "SHARDED" else "replica_set"
        tier, provider, region, auto_compute, auto_disk = extract_cluster_facts(cluster)

        all_processes = attempt("cluster_discovery:list_processes",
                                lambda: api_get_all("/groups/%s/processes" % opts["project_id"]),
                                default=[])
        procs = processes_for_cluster(all_processes, cluster_name)
        if not procs:
            STATE["errors"].append({
                "source": "cluster_discovery:process_match",
                "detail": "no processes matched cluster name prefix; "
                          "per-process measurements will be unavailable",
                "at": _now_iso()})
        primary = next((p for p in procs
                        if (p.get("typeName") or "").upper() == "REPLICA_PRIMARY"), None)
        mongod_members = [p for p in procs
                          if (p.get("typeName") or "").upper().startswith("REPLICA")]
        reference = primary or (mongod_members[0] if mongod_members else None)
        reference_id = reference.get("id") if reference else None

        # ---- measurements ----------------------------------------------------
        set_phase("measurements")
        meas = {}
        if reference_id:
            wanted = ["OPCOUNTER_INSERT", "OPCOUNTER_QUERY", "OPCOUNTER_UPDATE",
                      "OPCOUNTER_DELETE", "OPCOUNTER_GETMORE", "OPCOUNTER_CMD",
                      "CONNECTIONS", "EXTRA_INFO_PAGE_FAULTS",
                      "TICKETS_AVAILABLE_READS", "TICKETS_AVAILABLE_WRITE",
                      "QUERY_TARGETING_SCANNED_PER_RETURNED",
                      "QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED",
                      "OPERATIONS_SCAN_AND_ORDER",
                      "CACHE_USED_BYTES", "CACHE_DIRTY_BYTES",
                      "DB_DATA_SIZE_TOTAL", "DB_STORAGE_TOTAL",
                      "DB_INDEX_SIZE_TOTAL", "OPLOG_MASTER_TIME"]
            meas = attempt("measurements:%s" % pseudo(reference_id, "host"),
                           lambda: fetch_measurements(reference_id, wanted, period),
                           default={})
            # disk measurements live under the partition sub-resource
            disks = attempt("measurements:list_disks",
                            lambda: http_get_json("/groups/%s/processes/%s/disks"
                                                  % (opts["project_id"], reference_id)),
                            default=None)
            partition = None
            if disks and disks.get("results"):
                partition = disks["results"][0].get("partitionName")
            if partition:
                disk_meas = attempt(
                    "measurements:disk:%s" % pseudo(reference_id, "host"),
                    lambda: fetch_measurements(
                        reference_id,
                        ["DISK_PARTITION_SPACE_PERCENT_USED",
                         "DISK_PARTITION_SPACE_USED_PERCENT",
                         "DISK_PARTITION_UTILIZATION",
                         "MAX_DISK_PARTITION_UTILIZATION"],
                        period, disk_partition=partition),
                    default={})
                meas.update(disk_meas or {})
        else:
            STATE["errors"].append({
                "source": "measurements",
                "detail": "no reference mongod process found for cluster; "
                          "measurement-based fields will be null",
                "at": _now_iso()})

        # ---- workload ---------------------------------------------------------
        set_phase("workload")
        top_namespaces = []
        ranked = attempt(
            "workload:collstats_namespaces",
            lambda: http_get_json("/groups/%s/clusters/%s/collStats/namespaces"
                                  % (opts["project_id"], cluster_name),
                                  {"clusterView": "PRIMARY"}))
        if ranked:
            for ns in (ranked.get("rankedNamespaces") or [])[:10]:
                name = ns if isinstance(ns, str) else ns.get("namespace")
                if name:
                    top_namespaces.append({"namespace": pseudo_ns(name),
                                           "data_size_bytes": None,
                                           "index_size_bytes": None,
                                           "document_count": None})
        workload = build_workload(meas, top_namespaces)

        # ---- availability -------------------------------------------------------
        set_phase("availability")
        members = [{"id": p.get("id"), "state":
                    "PRIMARY" if (p.get("typeName") or "").upper() == "REPLICA_PRIMARY"
                    else "SECONDARY"} for p in mongod_members]
        secondary_lags = []
        for p in mongod_members:
            if (p.get("typeName") or "").upper() == "REPLICA_SECONDARY":
                pid = p.get("id")
                lag_series = attempt(
                    "availability:lag:%s" % pseudo(pid, "host"),
                    lambda pid=pid: fetch_measurements(
                        pid, ["OPLOG_SLAVE_LAG_MASTER_TIME",
                              "REPLICATION_LAG"], period),
                    default={})
                lag = series_avg(first_available(
                    lag_series, ["OPLOG_SLAVE_LAG_MASTER_TIME", "REPLICATION_LAG"]))
                secondary_lags.append((pid, lag))
        hostnames = [p.get("hostname") or p.get("id", "").split(":")[0]
                     for p in procs]
        elections = attempt("availability:events:PRIMARY_ELECTED",
                            lambda: count_events("PRIMARY_ELECTED", window_start,
                                                 hostnames, cluster_name))
        restarts = attempt("availability:events:HOST_RESTARTED",
                           lambda: count_events("HOST_RESTARTED", window_start,
                                                hostnames, cluster_name))
        availability = build_availability(meas, members, secondary_lags,
                                          elections, restarts)

        # ---- efficiency ----------------------------------------------------------
        set_phase("efficiency")
        unused_indexes = []
        drop_suggestions = attempt(
            "efficiency:pa_drop_index_suggestions",
            lambda: http_get_json(
                "/groups/%s/clusters/%s/performanceAdvisor/dropIndexSuggestions"
                % (opts["project_id"], cluster_name)))
        drop_count = None
        if drop_suggestions is not None:
            entries = []
            for key in ("hiddenIndexes", "redundantIndexes", "unusedIndexes"):
                entries.extend(drop_suggestions.get(key) or [])
            drop_count = len(entries)
            for e in entries[:25]:
                ns = e.get("namespace") or "%s.%s" % (e.get("dbName", "?"),
                                                      e.get("collectionName", "?"))
                unused_indexes.append({
                    "namespace": pseudo_ns(ns),
                    "index": pseudo(e.get("index") or e.get("name") or "unknown",
                                    "idx"),
                    "accesses_ops": None,
                    "accesses_since": None,
                    "size_bytes": e.get("sizeBytes"),
                })
        suggested = attempt(
            "efficiency:pa_suggested_indexes",
            lambda: http_get_json(
                "/groups/%s/clusters/%s/performanceAdvisor/suggestedIndexes"
                % (opts["project_id"], cluster_name)))
        suggested_count = None
        if suggested is not None:
            content = suggested.get("content") or suggested
            idx_list = (content.get("suggestedIndexes")
                        if isinstance(content, dict) else None) or []
            suggested_count = len(idx_list)
        schema_advice = attempt(
            "efficiency:pa_schema_advice",
            lambda: http_get_json(
                "/groups/%s/clusters/%s/performanceAdvisor/schemaAdvice"
                % (opts["project_id"], cluster_name)))
        schema_count = None
        if schema_advice is not None:
            content = schema_advice.get("content") or {}
            recs = content.get("recommendations") if isinstance(content, dict) else None
            schema_count = len(recs) if isinstance(recs, list) else None
        pa_counts = {
            # Counts and index names only — query shapes are never collected.
            "suggested_indexes_count": suggested_count,
            "drop_index_suggestions_count": drop_count,
            "schema_suggestions_count": schema_count,
            "data_platform_suggestions_count": None,
        }
        efficiency = build_efficiency(meas, unused_indexes, pa_counts)

        # ---- financial ---------------------------------------------------------
        set_phase("financial")
        financial = section(False, None,
                            "pending invoice not readable with this API key "
                            "(organization-level read access required)")
        if org_id:
            invoice = attempt("financial:pending_invoice",
                              lambda: http_get_json("/orgs/%s/invoices/pending" % org_id))
            if invoice is not None:
                items = [li for li in (invoice.get("lineItems") or [])
                         if li.get("groupId") == opts["project_id"]
                         and (li.get("clusterName") in (None, cluster_name))]
                financial = build_financial(items)
        else:
            STATE["errors"].append({"source": "financial:org_id",
                                    "detail": "orgId not present in project document",
                                    "at": _now_iso()})

        # ---- alert_hygiene --------------------------------------------------------
        set_phase("alert_hygiene")
        alert_configs = attempt("alert_hygiene:list_alert_configs",
                                lambda: api_get_all("/groups/%s/alertConfigs"
                                                    % opts["project_id"]))
        if alert_configs is None:
            alert_hygiene = section(False, None,
                                    "alert configurations not readable with this API key")
        else:
            alert_hygiene = build_alert_hygiene(alert_configs)

        # ---- sharding ----------------------------------------------------------
        set_phase("sharding")
        sharding = None  # null == not sharded (invariant: distinct from unavailable)
        if topology == "sharded_cluster":
            shards_count = None
            specs = cluster.get("replicationSpecs") or []
            if specs and specs[0].get("numShards"):
                shards_count = specs[0]["numShards"]
            elif specs:
                # new API shape: one replicationSpec per shard
                shards_count = len(specs)
            sharding = {
                "shards_count": shards_count,
                "balancer_enabled": None,
                "balancer_running": None,
                "sharded_collections_count": None,
                "jumbo_chunks_count": None,
                "data_distribution": [],
                "orphaned_docs_total": None,
                "imbalance_percent": None,
            }
            STATE["errors"].append({
                "source": "sharding",
                "detail": "balancer/chunk/orphan internals are not exposed by "
                          "the Atlas Admin API; run maestrina_collect.js "
                          "through a mongos for the full sharding block",
                "at": _now_iso()})

        # ---- baseline ----------------------------------------------------------
        set_phase("baseline")
        baseline = None
        if opts["baseline_file"]:
            baseline = attempt("baseline:load_file",
                               lambda: json.load(open(opts["baseline_file"],
                                                      encoding="utf-8")))

        # ---- assemble_output ------------------------------------------------------
        set_phase("assemble_output")
        output = {
            "maestrina": {
                "schema_version": SCHEMA_VERSION,
                "collector": {"name": COLLECTOR_NAME,
                              "version": COLLECTOR_VERSION},
                "collection_mode": "atlas_api",
                "generated_at": _now_iso(),
                "collection_window": {
                    "start": window_start,
                    "end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "second_sample_offset_seconds": None,
                    "server_uptime_seconds": None,
                },
            },
            "compliance_manifest": {
                "statement": ("This file contains only operational metadata and "
                              "statistics (counters, sizes, rates, configuration "
                              "flags, billing totals by SKU category). It contains "
                              "no business data, no document contents, no query "
                              "predicate values, no query shapes, and no personally "
                              "identifiable information."),
                "categories_collected": [
                    "cluster_configuration", "historical_metrics_series",
                    "storage_and_index_sizes", "performance_advisor_summaries",
                    "billing_by_sku_category", "alert_configuration",
                    "connection_statistics"],
                "categories_excluded": [
                    "document_contents", "query_predicate_values",
                    "query_shapes", "user_accounts_and_credentials",
                    "application_business_data",
                    "personally_identifiable_information"],
                "pseudonymization": {
                    "enabled": opts["pseudo"],
                    "method": "sha256_trunc8" if opts["pseudo"] else None,
                    "applies_to": (["hosts", "namespaces", "database_names",
                                    "index_names"]
                                   if opts["pseudo"] else []),
                },
            },
            "target": {
                "cluster_name": cluster_name,  # NEVER pseudonymized
                "mongodb_version": mongodb_version,
                "topology": topology,
                "atlas": {
                    "project_id": opts["project_id"],
                    "instance_tier": tier,
                    "provider": provider,
                    "region": region,
                    "autoscaling_compute_enabled": auto_compute,
                    "autoscaling_disk_enabled": auto_disk,
                },
            },
            "baseline": baseline,
            "clusters": [{
                "name": cluster_name,
                "workload": workload,
                "availability": availability,
                "efficiency": efficiency,
                "financial": financial,
                "alert_hygiene": alert_hygiene,
                "sharding": sharding,
            }],
            "errors": STATE["errors"],
        }

        # ---- write_output ----------------------------------------------------------
        set_phase("write_output")
        out_path = opts["out_override"] or os.path.join(
            os.getcwd(), "maestrina_output_%s_%s.json"
            % (re.sub(r"[^A-Za-z0-9_-]", "_", cluster_name), STATE["run_ts"]))
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(output, fh, indent=2)
        print("[maestrina] output written: %s" % out_path)

        if opts["pseudo"] and STATE["pseudo_map"]:
            map_path = out_path.replace(".json", "") + ".pseudonym_map.json"
            with open(map_path, "w", encoding="utf-8") as fh:
                json.dump({"note": ("Real-name map. This file stays with whoever "
                                    "ran the collection and must NOT be sent "
                                    "with the output."),
                           "map": STATE["pseudo_map"]}, fh, indent=2)
            print("[maestrina] pseudonym map (keep local, do NOT send): %s" % map_path)

        if opts["debug"]:
            write_debug_file()
        set_phase("done")
        print("[maestrina] done — %d error(s) recorded in errors[]"
              % len(STATE["errors"]))
        return output

    except Exception as exc:  # noqa: BLE001 — fatal path: crash file + re-raise
        write_crash_file(exc)
        raise


if __name__ == "__main__":
    run()

# END OF FILE — atlas_healthcheck.py v2.0.0
