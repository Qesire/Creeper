const encoder = new TextEncoder();

class FabricHttpError extends Error {
  constructor(status, payload) {
    super(`Fabric HTTP ${status}: ${JSON.stringify(payload)}`);
    this.status = status;
    this.payload = payload;
  }
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map(canonicalJson).join(",") + "]";
  }
  const keys = Object.keys(value).sort();
  return (
    "{" +
    keys.map((key) => JSON.stringify(key) + ":" + canonicalJson(value[key])).join(",") +
    "}"
  );
}

function hex(bytes) {
  return Array.from(new Uint8Array(bytes), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

async function sha256Hex(bytes) {
  return hex(await crypto.subtle.digest("SHA-256", bytes));
}

async function hmacHex(secret, message) {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return hex(await crypto.subtle.sign("HMAC", key, encoder.encode(message)));
}

function nonceHex() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return hex(bytes);
}

async function signedPost(env, path, payload) {
  const body = canonicalJson(payload);
  const bodyBytes = encoder.encode(body);
  const timestamp = (Date.now() / 1000).toFixed(6);
  const nonce = nonceHex();
  const digest = await sha256Hex(bodyBytes);
  const canonical = ["POST", path, digest, timestamp, nonce].join("\n");
  const signature = await hmacHex(env.CREEPER_WORKER_SECRET, canonical);
  const response = await fetch(env.COORDINATOR_URL.replace(/\/$/, "") + path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Creeper-Worker": env.WORKER_ID,
      "X-Creeper-Timestamp": timestamp,
      "X-Creeper-Nonce": nonce,
      "X-Creeper-Signature": signature,
    },
    body,
  });
  let value = {};
  try {
    value = await response.json();
  } catch {
    value = { error: "NON_JSON_AUTHORITY_RESPONSE" };
  }
  if (!response.ok) {
    throw new FabricHttpError(response.status, value);
  }
  return value;
}

function providers(env) {
  const value = env.PROVIDERS;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("PROVIDERS JSON binding is required");
  }
  return value;
}

function descriptor(env) {
  return {
    worker_id: env.WORKER_ID,
    runtime_class: "cloudflare_worker",
    region: env.REGION || "cloudflare-global",
    architecture: "wasm",
    memory_bytes: Number(env.MEMORY_BYTES || 134217728),
    cpu_count: 1,
    network_class: "edge",
    capabilities: ["THIN_QUERY"],
    producers: ["ThinHistoricalQueryProducer"],
    allowed_providers: Object.keys(providers(env)).sort(),
    daily_egress_budget_bytes: Number(env.DAILY_EGRESS_BUDGET_BYTES || 0),
    protocol_version: "creeper-fabric-v1",
    edition_version: "0.1.0-dev",
  };
}

async function failTask(env, lease, error) {
  await signedPost(env, "/v1/tasks/fail", {
    task_id: lease.task_id,
    generation: lease.generation,
    error: String(error),
    retryable: true,
  });
}

async function finishTask(env, lease) {
  await signedPost(env, "/v1/tasks/finish", {
    task_id: lease.task_id,
    generation: lease.generation,
  });
}

function retryAfterSeconds(headers) {
  const raw = headers.get("Retry-After");
  if (!raw) return 0;
  const numeric = Number(raw);
  if (Number.isFinite(numeric)) return Math.max(0, numeric);
  const target = Date.parse(raw);
  if (!Number.isFinite(target)) return 0;
  return Math.max(0, (target - Date.now()) / 1000);
}

async function readBounded(response, maxBytes) {
  const declared = Number(response.headers.get("Content-Length") || "");
  if (Number.isFinite(declared) && declared > maxBytes) {
    if (response.body) await response.body.cancel();
    return { bytes: new Uint8Array(), count: 0, overflow: true };
  }
  if (!response.body) {
    return { bytes: new Uint8Array(), count: 0, overflow: false };
  }
  const reader = response.body.getReader();
  const chunks = [];
  let count = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      count += value.byteLength;
      if (count > maxBytes) {
        await reader.cancel();
        return { bytes: new Uint8Array(), count, overflow: true };
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const merged = new Uint8Array(count);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { bytes: merged, count, overflow: false };
}

function providerParams(config, hostname, year) {
  const url = new URL(config.endpoint);
  url.searchParams.set("url", `http://${hostname}/`);
  url.searchParams.set("matchType", "host");
  url.searchParams.set("output", "json");
  url.searchParams.set("limit", "1");
  if (config.dialect === "arquivo") {
    url.searchParams.set("from", String(year));
    url.searchParams.set("to", String(year));
    url.searchParams.set("fields", "url,timestamp,status");
  } else {
    url.searchParams.set("from", `${year}0101000000`);
    url.searchParams.set("to", `${year}1231235959`);
    url.searchParams.set("fl", "timestamp,original,statuscode");
    url.searchParams.set("filter", "statuscode:[23][0-9][0-9]");
  }
  return url;
}

function parseRows(config, bytes) {
  const text = new TextDecoder().decode(bytes).trim();
  if (!text) return [];
  if (config.dialect === "arquivo") {
    const values = [];
    try {
      const parsed = JSON.parse(text);
      values.push(...(Array.isArray(parsed) ? parsed : [parsed]));
    } catch {
      for (const line of text.split(/\r?\n/)) {
        try {
          values.push(JSON.parse(line));
        } catch {}
      }
    }
    return values
      .filter((item) => item && typeof item === "object" && !Array.isArray(item))
      .map((item) => ({
        ...item,
        original: item.original ?? item.url,
        statuscode: item.statuscode ?? item.status,
      }));
  }
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed) || parsed.length === 0) return [];
  if (parsed.every((item) => item && typeof item === "object" && !Array.isArray(item))) {
    return parsed;
  }
  if (!Array.isArray(parsed[0])) return [];
  const header = parsed[0];
  return parsed.slice(1).filter(Array.isArray).map((row) => {
    const item = {};
    header.forEach((field, index) => {
      if (typeof field === "string" && index < row.length) item[field] = row[index];
    });
    return item;
  });
}

function acceptedRow(rows, hostname, year) {
  for (const row of rows) {
    const timestamp = String(row.timestamp ?? "");
    const original = String(row.original ?? row.url ?? "");
    const status = String(row.statuscode ?? row.status ?? "");
    let originalHost = "";
    try {
      originalHost = new URL(original).hostname.toLowerCase().replace(/\.$/, "");
    } catch {
      continue;
    }
    if (
      timestamp.length >= 4 &&
      Number(timestamp.slice(0, 4)) === year &&
      originalHost === hostname &&
      (status.startsWith("2") || status.startsWith("3"))
    ) {
      return row;
    }
  }
  return null;
}

async function providerPermit(env, lease, provider) {
  const requestId = crypto.randomUUID();
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const value = await signedPost(env, "/v1/providers/permit", {
        provider,
        task_id: lease.task_id,
        generation: lease.generation,
        permit_request_id: requestId,
        ttl_seconds: 30,
      });
      return value.permit ?? null;
    } catch (error) {
      if (error instanceof FabricHttpError || attempt === 2) throw error;
      await new Promise((resolve) => setTimeout(resolve, 100 * (attempt + 1)));
    }
  }
  return null;
}

async function reportPermit(env, permit, statusCode, responseBytes, headers) {
  let cooldown = 0;
  if (statusCode === 429 || statusCode === 503) {
    cooldown = Math.max(2, retryAfterSeconds(headers));
  }
  await signedPost(env, "/v1/providers/report", {
    permit_id: permit.permit_id,
    status_code: statusCode,
    cooldown_seconds: cooldown,
    response_bytes: responseBytes,
  });
}

async function runThinTask(env, lease) {
  const coverage = lease.work?.coverage ?? {};
  if (
    lease.work?.producer !== "ThinHistoricalQueryProducer" ||
    coverage.thin_eligible !== true ||
    Number(coverage.max_provider_requests) !== 1 ||
    Number(coverage.year_from) !== Number(coverage.year_to)
  ) {
    throw new Error("Authority returned non-thin work to cloudflare runtime");
  }

  const provider = String(coverage.provider || "");
  const config = providers(env)[provider];
  if (!config) throw new Error(`provider not configured: ${provider}`);
  const hostname = String(lease.work.input_identity || "").toLowerCase().replace(/\.$/, "");
  const year = Number(coverage.year_from);
  const maxBytes = Number(coverage.estimated_response_bytes);
  if (!(year >= 1996 && year <= 2001 && maxBytes > 0 && maxBytes <= 262144)) {
    throw new Error("invalid thin work coverage");
  }

  const permit = await providerPermit(env, lease, provider);
  if (!permit) {
    await failTask(env, lease, "provider budget wait");
    return { status: "budget_wait" };
  }

  let response;
  let body = { bytes: new Uint8Array(), count: 0, overflow: false };
  try {
    response = await fetch(providerParams(config, hostname, year), {
      method: "GET",
      redirect: "manual",
      headers: {
        "User-Agent": "Creeper-Fabric/0.1 cloudflare-thin",
        Accept: "application/json",
        "Accept-Encoding": "gzip, deflate",
      },
    });
    body = await readBounded(response, maxBytes);
  } catch (error) {
    await reportPermit(env, permit, null, body.count, new Headers());
    throw error;
  }

  await reportPermit(env, permit, response.status, body.count, response.headers);

  if (response.status === 429 || response.status === 503 || response.status >= 500) {
    throw new Error(`provider transient HTTP ${response.status}`);
  }
  if (response.status >= 300 || body.overflow) {
    await finishTask(env, lease);
    return { status: body.overflow ? "oversize" : "no_positive" };
  }

  const row = acceptedRow(parseRows(config, body.bytes), hostname, year);
  if (!row) {
    await finishTask(env, lease);
    return { status: "no_positive" };
  }

  const locator = `${provider}:${hostname}:${year}:thin`;
  const probe = await signedPost(env, "/v1/results/hy-probe", {
    task_id: lease.task_id,
    generation: lease.generation,
    probes: [{ hostname, year, locator }],
  });
  const decision = Array.isArray(probe.decisions) ? probe.decisions[0] : null;
  if (decision?.status === "NEED_FULL_EVIDENCE") {
    const canonicalRow = canonicalJson(row);
    const payloadHash = await sha256Hex(encoder.encode(canonicalRow));
    await signedPost(env, "/v1/results/hy-full", {
      task_id: lease.task_id,
      generation: lease.generation,
      evidence: [
        {
          hostname,
          year,
          evidence_class: "exact_host_cdx_capture",
          source: provider,
          timestamp: String(row.timestamp || ""),
          locator,
          original_url: String(row.original ?? row.url ?? ""),
          provider: "wayback",
          policy_version: "fabric-thin-positive-v1",
          payload_hash: payloadHash,
          extraction_method: "fabric_cloudflare_thin_exact_year",
        },
      ],
    });
  }

  await finishTask(env, lease);
  return { status: decision?.status || "positive" };
}

async function runOnce(env) {
  if (!env.CREEPER_WORKER_SECRET) throw new Error("CREEPER_WORKER_SECRET secret is required");
  await signedPost(env, "/v1/workers/register", descriptor(env));
  await signedPost(env, "/v1/heartbeat", {});
  const claimed = await signedPost(env, "/v1/tasks/claim", { lease_seconds: 45 });
  const lease = claimed.task;
  if (!lease) return { status: "idle" };

  try {
    return await runThinTask(env, lease);
  } catch (error) {
    try {
      await failTask(env, lease, `${error?.name || "Error"}: ${error?.message || error}`);
    } catch {}
    throw error;
  }
}

export default {
  async scheduled(_controller, env, _ctx) {
    await runOnce(env);
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return Response.json({
        status: "ok",
        edition: "Creeper Fabric",
        protocol_version: "creeper-fabric-v1",
        runtime_class: "cloudflare_worker",
      });
    }
    if (url.pathname === "/meta") {
      return Response.json({
        worker_id: env.WORKER_ID,
        region: env.REGION || "cloudflare-global",
        allowed_providers: Object.keys(providers(env)).sort(),
      });
    }
    return new Response("Not Found", { status: 404 });
  },
};
