#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

function parseArgs(argv) {
  const result = {};
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--scenario") {
      result.scenario = argv[index + 1];
      index += 1;
    } else if (arg === "--help" || arg === "-h") {
      result.help = true;
    }
  }
  return result;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isAbsoluteUrl(value) {
  return /^https?:\/\//i.test(value);
}

function appendQuery(urlString, query) {
  if (!query || typeof query !== "object") {
    return urlString;
  }

  const url = new URL(urlString);
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null) {
      continue;
    }
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

function mergeBody(userBody, stepBody) {
  if (userBody && typeof userBody !== "object") {
    return stepBody;
  }
  if (stepBody && typeof stepBody !== "object") {
    return stepBody;
  }
  return {
    ...(userBody || {}),
    ...(stepBody || {}),
  };
}

function buildCookieHeader(cookies) {
  if (!cookies || typeof cookies !== "object") {
    return undefined;
  }

  const pairs = [];
  for (const [key, value] of Object.entries(cookies)) {
    pairs.push(`${key}=${value}`);
  }
  return pairs.join("; ");
}

function percentile(sortedValues, p) {
  if (sortedValues.length === 0) {
    return null;
  }
  const index = Math.min(
    sortedValues.length - 1,
    Math.max(0, Math.ceil((p / 100) * sortedValues.length) - 1),
  );
  return sortedValues[index];
}

function validateScenario(scenario) {
  if (!scenario || typeof scenario !== "object") {
    throw new Error("Scenario must be a JSON object.");
  }
  if (!scenario.baseUrl || typeof scenario.baseUrl !== "string") {
    throw new Error("Scenario must include a string baseUrl.");
  }
  if (!Number.isInteger(scenario.concurrency) || scenario.concurrency < 1) {
    throw new Error("Scenario must include concurrency >= 1.");
  }
  if (!Number.isInteger(scenario.repeatPerWorker) || scenario.repeatPerWorker < 1) {
    throw new Error("Scenario must include repeatPerWorker >= 1.");
  }
  if (!Array.isArray(scenario.steps) || scenario.steps.length === 0) {
    throw new Error("Scenario must include at least one step.");
  }
}

async function runStep({ scenario, step, user, timeoutMs }) {
  const startedAt = Date.now();

  if (step.delayBeforeMs) {
    await sleep(step.delayBeforeMs);
  }

  const baseUrl = isAbsoluteUrl(step.path)
    ? step.path
    : new URL(step.path, scenario.baseUrl).toString();
  const url = appendQuery(baseUrl, step.query);

  const headers = {
    ...(user.headers || {}),
    ...(step.headers || {}),
  };

  const cookieHeader = buildCookieHeader(user.cookies);
  if (cookieHeader && !headers.Cookie) {
    headers.Cookie = cookieHeader;
  }

  let body;
  if (typeof step.rawBody === "string") {
    body = step.rawBody;
  } else {
    const mergedBody = mergeBody(user.body, step.body);
    if (mergedBody && Object.keys(mergedBody).length > 0) {
      body = JSON.stringify(mergedBody);
      if (!headers["Content-Type"]) {
        headers["Content-Type"] = "application/json";
      }
    }
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: step.method,
      headers,
      body,
      signal: controller.signal,
    });
    const endedAt = Date.now();
    const ok =
      !Array.isArray(step.expectedStatus) ||
      step.expectedStatus.includes(response.status);

    const result = {
      name: step.name,
      status: response.status,
      ok,
      latencyMs: endedAt - startedAt,
      user: user.name || null,
    };

    if (step.delayAfterMs) {
      await sleep(step.delayAfterMs);
    }

    return result;
  } catch (error) {
    const endedAt = Date.now();
    return {
      name: step.name,
      status: 0,
      ok: false,
      latencyMs: endedAt - startedAt,
      error: error.name === "AbortError" ? "timeout" : String(error.message || error),
      user: user.name || null,
    };
  } finally {
    clearTimeout(timer);
  }
}

async function runWorker({ workerId, scenario }) {
  const results = [];
  const users = Array.isArray(scenario.users) && scenario.users.length > 0
    ? scenario.users
    : [{}];
  const timeoutMs = scenario.requestTimeoutMs || 10000;

  for (let round = 0; round < scenario.repeatPerWorker; round += 1) {
    const user = users[(workerId + round) % users.length];

    for (const step of scenario.steps) {
      const repeat = Number.isInteger(step.repeatPerRound) && step.repeatPerRound > 0
        ? step.repeatPerRound
        : 1;
      for (let count = 0; count < repeat; count += 1) {
        const result = await runStep({ scenario, step, user, timeoutMs });
        results.push(result);
      }
    }
  }

  return results;
}

function summarize(results) {
  const latencies = results
    .map((result) => result.latencyMs)
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);

  const statuses = {};
  let okCount = 0;
  for (const result of results) {
    const key = String(result.status);
    statuses[key] = (statuses[key] || 0) + 1;
    if (result.ok) {
      okCount += 1;
    }
  }

  const errorSamples = results
    .filter((result) => result.error)
    .slice(0, 5)
    .map((result) => ({
      step: result.name,
      status: result.status,
      error: result.error,
      user: result.user,
    }));

  const totalLatency = latencies.reduce((sum, value) => sum + value, 0);

  return {
    totalRequests: results.length,
    okCount,
    failedCount: results.length - okCount,
    statuses,
    avgLatencyMs: latencies.length > 0 ? Number((totalLatency / latencies.length).toFixed(2)) : null,
    p50LatencyMs: percentile(latencies, 50),
    p95LatencyMs: percentile(latencies, 95),
    errorSamples,
  };
}

function printHelp() {
  console.log("Usage: node http_burst.js --scenario <path-to-scenario.json>");
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.scenario) {
    printHelp();
    process.exit(args.help ? 0 : 1);
  }

  const scenarioPath = path.resolve(process.cwd(), args.scenario);
  const scenario = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
  validateScenario(scenario);

  const startedAt = Date.now();
  const workerPromises = [];
  for (let workerId = 0; workerId < scenario.concurrency; workerId += 1) {
    workerPromises.push(runWorker({ workerId, scenario }));
  }

  const nestedResults = await Promise.all(workerPromises);
  const results = nestedResults.flat();
  const summary = summarize(results);

  console.log(JSON.stringify({
    scenarioPath,
    durationMs: Date.now() - startedAt,
    ...summary,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exit(1);
});
