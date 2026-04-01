import express from "express";
import axios from "axios";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const app = express();
const PORT = Number(process.env.PORT || 3000);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* =========================================================
   RAW BODY ROUTES
========================================================= */

const RAW_BODY_PATHS = new Set([
  "/webhooks/products",
  "/webhooks/inventory",
  "/ebay/notifications",
  "/ebay/oauth/callback"
]);

app.use((req, res, next) => {
  if (RAW_BODY_PATHS.has(req.path)) return next();
  return express.json({ limit: "6mb" })(req, res, next);
});

app.use((req, res, next) => {
  if (!RAW_BODY_PATHS.has(req.path)) return next();

  let data = "";
  req.setEncoding("utf8");
  req.on("data", (chunk) => {
    data += chunk;
  });
  req.on("end", () => {
    req.rawBody = data || "";
    try {
      req.body = data ? JSON.parse(data) : {};
    } catch {
      req.body = {};
    }
    next();
  });
});

/* =========================================================
   CONFIG
========================================================= */

const EBAY_ENVIRONMENT = (process.env.EBAY_ENVIRONMENT || "production").toLowerCase();
const EBAY_API_BASE =
  EBAY_ENVIRONMENT === "sandbox"
    ? "https://api.sandbox.ebay.com"
    : "https://api.ebay.com";
const EBAY_AUTH_BASE =
  EBAY_ENVIRONMENT === "sandbox"
    ? "https://auth.sandbox.ebay.com"
    : "https://auth.ebay.com";
const EBAY_IDENTITY_BASE =
  EBAY_ENVIRONMENT === "sandbox"
    ? "https://api.sandbox.ebay.com"
    : "https://api.ebay.com";

const SHOPIFY_STORE_DOMAIN = (process.env.SHOPIFY_STORE_DOMAIN || "").trim();
const SHOPIFY_ADMIN_ACCESS_TOKEN = (process.env.SHOPIFY_ADMIN_ACCESS_TOKEN || "").trim();
const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2024-10";

const EBAY_SCOPES = [
  "https://api.ebay.com/oauth/api_scope",
  "https://api.ebay.com/oauth/api_scope/sell.account",
  "https://api.ebay.com/oauth/api_scope/sell.inventory",
  "https://api.ebay.com/oauth/api_scope/sell.fulfillment",
  "https://api.ebay.com/oauth/api_scope/commerce.taxonomy.readonly"
];

const EBAY_MARKETPLACES = [
  { id: "EBAY_IT", lang: "it-IT", country: "IT", currency: "EUR" },
  { id: "EBAY_DE", lang: "de-DE", country: "DE", currency: "EUR" },
  { id: "EBAY_FR", lang: "fr-FR", country: "FR", currency: "EUR" },
  { id: "EBAY_ES", lang: "es-ES", country: "ES", currency: "EUR" },
  { id: "EBAY_GB", lang: "en-GB", country: "GB", currency: "GBP" }
];

const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "sync-data.json");

const DEFAULT_LOCATION_KEY_BY_MARKET = {
  EBAY_IT: process.env.EBAY_MERCHANT_LOCATION_KEY_IT || process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT",
  EBAY_DE: process.env.EBAY_MERCHANT_LOCATION_KEY_DE || process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT",
  EBAY_FR: process.env.EBAY_MERCHANT_LOCATION_KEY_FR || process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT",
  EBAY_ES: process.env.EBAY_MERCHANT_LOCATION_KEY_ES || process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT",
  EBAY_GB: process.env.EBAY_MERCHANT_LOCATION_KEY_GB || process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT"
};

const ORDER_POLL_WINDOW_MINUTES = Number(process.env.EBAY_ORDER_POLL_WINDOW_MINUTES || 120);
const ORDER_POLL_INTERVAL_MS = Number(process.env.EBAY_ORDER_POLL_INTERVAL_MS || 60000);
const AUTO_POLL_EBAY_ORDERS = String(process.env.AUTO_POLL_EBAY_ORDERS || "false").toLowerCase() === "true";
const AUTO_PUBLISH_ALL_MARKETS = String(process.env.AUTO_PUBLISH_ALL_MARKETS || "true").toLowerCase() === "true";

/* =========================================================
   STATE
========================================================= */

let state = {
  oauth: {
    accessToken: null,
    accessTokenExpiresAt: 0,
    refreshToken: process.env.EBAY_REFRESH_TOKEN || null
  },
  listingMap: {},
  processedOrders: {},
  lastOrderPollByMarketplace: {}
};

const categoryTreeCache = new Map();
const aspectCache = new Map();
const policyCache = new Map();

/* =========================================================
   PERSISTENCE
========================================================= */

async function ensureDataDir() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function loadState() {
  try {
    await ensureDataDir();
    const raw = await fs.readFile(DATA_FILE, "utf8");
    const parsed = JSON.parse(raw);

    state = {
      oauth: {
        accessToken: null,
        accessTokenExpiresAt: 0,
        refreshToken:
          process.env.EBAY_REFRESH_TOKEN ||
          parsed?.oauth?.refreshToken ||
          null
      },
      listingMap: parsed?.listingMap || {},
      processedOrders: parsed?.processedOrders || {},
      lastOrderPollByMarketplace: parsed?.lastOrderPollByMarketplace || {}
    };
  } catch {
    await saveState();
  }
}

async function saveState() {
  await ensureDataDir();
  await fs.writeFile(
    DATA_FILE,
    JSON.stringify(
      {
        oauth: {
          refreshToken: state.oauth.refreshToken || null
        },
        listingMap: state.listingMap,
        processedOrders: state.processedOrders,
        lastOrderPollByMarketplace: state.lastOrderPollByMarketplace
      },
      null,
      2
    ),
    "utf8"
  );
}

/* =========================================================
   GENERIC HELPERS
========================================================= */

function nowMs() {
  return Date.now();
}

function normalizeText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function truncate(value, max) {
  const txt = String(value || "");
  return txt.length <= max ? txt : txt.slice(0, max);
}

function slugify(value) {
  return normalizeText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function uniqueNonEmpty(values) {
  const out = [];
  const seen = new Set();

  for (const val of values || []) {
    const v = normalizeText(val);
    if (!v) continue;
    const key = v.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out;
}

function safeJson(data) {
  try {
    return JSON.stringify(data);
  } catch {
    return String(data);
  }
}

function env(name, fallback = "") {
  return process.env[name] || fallback;
}

function marketplaceConfig(marketplaceId) {
  return EBAY_MARKETPLACES.find((m) => m.id === marketplaceId) || EBAY_MARKETPLACES[0];
}

function getMerchantLocationKey(marketplaceId) {
  return DEFAULT_LOCATION_KEY_BY_MARKET[marketplaceId] || process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT";
}

function parseNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function cleanHtmlToText(html) {
  return normalizeText(
    String(html || "")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&quot;/gi, '"')
      .replace(/&#39;/gi, "'")
  );
}

function getShopifyFirstImageUrls(product) {
  const images = Array.isArray(product?.images) ? product.images : [];
  return uniqueNonEmpty(images.map((img) => img?.src)).slice(0, 24);
}

function getVariantOptionMap(product, variant) {
  const out = {};
  const options = Array.isArray(product?.options) ? product.options : [];
  for (let i = 0; i < options.length; i += 1) {
    const optionName = normalizeText(options[i]?.name || `Option ${i + 1}`);
    const optionValue = normalizeText(variant?.[`option${i + 1}`] || "");
    if (optionName && optionValue) out[optionName.toLowerCase()] = optionValue;
  }
  return out;
}

function getPreferredBarcode(product, variant) {
  return (
    normalizeText(variant?.barcode) ||
    normalizeText(product?.barcode) ||
    ""
  );
}

function getPreferredBrand(product) {
  return normalizeText(product?.vendor) || "Generico";
}

function getPreferredProductType(product) {
  return (
    normalizeText(product?.product_type) ||
    normalizeText(product?.productType) ||
    normalizeText(product?.category?.name) ||
    "Altro"
  );
}

function getVariantTitle(variant) {
  const title = normalizeText(variant?.title);
  return title && title.toLowerCase() !== "default title" ? title : "";
}

function getModelCandidate(product, variant) {
  const sku = normalizeText(variant?.sku);
  const variantTitle = getVariantTitle(variant);
  const title = normalizeText(product?.title);
  const productType = getPreferredProductType(product);
  const barcode = getPreferredBarcode(product, variant);

  const candidates = uniqueNonEmpty([
    sku,
    variantTitle,
    `${title} ${variantTitle}`.trim(),
    title,
    `${productType} ${sku}`.trim(),
    barcode
  ]);

  return candidates[0] || "Modello Generico";
}

function inferMaterial(product, variant) {
  const source = [
    product?.title,
    product?.body_html,
    product?.product_type,
    variant?.title
  ]
    .map((v) => String(v || "").toLowerCase())
    .join(" ");

  if (source.includes("plexiglass") || source.includes("acril")) return "Plexiglass";
  if (source.includes("legno") || source.includes("wood") || source.includes("multistrato")) return "Legno";
  if (source.includes("metallo") || source.includes("metal")) return "Metallo";
  if (source.includes("cartoncino") || source.includes("paper") || source.includes("carta")) return "Carta";
  if (source.includes("vinile") || source.includes("vinyl")) return "Vinile";
  return "";
}

function inferColor(product, variant) {
  const optionMap = getVariantOptionMap(product, variant);
  const source = [
    optionMap["color"],
    optionMap["colore"],
    variant?.title,
    product?.title
  ]
    .map((v) => String(v || "").toLowerCase())
    .join(" ");

  const colors = [
    "nero",
    "bianco",
    "rosso",
    "blu",
    "verde",
    "giallo",
    "rosa",
    "oro",
    "argento",
    "trasparente",
    "marrone",
    "beige",
    "viola",
    "azzurro",
    "multicolore",
    "black",
    "white",
    "red",
    "blue",
    "green",
    "yellow",
    "pink",
    "gold",
    "silver",
    "transparent",
    "brown",
    "beige",
    "purple",
    "light blue",
    "multicolor"
  ];

  for (const color of colors) {
    if (source.includes(color)) return color;
  }

  return "";
}

function inferSize(product, variant) {
  const optionMap = getVariantOptionMap(product, variant);
  return (
    normalizeText(optionMap["size"]) ||
    normalizeText(optionMap["taglia"]) ||
    ""
  );
}

function inferStyle(product) {
  const source = [
    product?.title,
    product?.body_html,
    product?.product_type
  ]
    .map((v) => String(v || "").toLowerCase())
    .join(" ");

  if (source.includes("personalizz")) return "Personalizzato";
  if (source.includes("artigian")) return "Artigianale";
  if (source.includes("wedding") || source.includes("matrimonio")) return "Matrimonio";
  if (source.includes("battesimo")) return "Battesimo";
  if (source.includes("comunione")) return "Comunione";
  return "";
}

function inferOccasion(product) {
  const source = [
    product?.title,
    product?.body_html,
    product?.product_type
  ]
    .map((v) => String(v || "").toLowerCase())
    .join(" ");

  if (source.includes("battesimo")) return "Battesimo";
  if (source.includes("comunione")) return "Comunione";
  if (source.includes("laurea")) return "Laurea";
  if (source.includes("18")) return "Compleanno";
  if (source.includes("compleanno")) return "Compleanno";
  if (source.includes("matrimonio")) return "Matrimonio";
  if (source.includes("nascita")) return "Nascita";
  return "";
}

/* =========================================================
   SHOPIFY HELPERS
========================================================= */

function getShopifyHeaders() {
  if (!SHOPIFY_ADMIN_ACCESS_TOKEN) {
    throw new Error("SHOPIFY_ADMIN_ACCESS_TOKEN mancante");
  }
  return {
    "X-Shopify-Access-Token": SHOPIFY_ADMIN_ACCESS_TOKEN,
    "Content-Type": "application/json"
  };
}

async function shopifyRest(method, resourcePath, data) {
  if (!SHOPIFY_STORE_DOMAIN) {
    throw new Error("SHOPIFY_STORE_DOMAIN mancante");
  }

  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}${resourcePath}`;

  const res = await axios({
    method,
    url,
    headers: getShopifyHeaders(),
    data,
    timeout: 60000
  });

  return res.data;
}

async function shopifyGraphQL(query, variables = {}) {
  if (!SHOPIFY_STORE_DOMAIN) {
    throw new Error("SHOPIFY_STORE_DOMAIN mancante");
  }

  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

  const res = await axios.post(
    url,
    { query, variables },
    {
      headers: getShopifyHeaders(),
      timeout: 60000
    }
  );

  if (res.data.errors?.length) {
    throw new Error(`Shopify GraphQL errors: ${safeJson(res.data.errors)}`);
  }

  return res.data.data;
}

async function getShopifyProductByHandle(handle) {
  const data = await shopifyRest("get", `/products.json?handle=${encodeURIComponent(handle)}&limit=1`);
  return data?.products?.[0] || null;
}

async function getShopifyProductById(productId) {
  const data = await shopifyRest("get", `/products/${productId}.json`);
  return data?.product || null;
}

async function getShopifyLocationId() {
  const data = await shopifyRest("get", `/locations.json`);
  const locationId = data?.locations?.[0]?.id;
  if (!locationId) throw new Error("Nessuna location Shopify trovata");
  return locationId;
}

async function getInventoryItemIdBySku(sku) {
  const query = `
    query inventoryItemsBySku($query: String!) {
      inventoryItems(first: 5, query: $query) {
        edges {
          node {
            id
            sku
            tracked
            variant {
              id
              displayName
              inventoryQuantity
              product {
                id
                title
              }
            }
          }
        }
      }
    }
  `;

  const data = await shopifyGraphQL(query, { query: `sku:${sku}` });
  const node = data?.inventoryItems?.edges?.[0]?.node;
  return node?.id || null;
}

async function adjustShopifyInventoryBySku(sku, delta) {
  const inventoryItemId = await getInventoryItemIdBySku(sku);
  if (!inventoryItemId) {
    return { ok: false, reason: `Inventory item non trovato per SKU ${sku}` };
  }

  const locationId = await getShopifyLocationId();

  const mutation = `
    mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
      inventoryAdjustQuantities(input: $input) {
        userErrors {
          field
          message
        }
        inventoryAdjustmentGroup {
          reason
          changes {
            name
            delta
          }
        }
      }
    }
  `;

  const data = await shopifyGraphQL(mutation, {
    input: {
      reason: "correction",
      name: "available",
      changes: [
        {
          delta,
          inventoryItemId,
          locationId: `gid://shopify/Location/${String(locationId).replace(/^gid:\/\/shopify\/Location\//, "")}`
        }
      ]
    }
  });

  const errors = data?.inventoryAdjustQuantities?.userErrors || [];
  if (errors.length) {
    return { ok: false, reason: safeJson(errors) };
  }

  return { ok: true, inventoryItemId, locationId, delta };
}

/* =========================================================
   EBAY OAUTH
========================================================= */

function buildBasicAuth() {
  const clientId = env("EBAY_CLIENT_ID");
  const clientSecret = env("EBAY_CLIENT_SECRET");
  if (!clientId || !clientSecret) {
    throw new Error("EBAY_CLIENT_ID / EBAY_CLIENT_SECRET mancanti");
  }
  return Buffer.from(`${clientId}:${clientSecret}`).toString("base64");
}

function getOAuthRedirectUri() {
  return env("EBAY_REDIRECT_URI");
}

function buildEbayConsentUrl() {
  const clientId = env("EBAY_CLIENT_ID");
  const redirectUri = getOAuthRedirectUri();
  const scope = encodeURIComponent(EBAY_SCOPES.join(" "));
  return `${EBAY_AUTH_BASE}/oauth2/authorize?client_id=${encodeURIComponent(clientId)}&response_type=code&redirect_uri=${encodeURIComponent(redirectUri)}&scope=${scope}`;
}

async function exchangeCodeForTokens(code) {
  const redirectUri = getOAuthRedirectUri();

  const body = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri
  });

  const res = await axios.post(
    `${EBAY_IDENTITY_BASE}/identity/v1/oauth2/token`,
    body.toString(),
    {
      headers: {
        Authorization: `Basic ${buildBasicAuth()}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      timeout: 60000
    }
  );

  const data = res.data || {};
  state.oauth.accessToken = data.access_token || null;
  state.oauth.accessTokenExpiresAt = nowMs() + ((Number(data.expires_in) || 7200) - 60) * 1000;
  state.oauth.refreshToken = data.refresh_token || state.oauth.refreshToken || null;
  await saveState();

  return data;
}

async function refreshEbayAccessToken() {
  if (!state.oauth.refreshToken) {
    throw new Error("EBAY_REFRESH_TOKEN mancante");
  }

  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: state.oauth.refreshToken,
    scope: EBAY_SCOPES.join(" ")
  });

  const res = await axios.post(
    `${EBAY_IDENTITY_BASE}/identity/v1/oauth2/token`,
    body.toString(),
    {
      headers: {
        Authorization: `Basic ${buildBasicAuth()}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      timeout: 60000
    }
  );

  const data = res.data || {};
  state.oauth.accessToken = data.access_token || null;
  state.oauth.accessTokenExpiresAt = nowMs() + ((Number(data.expires_in) || 7200) - 60) * 1000;

  if (data.refresh_token) {
    state.oauth.refreshToken = data.refresh_token;
  }

  await saveState();
  return state.oauth.accessToken;
}

async function getEbayAccessToken() {
  if (state.oauth.accessToken && state.oauth.accessTokenExpiresAt > nowMs()) {
    return state.oauth.accessToken;
  }
  return refreshEbayAccessToken();
}

/* =========================================================
   EBAY HTTP
========================================================= */

async function ebayRequest(method, endpoint, { token, data, params, headers } = {}) {
  const accessToken = token || (await getEbayAccessToken());

  const res = await axios({
    method,
    url: `${EBAY_API_BASE}${endpoint}`,
    data,
    params,
    timeout: 90000,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      ...(headers || {})
    }
  });

  return res.data;
}

/* =========================================================
   EBAY TAXONOMY
========================================================= */

async function getDefaultCategoryTreeId(marketplaceId, token) {
  const cached = categoryTreeCache.get(marketplaceId);
  if (cached) return cached;

  const data = await ebayRequest("get", "/commerce/taxonomy/v1/get_default_category_tree_id", {
    token,
    params: { marketplace_id: marketplaceId }
  });

  const treeId = data?.categoryTreeId;
  if (!treeId) throw new Error(`categoryTreeId non trovato per ${marketplaceId}`);

  categoryTreeCache.set(marketplaceId, treeId);
  return treeId;
}

async function getCategorySuggestions({ marketplaceId, query, token }) {
  const categoryTreeId = await getDefaultCategoryTreeId(marketplaceId, token);

  const data = await ebayRequest(
    "get",
    `/commerce/taxonomy/v1/category_tree/${categoryTreeId}/get_category_suggestions`,
    {
      token,
      params: { q: query }
    }
  );

  return data?.categorySuggestions || [];
}

async function getBestCategorySuggestion({ product, marketplaceId, token }) {
  const queries = uniqueNonEmpty([
    product?.title,
    `${product?.title || ""} ${getPreferredProductType(product)}`.trim(),
    getPreferredProductType(product)
  ]);

  for (const q of queries) {
    const suggestions = await getCategorySuggestions({ marketplaceId, query: q, token });
    const best = suggestions?.[0];
    if (best?.category?.categoryId) {
      return {
        categoryId: best.category.categoryId,
        categoryName: best.category.categoryName || "",
        raw: best
      };
    }
  }

  throw new Error(`Nessuna categoria suggerita per ${marketplaceId}`);
}

async function getItemAspectsForCategory({ marketplaceId, categoryId, token }) {
  const cacheKey = `${marketplaceId}::${categoryId}`;
  const cached = aspectCache.get(cacheKey);
  if (cached) return cached;

  const categoryTreeId = await getDefaultCategoryTreeId(marketplaceId, token);

  const data = await ebayRequest(
    "get",
    `/commerce/taxonomy/v1/category_tree/${categoryTreeId}/get_item_aspects_for_category`,
    {
      token,
      params: { category_id: categoryId }
    }
  );

  const aspects = Array.isArray(data?.aspects) ? data.aspects : [];
  aspectCache.set(cacheKey, aspects);
  return aspects;
}

/* =========================================================
   EBAY ACCOUNT POLICIES
========================================================= */

function normalizeMarketplaceId(marketplaceId) {
  const map = {
    EBAY_UK: "EBAY_GB"
  };
  return map[marketplaceId] || marketplaceId;
}

function extractPolicyArrayByType(raw, type) {
  if (!raw || typeof raw !== "object") return [];

  const pools = {
    payment: [raw.paymentPolicies, raw.paymentPolicySummaries, raw.policies],
    fulfillment: [raw.fulfillmentPolicies, raw.fulfillmentPolicySummaries, raw.policies],
    return: [raw.returnPolicies, raw.returnPolicySummaries, raw.policies]
  };

  for (const arr of pools[type] || []) {
    if (Array.isArray(arr)) return arr;
  }
  return [];
}

function getPolicyIdField(policy, type) {
  if (!policy) return null;
  if (type === "payment") return policy.paymentPolicyId || policy.policyId || policy.id || null;
  if (type === "fulfillment") return policy.fulfillmentPolicyId || policy.policyId || policy.id || null;
  if (type === "return") return policy.returnPolicyId || policy.policyId || policy.id || null;
  return null;
}

function scorePolicy(policy, marketplaceId, type) {
  let score = 0;
  const normalizedMarketplace = normalizeMarketplaceId(marketplaceId);
  const policyMarketplace = policy?.marketplaceId || policy?.marketplace || policy?.marketplace_id || null;

  if (policyMarketplace === normalizedMarketplace) score += 100;
  if (policy?.name) {
    const name = String(policy.name).toLowerCase();
    if (name.includes("default")) score += 20;
    if (name.includes("standard")) score += 10;
  }
  if (type === "fulfillment" && Array.isArray(policy?.shippingOptions) && policy.shippingOptions.length) score += 8;
  if (type === "payment") score += 5;
  if (type === "return" && (policy?.returnsAccepted || policy?.returnPeriod?.value)) score += 8;

  return score;
}

function pickBestPolicy(policies, marketplaceId, type) {
  if (!Array.isArray(policies) || !policies.length) return null;

  const normalizedMarketplace = normalizeMarketplaceId(marketplaceId);
  const exact = policies.filter((p) => {
    const market = p?.marketplaceId || p?.marketplace || p?.marketplace_id || null;
    return market === normalizedMarketplace;
  });

  const pool = exact.length ? exact : policies;
  const sorted = [...pool].sort(
    (a, b) => scorePolicy(b, marketplaceId, type) - scorePolicy(a, marketplaceId, type)
  );
  return sorted[0] || null;
}

function resolvePolicyId(autoId, envExact, envGeneric) {
  return autoId || envExact || envGeneric || null;
}

async function getPoliciesForMarketplace(marketplaceId, token) {
  const key = normalizeMarketplaceId(marketplaceId);
  if (policyCache.has(key)) return policyCache.get(key);

  const [paymentRaw, fulfillmentRaw, returnRaw] = await Promise.all([
    ebayRequest("get", "/sell/account/v1/payment_policy", {
      token,
      params: { marketplace_id: key }
    }),
    ebayRequest("get", "/sell/account/v1/fulfillment_policy", {
      token,
      params: { marketplace_id: key }
    }),
    ebayRequest("get", "/sell/account/v1/return_policy", {
      token,
      params: { marketplace_id: key }
    })
  ]);

  const paymentPolicies = extractPolicyArrayByType(paymentRaw, "payment");
  const fulfillmentPolicies = extractPolicyArrayByType(fulfillmentRaw, "fulfillment");
  const returnPolicies = extractPolicyArrayByType(returnRaw, "return");

  const payment = pickBestPolicy(paymentPolicies, key, "payment");
  const fulfillment = pickBestPolicy(fulfillmentPolicies, key, "fulfillment");
  const ret = pickBestPolicy(returnPolicies, key, "return");

  const result = {
    marketplaceId: key,
    paymentPolicyId: getPolicyIdField(payment, "payment"),
    fulfillmentPolicyId: getPolicyIdField(fulfillment, "fulfillment"),
    returnPolicyId: getPolicyIdField(ret, "return"),
    selected: {
      payment,
      fulfillment,
      return: ret
    },
    raw: {
      paymentPolicies,
      fulfillmentPolicies,
      returnPolicies
    }
  };

  policyCache.set(key, result);
  return result;
}

/* =========================================================
   ASPECTS BUILDER
========================================================= */

function aspectNameMatches(name, probes) {
  const n = String(name || "").toLowerCase();
  return probes.some((probe) => n.includes(probe));
}

function getAspectValueCandidates(name, product, variant, marketplaceId) {
  const brand = getPreferredBrand(product);
  const model = getModelCandidate(product, variant);
  const productType = getPreferredProductType(product);
  const sku = normalizeText(variant?.sku);
  const barcode = getPreferredBarcode(product, variant);
  const title = normalizeText(product?.title);
  const variantTitle = getVariantTitle(variant);
  const material = inferMaterial(product, variant);
  const color = inferColor(product, variant);
  const size = inferSize(product, variant);
  const style = inferStyle(product);
  const occasion = inferOccasion(product);
  const optionMap = getVariantOptionMap(product, variant);

  if (aspectNameMatches(name, ["brand", "marca"])) {
    return [brand];
  }

  if (aspectNameMatches(name, ["model", "modello"])) {
    return uniqueNonEmpty([model, sku, variantTitle, title]);
  }

  if (aspectNameMatches(name, ["type", "tipo"])) {
    return uniqueNonEmpty([productType, variantTitle, title]);
  }

  if (aspectNameMatches(name, ["mpn", "manufacturer part number", "part number", "codice produttore"])) {
    return uniqueNonEmpty([sku, model]);
  }

  if (aspectNameMatches(name, ["ean", "gtin", "upc", "isbn", "barcode", "bar code"])) {
    return uniqueNonEmpty([barcode, "Non applicabile"]);
  }

  if (aspectNameMatches(name, ["material", "materiale"])) {
    return uniqueNonEmpty([material, productType]);
  }

  if (aspectNameMatches(name, ["color", "colour", "colore"])) {
    return uniqueNonEmpty([color, variantTitle]);
  }

  if (aspectNameMatches(name, ["size", "taglia", "dimensione", "dimensions"])) {
    return uniqueNonEmpty([size, variantTitle]);
  }

  if (aspectNameMatches(name, ["style", "stile"])) {
    return uniqueNonEmpty([style, productType]);
  }

  if (aspectNameMatches(name, ["occasion", "occasione"])) {
    return uniqueNonEmpty([occasion]);
  }

  if (aspectNameMatches(name, ["shape", "forma"])) {
    return uniqueNonEmpty([variantTitle, productType]);
  }

  if (aspectNameMatches(name, ["theme", "tema"])) {
    return uniqueNonEmpty([occasion, style, title]);
  }

  if (aspectNameMatches(name, ["department", "reparto"])) {
    return ["Unisex"];
  }

  if (aspectNameMatches(name, ["custom", "personalised", "personalized", "personalizzato", "personalizzabile"])) {
    return ["Sì"];
  }

  for (const [optName, optValue] of Object.entries(optionMap)) {
    if (aspectNameMatches(name, [optName])) {
      return [optValue];
    }
  }

  return uniqueNonEmpty([
    variantTitle,
    productType,
    model,
    title
  ]);
}

function chooseAspectValue(name, candidates, aspectMeta) {
  const values = uniqueNonEmpty(candidates);
  if (!values.length) return null;

  const mode = String(aspectMeta?.aspectConstraint?.aspectMode || "").toUpperCase();
  const maxLength = Number(aspectMeta?.aspectConstraint?.aspectMaxLength) || 65;
  const itemValues = Array.isArray(aspectMeta?.aspectValues) ? aspectMeta.aspectValues : [];
  const localizedAllowed = new Set(itemValues.map((v) => normalizeText(v?.localizedValue).toLowerCase()).filter(Boolean));

  const cleaned = values
    .map((v) => truncate(v, maxLength))
    .filter(Boolean);

  if (localizedAllowed.size) {
    const exactAllowed = cleaned.find((v) => localizedAllowed.has(v.toLowerCase()));
    if (exactAllowed) return [exactAllowed];
  }

  if (aspectNameMatches(name, ["ean", "gtin", "upc", "isbn", "barcode"])) {
    const validBarcode = cleaned.find((v) => /^[0-9A-Za-z\- ]{8,20}$/.test(v) && !/^non applicabile$/i.test(v));
    if (validBarcode) return [validBarcode];
    return ["Non applicabile"];
  }

  if (aspectNameMatches(name, ["mpn", "manufacturer part number", "part number", "codice produttore"])) {
    return [cleaned[0] || "Non applicabile"];
  }

  if (mode === "FREE_TEXT" || !mode) {
    return [cleaned[0]];
  }

  return [cleaned[0]];
}

async function buildDynamicAspects({ product, variant, marketplaceId, categoryId, token }) {
  const aspectMetaList = await getItemAspectsForCategory({ marketplaceId, categoryId, token });

  const aspects = {};
  const requiredAspectNames = [];

  for (const aspectMeta of aspectMetaList) {
    const localizedAspectName = normalizeText(aspectMeta?.localizedAspectName);
    if (!localizedAspectName) continue;

    const isRequired = Boolean(aspectMeta?.aspectConstraint?.aspectRequired);
    if (isRequired) requiredAspectNames.push(localizedAspectName);

    const candidates = getAspectValueCandidates(localizedAspectName, product, variant, marketplaceId);
    const selected = chooseAspectValue(localizedAspectName, candidates, aspectMeta);

    if (selected?.length) {
      aspects[localizedAspectName] = selected;
    }
  }

  for (const fallbackName of ["Marca", "Brand", "Modello", "Model", "Tipo", "Type", "MPN", "EAN"]) {
    if (!aspects[fallbackName]) {
      const val = chooseAspectValue(
        fallbackName,
        getAspectValueCandidates(fallbackName, product, variant, marketplaceId),
        {}
      );
      if (val?.length) aspects[fallbackName] = val;
    }
  }

  return {
    aspects,
    requiredAspectNames,
    aspectMetaList
  };
}

/* =========================================================
   EBAY INVENTORY + OFFER
========================================================= */

function buildInventoryPayload({ product, variant, aspects, marketplaceId }) {
  const market = marketplaceConfig(marketplaceId);
  const images = getShopifyFirstImageUrls(product);
  const descriptionText = cleanHtmlToText(product?.body_html) || normalizeText(product?.title);

  return {
    availability: {
      shipToLocationAvailability: {
        quantity: Math.max(0, parseNumber(variant?.inventory_quantity, 0))
      }
    },
    condition: "NEW",
    product: {
      title: truncate(normalizeText(product?.title), 80),
      description: truncate(descriptionText, 500000),
      aspects,
      imageUrls: images,
      brand: getPreferredBrand(product),
      mpn: normalizeText(variant?.sku) || getModelCandidate(product, variant)
    },
    packageWeightAndSize: undefined,
    locale: market.lang
  };
}

async function createOrReplaceInventoryItem({ sku, product, variant, aspects, marketplaceId, token }) {
  const payload = buildInventoryPayload({ product, variant, aspects, marketplaceId });

  await ebayRequest("put", `/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
    token,
    data: payload,
    headers: {
      "Content-Language": marketplaceConfig(marketplaceId).lang
    }
  });

  return payload;
}

async function getInventoryItem(sku, token) {
  try {
    return await ebayRequest("get", `/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { token });
  } catch {
    return null;
  }
}

async function getOffersBySku(sku, marketplaceId, token) {
  try {
    const data = await ebayRequest("get", "/sell/inventory/v1/offer", {
      token,
      params: {
        sku,
        marketplace_id: marketplaceId,
        limit: 200
      }
    });

    return Array.isArray(data?.offers) ? data.offers : [];
  } catch {
    return [];
  }
}

async function createOffer({ sku, price, marketplaceId, categoryId, token }) {
  const policies = await getPoliciesForMarketplace(marketplaceId, token);

  const marketSuffix = marketplaceId.replace("EBAY_", "");
  const paymentPolicyId = resolvePolicyId(
    policies.paymentPolicyId,
    env(`EBAY_PAYMENT_POLICY_ID_${marketSuffix}`),
    env("EBAY_PAYMENT_POLICY_ID")
  );
  const fulfillmentPolicyId = resolvePolicyId(
    policies.fulfillmentPolicyId,
    env(`EBAY_FULFILLMENT_POLICY_ID_${marketSuffix}`),
    env("EBAY_FULFILLMENT_POLICY_ID")
  );
  const returnPolicyId = resolvePolicyId(
    policies.returnPolicyId,
    env(`EBAY_RETURN_POLICY_ID_${marketSuffix}`),
    env("EBAY_RETURN_POLICY_ID")
  );

  if (!paymentPolicyId || !fulfillmentPolicyId || !returnPolicyId) {
    throw new Error(
      `Policy mancanti per ${marketplaceId}: payment=${paymentPolicyId || "null"} fulfillment=${fulfillmentPolicyId || "null"} return=${returnPolicyId || "null"}`
    );
  }

  const market = marketplaceConfig(marketplaceId);
  const merchantLocationKey = getMerchantLocationKey(marketplaceId);

  const payload = {
    sku,
    marketplaceId,
    format: "FIXED_PRICE",
    availableQuantity: 1,
    categoryId,
    merchantLocationKey,
    pricingSummary: {
      price: {
        value: String(price),
        currency: market.currency
      }
    },
    listingPolicies: {
      paymentPolicyId,
      fulfillmentPolicyId,
      returnPolicyId
    }
  };

  const data = await ebayRequest("post", "/sell/inventory/v1/offer", {
    token,
    data: payload,
    headers: {
      "Content-Language": market.lang
    }
  });

  return {
    offerId: data?.offerId,
    payload,
    policiesUsed: {
      paymentPolicyId,
      fulfillmentPolicyId,
      returnPolicyId,
      merchantLocationKey
    }
  };
}

async function updateOffer({ offerId, sku, price, marketplaceId, categoryId, token }) {
  const policies = await getPoliciesForMarketplace(marketplaceId, token);

  const marketSuffix = marketplaceId.replace("EBAY_", "");
  const paymentPolicyId = resolvePolicyId(
    policies.paymentPolicyId,
    env(`EBAY_PAYMENT_POLICY_ID_${marketSuffix}`),
    env("EBAY_PAYMENT_POLICY_ID")
  );
  const fulfillmentPolicyId = resolvePolicyId(
    policies.fulfillmentPolicyId,
    env(`EBAY_FULFILLMENT_POLICY_ID_${marketSuffix}`),
    env("EBAY_FULFILLMENT_POLICY_ID")
  );
  const returnPolicyId = resolvePolicyId(
    policies.returnPolicyId,
    env(`EBAY_RETURN_POLICY_ID_${marketSuffix}`),
    env("EBAY_RETURN_POLICY_ID")
  );

  const market = marketplaceConfig(marketplaceId);

  const payload = {
    sku,
    marketplaceId,
    format: "FIXED_PRICE",
    availableQuantity: 1,
    categoryId,
    merchantLocationKey: getMerchantLocationKey(marketplaceId),
    pricingSummary: {
      price: {
        value: String(price),
        currency: market.currency
      }
    },
    listingPolicies: {
      paymentPolicyId,
      fulfillmentPolicyId,
      returnPolicyId
    }
  };

  await ebayRequest("put", `/sell/inventory/v1/offer/${encodeURIComponent(offerId)}`, {
    token,
    data: payload,
    headers: {
      "Content-Language": market.lang
    }
  });

  return { offerId, payload };
}

async function publishOffer(offerId, token) {
  return ebayRequest("post", `/sell/inventory/v1/offer/${encodeURIComponent(offerId)}/publish`, {
    token,
    data: {}
  });
}

async function createOrUpdateOffer({ sku, price, marketplaceId, categoryId, token }) {
  const offers = await getOffersBySku(sku, marketplaceId, token);
  const existing = offers.find((o) => normalizeText(o?.sku) === normalizeText(sku));

  if (existing?.offerId) {
    const updated = await updateOffer({
      offerId: existing.offerId,
      sku,
      price,
      marketplaceId,
      categoryId,
      token
    });
    return {
      mode: "updated",
      offerId: existing.offerId,
      ...updated
    };
  }

  const created = await createOffer({
    sku,
    price,
    marketplaceId,
    categoryId,
    token
  });

  return {
    mode: "created",
    ...created
  };
}

/* =========================================================
   LISTING MAP
========================================================= */

function setListingMapEntry({ sku, marketplaceId, offerId, listingId, categoryId, categoryName }) {
  const key = `${marketplaceId}::${sku}`;
  state.listingMap[key] = {
    sku,
    marketplaceId,
    offerId: offerId || null,
    listingId: listingId || null,
    categoryId: categoryId || null,
    categoryName: categoryName || null,
    updatedAt: new Date().toISOString()
  };
}

function getListingMapEntries() {
  return Object.values(state.listingMap || {});
}

function findListingMapBySku(sku) {
  return getListingMapEntries().filter((e) => e.sku === sku);
}

/* =========================================================
   CORE PUBLISH / MIGRATE / BULK UPDATE
========================================================= */

async function publishSkuToMarketplace({ product, variant, marketplaceId, token }) {
  const sku = normalizeText(variant?.sku);
  if (!sku) throw new Error("SKU mancante");

  const category = await getBestCategorySuggestion({
    product,
    marketplaceId,
    token
  });

  const { aspects, requiredAspectNames } = await buildDynamicAspects({
    product,
    variant,
    marketplaceId,
    categoryId: category.categoryId,
    token
  });

  await createOrReplaceInventoryItem({
    sku,
    product,
    variant,
    aspects,
    marketplaceId,
    token
  });

  const offerResult = await createOrUpdateOffer({
    sku,
    price: variant?.price,
    marketplaceId,
    categoryId: category.categoryId,
    token
  });

  const published = await publishOffer(offerResult.offerId, token);

  setListingMapEntry({
    sku,
    marketplaceId,
    offerId: offerResult.offerId,
    listingId: published?.listingId || published?.listing_id || null,
    categoryId: category.categoryId,
    categoryName: category.categoryName
  });

  await saveState();

  return {
    ok: true,
    sku,
    marketplaceId,
    requiredAspectNames,
    aspects,
    categoryId: category.categoryId,
    categoryName: category.categoryName,
    offerId: offerResult.offerId,
    publishResult: published,
    offerMode: offerResult.mode || "created"
  };
}

async function publishProductAllMarkets({ product, variant, token }) {
  const targets = AUTO_PUBLISH_ALL_MARKETS ? EBAY_MARKETPLACES : [EBAY_MARKETPLACES[0]];
  const results = [];

  for (const market of targets) {
    try {
      const result = await publishSkuToMarketplace({
        product,
        variant,
        marketplaceId: market.id,
        token
      });
      results.push(result);
    } catch (error) {
      results.push({
        ok: false,
        marketplaceId: market.id,
        sku: normalizeText(variant?.sku),
        error: error?.response?.data || error.message
      });
    }
  }

  return results;
}

async function reviseQuantityAndPriceForMarketplace({ sku, price, quantity, marketplaceId, token }) {
  const inventoryItem = await getInventoryItem(sku, token);
  if (!inventoryItem) {
    throw new Error(`Inventory item non trovato su eBay per SKU ${sku}`);
  }

  const updatedInventoryPayload = {
    availability: {
      shipToLocationAvailability: {
        quantity: Math.max(0, parseNumber(quantity, 0))
      }
    },
    condition: inventoryItem.condition || "NEW",
    product: inventoryItem.product || {}
  };

  await ebayRequest("put", `/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
    token,
    data: updatedInventoryPayload
  });

  const offers = await getOffersBySku(sku, marketplaceId, token);
  const offer = offers.find((o) => normalizeText(o?.sku) === normalizeText(sku));
  if (!offer?.offerId) {
    return {
      ok: false,
      marketplaceId,
      sku,
      reason: "Offer non trovata"
    };
  }

  const market = marketplaceConfig(marketplaceId);
  const payload = {
    sku,
    marketplaceId,
    format: offer.format || "FIXED_PRICE",
    availableQuantity: 1,
    categoryId: offer.categoryId,
    merchantLocationKey: offer.merchantLocationKey || getMerchantLocationKey(marketplaceId),
    pricingSummary: {
      price: {
        value: String(price),
        currency: market.currency
      }
    },
    listingPolicies: {
      paymentPolicyId: offer.listingPolicies?.paymentPolicyId,
      returnPolicyId: offer.listingPolicies?.returnPolicyId,
      fulfillmentPolicyId: offer.listingPolicies?.fulfillmentPolicyId
    }
  };

  await ebayRequest("put", `/sell/inventory/v1/offer/${encodeURIComponent(offer.offerId)}`, {
    token,
    data: payload
  });

  return {
    ok: true,
    marketplaceId,
    sku,
    offerId: offer.offerId,
    quantity,
    price
  };
}

async function bulkUpdateQtyPrice({ sku, price, quantity, marketplaces, token }) {
  const targets = Array.isArray(marketplaces) && marketplaces.length
    ? marketplaces
    : EBAY_MARKETPLACES.map((m) => m.id);

  const results = [];

  for (const marketplaceId of targets) {
    try {
      const result = await reviseQuantityAndPriceForMarketplace({
        sku,
        price,
        quantity,
        marketplaceId,
        token
      });
      results.push(result);
    } catch (error) {
      results.push({
        ok: false,
        marketplaceId,
        sku,
        error: error?.response?.data || error.message
      });
    }
  }

  return results;
}

/* =========================================================
   ORDER SYNC EBAY -> SHOPIFY
========================================================= */

function getProcessedOrderKey(marketplaceId, orderId, lineItemId) {
  return `${marketplaceId}::${orderId}::${lineItemId || "all"}`;
}

async function getRecentEbayOrders({ marketplaceId, minutes, token }) {
  const now = new Date();
  const from = new Date(now.getTime() - minutes * 60 * 1000);

  const filter = `creationdate:[${from.toISOString()}..${now.toISOString()}]`;

  const data = await ebayRequest("get", "/sell/fulfillment/v1/order", {
    token,
    params: {
      filter,
      limit: 200
    },
    headers: {
      "X-EBAY-C-MARKETPLACE-ID": marketplaceId
    }
  });

  return Array.isArray(data?.orders) ? data.orders : [];
}

async function processEbayOrders({ marketplaceId, token }) {
  const orders = await getRecentEbayOrders({
    marketplaceId,
    minutes: ORDER_POLL_WINDOW_MINUTES,
    token
  });

  const results = [];

  for (const order of orders) {
    const orderId = order?.orderId || order?.legacyOrderId || crypto.randomUUID();
    const lineItems = Array.isArray(order?.lineItems) ? order.lineItems : [];

    for (const item of lineItems) {
      const lineItemId = item?.lineItemId || item?.legacyItemId || "";
      const key = getProcessedOrderKey(marketplaceId, orderId, lineItemId);
      if (state.processedOrders[key]) {
        results.push({
          ok: true,
          skipped: true,
          reason: "già processato",
          marketplaceId,
          orderId,
          lineItemId
        });
        continue;
      }

      const sku = normalizeText(item?.sku);
      const qty = Math.max(0, parseNumber(item?.quantity, 0));

      if (!sku || qty <= 0) {
        results.push({
          ok: false,
          marketplaceId,
          orderId,
          lineItemId,
          reason: "sku o quantity non validi"
        });
        continue;
      }

      try {
        const adjust = await adjustShopifyInventoryBySku(sku, -qty);
        if (adjust.ok) {
          state.processedOrders[key] = {
            marketplaceId,
            orderId,
            lineItemId,
            sku,
            quantity: qty,
            processedAt: new Date().toISOString()
          };
          results.push({
            ok: true,
            marketplaceId,
            orderId,
            lineItemId,
            sku,
            quantity: qty,
            shopify: adjust
          });
        } else {
          results.push({
            ok: false,
            marketplaceId,
            orderId,
            lineItemId,
            sku,
            quantity: qty,
            reason: adjust.reason
          });
        }
      } catch (error) {
        results.push({
          ok: false,
          marketplaceId,
          orderId,
          lineItemId,
          sku,
          quantity: qty,
          error: error?.response?.data || error.message
        });
      }
    }
  }

  state.lastOrderPollByMarketplace[marketplaceId] = new Date().toISOString();
  await saveState();
  return results;
}

async function pollAllEbayOrders() {
  const token = await getEbayAccessToken();
  const out = {};

  for (const market of EBAY_MARKETPLACES) {
    try {
      out[market.id] = await processEbayOrders({
        marketplaceId: market.id,
        token
      });
    } catch (error) {
      out[market.id] = {
        ok: false,
        error: error?.response?.data || error.message
      };
    }
  }

  return out;
}

/* =========================================================
   SHOPIFY WEBHOOK HANDLERS
========================================================= */

function verifyShopifyWebhook(rawBody, hmacHeader) {
  const secret = env("SHOPIFY_WEBHOOK_SECRET");
  if (!secret) return true;
  const digest = crypto
    .createHmac("sha256", secret)
    .update(rawBody, "utf8")
    .digest("base64");
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(hmacHeader || ""));
}

function extractVariantFromWebhook(payload) {
  if (Array.isArray(payload?.variants) && payload.variants.length) {
    return payload.variants[0];
  }
  return payload?.variant || null;
}

async function handleShopifyProductWebhook(payload) {
  const token = await getEbayAccessToken();
  const product = payload;
  const variants = Array.isArray(product?.variants) ? product.variants : [];

  const results = [];
  for (const variant of variants) {
    const sku = normalizeText(variant?.sku);
    if (!sku) {
      results.push({ ok: false, reason: "sku mancante", variantId: variant?.id || null });
      continue;
    }

    const res = await publishProductAllMarkets({
      product,
      variant,
      token
    });

    results.push({
      sku,
      results: res
    });
  }

  return results;
}

async function handleShopifyInventoryWebhook(payload) {
  const token = await getEbayAccessToken();

  const sku = normalizeText(payload?.sku);
  const price = parseNumber(payload?.price, 0);
  const quantity = parseNumber(payload?.inventory_quantity, 0);

  if (!sku) {
    return {
      ok: false,
      reason: "sku mancante nel webhook inventory"
    };
  }

  const results = await bulkUpdateQtyPrice({
    sku,
    price,
    quantity,
    marketplaces: EBAY_MARKETPLACES.map((m) => m.id),
    token
  });

  return { ok: true, sku, results };
}

/* =========================================================
   EBAY NOTIFICATIONS
========================================================= */

function verifyEbayNotificationSignature() {
  return true;
}

/* =========================================================
   ROUTES
========================================================= */

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "sync Shopify ↔ eBay",
    environment: EBAY_ENVIRONMENT,
    oauthConnected: Boolean(state.oauth.refreshToken),
    marketplaces: EBAY_MARKETPLACES.map((m) => m.id)
  });
});

app.get("/ebay/health", async (req, res) => {
  try {
    const token = await getEbayAccessToken();
    const policyStatus = {};

    for (const market of EBAY_MARKETPLACES) {
      try {
        const policies = await getPoliciesForMarketplace(market.id, token);
        policyStatus[market.id] = {
          paymentPolicyId: policies.paymentPolicyId,
          fulfillmentPolicyId: policies.fulfillmentPolicyId,
          returnPolicyId: policies.returnPolicyId,
          merchantLocationKey: getMerchantLocationKey(market.id)
        };
      } catch (error) {
        policyStatus[market.id] = {
          error: error?.response?.data || error.message
        };
      }
    }

    res.json({
      ok: true,
      ebayEnvironment: EBAY_ENVIRONMENT,
      oauthConnected: Boolean(state.oauth.refreshToken),
      accessTokenCached: Boolean(state.oauth.accessToken),
      listingMapCount: Object.keys(state.listingMap || {}).length,
      lastOrderPollByMarketplace: state.lastOrderPollByMarketplace,
      policies: policyStatus
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.get("/ebay/oauth/connect", (req, res) => {
  res.redirect(buildEbayConsentUrl());
});

app.get("/ebay/oauth/callback", async (req, res) => {
  try {
    const code = req.query.code || req.body?.code;
    if (!code) {
      return res.status(400).send("Code mancante");
    }

    await exchangeCodeForTokens(code);

    res.send("OAuth eBay completato correttamente.");
  } catch (error) {
    res.status(500).send(`OAuth error: ${safeJson(error?.response?.data || error.message)}`);
  }
});

app.get("/ebay/account/policies", async (req, res) => {
  try {
    const token = await getEbayAccessToken();
    const results = {};

    for (const market of EBAY_MARKETPLACES) {
      results[market.id] = await getPoliciesForMarketplace(market.id, token);
    }

    res.json({
      ok: true,
      policies: results
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/ebay/account/policies/clear-cache", async (req, res) => {
  policyCache.clear();
  res.json({ ok: true, message: "Policy cache svuotata" });
});

app.get("/ebay/category/suggest-all", async (req, res) => {
  try {
    const q = normalizeText(req.query.q || req.query.title || "");
    if (!q) {
      return res.status(400).json({ ok: false, error: "Parametro q mancante" });
    }

    const token = await getEbayAccessToken();
    const out = {};

    for (const market of EBAY_MARKETPLACES) {
      const suggestions = await getCategorySuggestions({
        marketplaceId: market.id,
        query: q,
        token
      });

      out[market.id] = suggestions.slice(0, 10).map((s) => ({
        categoryId: s?.category?.categoryId || null,
        categoryName: s?.category?.categoryName || null,
        categoryTreeNodeLevel: s?.categoryTreeNodeLevel || null
      }));
    }

    res.json({
      ok: true,
      query: q,
      suggestions: out
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.get("/ebay/listing-map", async (req, res) => {
  res.json({
    ok: true,
    count: getListingMapEntries().length,
    entries: getListingMapEntries()
  });
});

app.post("/ebay/listing-map/clear", async (req, res) => {
  state.listingMap = {};
  await saveState();
  res.json({ ok: true, message: "Listing map svuotata" });
});

app.post("/ebay/migrate-listing-map", async (req, res) => {
  try {
    const entries = Array.isArray(req.body?.entries) ? req.body.entries : [];
    for (const entry of entries) {
      if (!entry?.sku || !entry?.marketplaceId) continue;
      setListingMapEntry({
        sku: entry.sku,
        marketplaceId: entry.marketplaceId,
        offerId: entry.offerId || null,
        listingId: entry.listingId || null,
        categoryId: entry.categoryId || null,
        categoryName: entry.categoryName || null
      });
    }
    await saveState();
    res.json({
      ok: true,
      count: getListingMapEntries().length
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

app.post("/ebay/publish", async (req, res) => {
  try {
    const product = req.body?.product;
    const variant = req.body?.variant;
    const marketplaceId = req.body?.marketplaceId;

    if (!product || !variant || !variant.sku) {
      return res.status(400).json({
        ok: false,
        error: "product, variant e variant.sku sono obbligatori"
      });
    }

    const token = await getEbayAccessToken();

    if (marketplaceId) {
      const result = await publishSkuToMarketplace({
        product,
        variant,
        marketplaceId,
        token
      });

      return res.json({ ok: true, result });
    }

    const results = await publishProductAllMarkets({
      product,
      variant,
      token
    });

    return res.json({ ok: true, results });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/ebay/sync", async (req, res) => {
  try {
    const product = req.body?.product;
    const variant = req.body?.variant;

    if (!product || !variant || !variant.sku) {
      return res.status(400).json({
        ok: false,
        error: "product, variant e variant.sku sono obbligatori"
      });
    }

    const token = await getEbayAccessToken();
    const results = await publishProductAllMarkets({
      product,
      variant,
      token
    });

    const hasErrors = results.some((r) => !r.ok);
    res.status(hasErrors ? 207 : 200).json({
      ok: !hasErrors,
      results
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/ebay/bulk-update", async (req, res) => {
  try {
    const sku = normalizeText(req.body?.sku);
    const price = parseNumber(req.body?.price, 0);
    const quantity = parseNumber(req.body?.quantity, 0);
    const marketplaces = Array.isArray(req.body?.marketplaces) ? req.body.marketplaces : undefined;

    if (!sku) {
      return res.status(400).json({
        ok: false,
        error: "sku mancante"
      });
    }

    const token = await getEbayAccessToken();
    const results = await bulkUpdateQtyPrice({
      sku,
      price,
      quantity,
      marketplaces,
      token
    });

    res.json({
      ok: results.every((r) => r.ok),
      results
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/ebay/order-sync/pull", async (req, res) => {
  try {
    const token = await getEbayAccessToken();
    const marketplaceId = normalizeText(req.body?.marketplaceId);

    if (marketplaceId) {
      const results = await processEbayOrders({
        marketplaceId,
        token
      });
      return res.json({ ok: true, marketplaceId, results });
    }

    const results = await pollAllEbayOrders();
    return res.json({ ok: true, results });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/webhooks/products", async (req, res) => {
  try {
    const hmac = req.get("X-Shopify-Hmac-Sha256");
    if (!verifyShopifyWebhook(req.rawBody || "", hmac)) {
      return res.status(401).json({ ok: false, error: "Webhook Shopify non valido" });
    }

    const results = await handleShopifyProductWebhook(req.body || {});
    res.json({ ok: true, results });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/webhooks/inventory", async (req, res) => {
  try {
    const hmac = req.get("X-Shopify-Hmac-Sha256");
    if (!verifyShopifyWebhook(req.rawBody || "", hmac)) {
      return res.status(401).json({ ok: false, error: "Webhook Shopify non valido" });
    }

    const results = await handleShopifyInventoryWebhook(req.body || {});
    res.json(results);
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.post("/ebay/notifications", async (req, res) => {
  try {
    if (!verifyEbayNotificationSignature(req)) {
      return res.status(401).json({ ok: false, error: "Notifica eBay non valida" });
    }

    console.log("EBAY NOTIFICATION RECEIVED", req.body || req.rawBody || "");
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});

app.get("/shopify/product-by-handle", async (req, res) => {
  try {
    const handle = normalizeText(req.query.handle || "");
    if (!handle) {
      return res.status(400).json({ ok: false, error: "handle mancante" });
    }
    const product = await getShopifyProductByHandle(handle);
    res.json({ ok: true, product });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

app.get("/shopify/product/:id", async (req, res) => {
  try {
    const product = await getShopifyProductById(req.params.id);
    res.json({ ok: true, product });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message
    });
  }
});

/* =========================================================
   STARTUP
========================================================= */

let orderPollTimer = null;

async function startup() {
  await loadState();

  if (process.env.EBAY_REFRESH_TOKEN && process.env.EBAY_REFRESH_TOKEN !== state.oauth.refreshToken) {
    state.oauth.refreshToken = process.env.EBAY_REFRESH_TOKEN;
    await saveState();
  }

  if (AUTO_POLL_EBAY_ORDERS) {
    orderPollTimer = setInterval(async () => {
      try {
        const results = await pollAllEbayOrders();
        console.log("AUTO ORDER POLL", safeJson(results));
      } catch (error) {
        console.error("AUTO ORDER POLL ERROR", error?.response?.data || error.message);
      }
    }, ORDER_POLL_INTERVAL_MS);
  }

  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

startup().catch((error) => {
  console.error("STARTUP ERROR", error?.response?.data || error.message);
  process.exit(1);
});
