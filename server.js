import express from "express";
import axios from "axios";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";

const app = express();
const PORT = Number(process.env.PORT || 3000);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =========================
// RAW ROUTES TO SKIP JSON PARSER
// =========================

const RAW_BODY_PATHS = new Set([
  "/webhooks/products",
  "/webhooks/inventory",
  "/ebay/notifications",
]);

app.use((req, res, next) => {
  if (RAW_BODY_PATHS.has(req.path)) return next();
  return express.json({ limit: "4mb" })(req, res, next);
});

app.use((req, res, next) => {
  if (RAW_BODY_PATHS.has(req.path)) return next();
  return express.urlencoded({ extended: true })(req, res, next);
});

app.use("/public", express.static(path.join(__dirname, "public")));

// =========================
// ENV
// =========================

// Amazon
const AMAZON_MARKETPLACE_ID = process.env.AMAZON_MARKETPLACE_ID;
const AMAZON_CLIENT_ID = process.env.AMAZON_LWA_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_LWA_CLIENT_SECRET;
const AMAZON_REFRESH_TOKEN = process.env.AMAZON_REFRESH_TOKEN;
const AMAZON_SELLER_ID = process.env.AMAZON_SELLER_ID;
const AMAZON_SQS_QUEUE_URL = process.env.AMAZON_SQS_QUEUE_URL;
const AMAZON_SQS_QUEUE_ARN = process.env.AMAZON_SQS_QUEUE_ARN;

// Shopify
const SHOPIFY_SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN;
const SHOPIFY_CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const SHOPIFY_CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
const SHOPIFY_LOCATION_ID = process.env.SHOPIFY_LOCATION_ID;

// Base URL
const APP_BASE_URL = (
  process.env.APP_BASE_URL || "https://syncamzeby.onrender.com"
).replace(/\/+$/, "");

// eBay
const EBAY_ENABLED = String(process.env.EBAY_ENABLED || "false") === "true";
const EBAY_ENVIRONMENT = String(
  process.env.EBAY_ENVIRONMENT || "production"
).toLowerCase();
const EBAY_CLIENT_ID = process.env.EBAY_CLIENT_ID || "";
const EBAY_CLIENT_SECRET = process.env.EBAY_CLIENT_SECRET || "";
const EBAY_RU_NAME = process.env.EBAY_RU_NAME || "";
const EBAY_REDIRECT_URI =
  process.env.EBAY_REDIRECT_URI || `${APP_BASE_URL}/ebay/oauth/callback`;
const EBAY_VERIFICATION_TOKEN = process.env.EBAY_VERIFICATION_TOKEN || "";
const EBAY_NOTIFICATION_ENDPOINT =
  process.env.EBAY_NOTIFICATION_ENDPOINT ||
  `${APP_BASE_URL}/ebay/notifications`;

const EBAY_SCOPES =
  process.env.EBAY_SCOPES ||
  [
    "https://api.ebay.com/oauth/api_scope/sell.inventory",
    "https://api.ebay.com/oauth/api_scope/sell.account",
    "https://api.ebay.com/oauth/api_scope/sell.fulfillment",
    "https://api.ebay.com/oauth/api_scope/commerce.identity.readonly",
  ].join(" ");

const EBAY_DEFAULT_MARKETPLACE_ID =
  process.env.EBAY_DEFAULT_MARKETPLACE_ID || "EBAY_IT";
const EBAY_DEFAULT_LISTING_DURATION =
  process.env.EBAY_DEFAULT_LISTING_DURATION || "GTC";
const EBAY_DEFAULT_CONDITION = process.env.EBAY_DEFAULT_CONDITION || "NEW";
const EBAY_DEFAULT_FORMAT = process.env.EBAY_DEFAULT_FORMAT || "FIXED_PRICE";

const EBAY_LOCATION_KEYS = {
  EBAY_IT:
    process.env.EBAY_MERCHANT_LOCATION_KEY_IT ||
    process.env.EBAY_MERCHANT_LOCATION_KEY ||
    "Default-EBAY_IT",
  EBAY_DE:
    process.env.EBAY_MERCHANT_LOCATION_KEY_DE ||
    process.env.EBAY_MERCHANT_LOCATION_KEY ||
    "Default-EBAY_IT",
  EBAY_FR:
    process.env.EBAY_MERCHANT_LOCATION_KEY_FR ||
    process.env.EBAY_MERCHANT_LOCATION_KEY ||
    "Default-EBAY_IT",
  EBAY_ES:
    process.env.EBAY_MERCHANT_LOCATION_KEY_ES ||
    process.env.EBAY_MERCHANT_LOCATION_KEY ||
    "Default-EBAY_IT",
  EBAY_GB:
    process.env.EBAY_MERCHANT_LOCATION_KEY_GB ||
    process.env.EBAY_MERCHANT_LOCATION_KEY ||
    "Default-EBAY_IT",
};

// eBay persistence
const EBAY_STATE_DIR =
  process.env.EBAY_STATE_DIR || path.join(__dirname, "data");
const EBAY_STATE_FILE = path.join(EBAY_STATE_DIR, "ebay-connection.json");
const EBAY_LISTING_MAP_FILE = path.join(EBAY_STATE_DIR, "ebay-listing-map.json");
const EBAY_ORDER_SYNC_FILE = path.join(EBAY_STATE_DIR, "ebay-order-sync.json");

// Optional env JSON maps
const EBAY_LISTING_ID_MAP_JSON = process.env.EBAY_LISTING_ID_MAP_JSON || "";
const EBAY_CATEGORY_MAP_JSON = process.env.EBAY_CATEGORY_MAP_JSON || "";

// Polling
const AUTO_POLL_SQS = String(process.env.AUTO_POLL_SQS || "false") === "true";
const SQS_POLL_INTERVAL_MS = Number(process.env.SQS_POLL_INTERVAL_MS || 20000);

// eBay order poll
const AUTO_POLL_EBAY_ORDERS =
  String(process.env.AUTO_POLL_EBAY_ORDERS || "false") === "true";
const EBAY_ORDER_POLL_INTERVAL_MS = Number(
  process.env.EBAY_ORDER_POLL_INTERVAL_MS || 60000
);
const EBAY_ORDER_PULL_WINDOW_MINUTES = Number(
  process.env.EBAY_ORDER_PULL_WINDOW_MINUTES || 120
);

// APIs
const AMAZON_SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com";
const EBAY_AUTH_BASE =
  EBAY_ENVIRONMENT === "sandbox"
    ? "https://auth.sandbox.ebay.com"
    : "https://auth.ebay.com";
const EBAY_API_BASE =
  EBAY_ENVIRONMENT === "sandbox"
    ? "https://api.sandbox.ebay.com"
    : "https://api.ebay.com";

// =========================
// MARKETPLACES / LOCALES
// =========================

const MARKETPLACES = [
  {
    marketplaceId: "EBAY_IT",
    site: "ebay.it",
    locale: "it-IT",
    language: "it",
    currency: "EUR",
    country: "IT",
    label: "Italia",
  },
  {
    marketplaceId: "EBAY_DE",
    site: "ebay.de",
    locale: "de-DE",
    language: "de",
    currency: "EUR",
    country: "DE",
    label: "Germania",
  },
  {
    marketplaceId: "EBAY_FR",
    site: "ebay.fr",
    locale: "fr-FR",
    language: "fr",
    currency: "EUR",
    country: "FR",
    label: "Francia",
  },
  {
    marketplaceId: "EBAY_ES",
    site: "ebay.es",
    locale: "es-ES",
    language: "es",
    currency: "EUR",
    country: "ES",
    label: "Spagna",
  },
  {
    marketplaceId: "EBAY_GB",
    site: "ebay.co.uk",
    locale: "en-GB",
    language: "en",
    currency: "GBP",
    country: "GB",
    label: "Regno Unito",
  },
];

function getMarketplaceMeta(marketplaceId) {
  return (
    MARKETPLACES.find(
      (m) => m.marketplaceId === String(marketplaceId || "").trim()
    ) || null
  );
}

function getMerchantLocationKey(marketplaceId) {
  return (
    EBAY_LOCATION_KEYS[String(marketplaceId || "").trim()] ||
    EBAY_LOCATION_KEYS.EBAY_IT
  );
}

// =========================
// IN-MEMORY STATE
// =========================

const inventoryMap = new Map();
const processedAmazonOrderEvents = new Set();
const ebayOAuthStates = new Map();
const ebayListingIdMap = new Map();

const ebayOrderSyncState = {
  processedOrderIds: new Set(),
  lastCheckedAt: null,
};

const ebayConnectionStore = {
  connected: false,
  connectedAt: null,
  environment: EBAY_ENVIRONMENT,
  accessToken: null,
  refreshToken: null,
  accessTokenExpiresAt: null,
  refreshTokenExpiresAt: null,
  tokenType: null,
  scope: null,
  userInfo: null,
};

const ebayCategoryTreeCache = new Map();
const ebayCategoryAspectsCache = new Map();

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

// =========================
// HELPERS
// =========================

function requireEnv(name, value) {
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`);
  }
}

function validateBaseEnv() {
  requireEnv("AMAZON_MARKETPLACE_ID", AMAZON_MARKETPLACE_ID);
  requireEnv("AMAZON_LWA_CLIENT_ID", AMAZON_CLIENT_ID);
  requireEnv("AMAZON_LWA_CLIENT_SECRET", AMAZON_CLIENT_SECRET);
  requireEnv("AMAZON_REFRESH_TOKEN", AMAZON_REFRESH_TOKEN);
  requireEnv("AMAZON_SELLER_ID", AMAZON_SELLER_ID);
  requireEnv("AMAZON_SQS_QUEUE_URL", AMAZON_SQS_QUEUE_URL);
  requireEnv("AMAZON_SQS_QUEUE_ARN", AMAZON_SQS_QUEUE_ARN);

  requireEnv("SHOPIFY_SHOP_DOMAIN", SHOPIFY_SHOP_DOMAIN);
  requireEnv("SHOPIFY_CLIENT_ID", SHOPIFY_CLIENT_ID);
  requireEnv("SHOPIFY_CLIENT_SECRET", SHOPIFY_CLIENT_SECRET);
  requireEnv("SHOPIFY_LOCATION_ID", SHOPIFY_LOCATION_ID);
}

function validateEbayEnv() {
  if (!EBAY_ENABLED) {
    throw new Error("eBay module disabled. Set EBAY_ENABLED=true");
  }

  if (!["production", "sandbox"].includes(EBAY_ENVIRONMENT)) {
    throw new Error("EBAY_ENVIRONMENT must be 'production' or 'sandbox'");
  }

  requireEnv("EBAY_CLIENT_ID", EBAY_CLIENT_ID);
  requireEnv("EBAY_CLIENT_SECRET", EBAY_CLIENT_SECRET);
  requireEnv("EBAY_RU_NAME", EBAY_RU_NAME);
}

function validateEbayVerificationConfig() {
  requireEnv("EBAY_VERIFICATION_TOKEN", EBAY_VERIFICATION_TOKEN);
  requireEnv("EBAY_NOTIFICATION_ENDPOINT", EBAY_NOTIFICATION_ENDPOINT);

  const tokenOk =
    EBAY_VERIFICATION_TOKEN.length >= 32 &&
    EBAY_VERIFICATION_TOKEN.length <= 80 &&
    /^[A-Za-z0-9_-]+$/.test(EBAY_VERIFICATION_TOKEN);

  if (!tokenOk) {
    throw new Error(
      "EBAY_VERIFICATION_TOKEN must be 32-80 chars and contain only letters, numbers, underscore, or hyphen"
    );
  }
}

function nowIso() {
  return new Date().toISOString();
}

function isoMinutesAgo(minutes) {
  return new Date(Date.now() - Number(minutes || 0) * 60 * 1000).toISOString();
}

function futureIsoFromSeconds(seconds) {
  return new Date(Date.now() + Number(seconds || 0) * 1000).toISOString();
}

function sanitizeEbayConnectionForResponse() {
  return {
    connected: ebayConnectionStore.connected,
    connectedAt: ebayConnectionStore.connectedAt,
    environment: ebayConnectionStore.environment,
    accessTokenExpiresAt: ebayConnectionStore.accessTokenExpiresAt,
    refreshTokenExpiresAt: ebayConnectionStore.refreshTokenExpiresAt,
    tokenType: ebayConnectionStore.tokenType,
    scope: ebayConnectionStore.scope,
    userInfo: ebayConnectionStore.userInfo,
    hasAccessToken: Boolean(ebayConnectionStore.accessToken),
    hasRefreshToken: Boolean(ebayConnectionStore.refreshToken),
  };
}

function buildEbayBasicAuthHeader() {
  const raw = `${EBAY_CLIENT_ID}:${EBAY_CLIENT_SECRET}`;
  return `Basic ${Buffer.from(raw).toString("base64")}`;
}

function buildEbayOAuthStartUrl(state) {
  const params = new URLSearchParams({
    client_id: EBAY_CLIENT_ID,
    redirect_uri: EBAY_RU_NAME,
    response_type: "code",
    scope: EBAY_SCOPES,
    state,
  });

  return `${EBAY_AUTH_BASE}/oauth2/authorize?${params.toString()}`;
}

function buildEbayApiHeaders(accessToken, extra = {}) {
  return {
    Authorization: `Bearer ${accessToken}`,
    Accept: "application/json",
    "Content-Type": "application/json",
    ...extra,
  };
}

function getQueryString(params = {}) {
  const sp = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && String(value) !== "") {
      sp.set(key, String(value));
    }
  }

  const qs = sp.toString();
  return qs ? `?${qs}` : "";
}

function buildEbayChallengeResponse({
  challengeCode,
  verificationToken,
  endpoint,
}) {
  const hash = crypto.createHash("sha256");
  hash.update(challengeCode);
  hash.update(verificationToken);
  hash.update(endpoint);
  return hash.digest("hex");
}

function stripHtml(input = "") {
  return String(input)
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<\/?[^>]+(>|$)/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = String(value || "").trim();
    if (text) return text;
  }
  return "";
}

function truncateText(value, max = 4000) {
  const text = String(value || "");
  if (text.length <= max) return text;
  return text.slice(0, max - 1) + "…";
}

function normalizeMoney(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? Number(n.toFixed(2)) : 0;
}

function cleanHtmlForEbay(html = "") {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .trim();
}

function getRequestedMarketplaceId(req) {
  return String(req.query.marketplaceId || EBAY_DEFAULT_MARKETPLACE_ID);
}

function getSourceLanguage(input) {
  const lang = String(input || "it").trim().toLowerCase();
  return ["it", "de", "fr", "es", "en"].includes(lang) ? lang : "it";
}

function languageToLocale(language) {
  const lang = getSourceLanguage(language);
  if (lang === "it") return "it-IT";
  if (lang === "de") return "de-DE";
  if (lang === "fr") return "fr-FR";
  if (lang === "es") return "es-ES";
  return "en-GB";
}

function normalizeContentLanguage(value, fallback = "it-IT") {
  const raw = String(value || "").trim();
  const regex = /^[a-z]{2}-[A-Z]{2}$/;

  if (regex.test(raw)) return raw;
  if (regex.test(fallback)) return fallback;
  return "it-IT";
}

function parsePossiblyJsonBody(body) {
  if (Buffer.isBuffer(body)) {
    const text = body.toString("utf8");
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  if (typeof body === "string") {
    try {
      return JSON.parse(body);
    } catch {
      return body;
    }
  }

  return body;
}

function errorToSerializable(error) {
  return error?.response?.data || error?.message || String(error);
}

function getEbayErrorsArray(errorLike) {
  const data = errorLike?.response?.data || errorLike || {};
  return Array.isArray(data?.errors) ? data.errors : [];
}

function isEbayOfferUnavailableError(error) {
  return getEbayErrorsArray(error).some((e) => Number(e?.errorId) === 25713);
}

function isPlainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value);
}

function normalizeSku(value) {
  return String(value || "").trim();
}

function normalizeListingId(value) {
  return String(value || "")
    .trim()
    .replace(/[^\d]/g, "");
}

function sanitizeListingMapForResponse() {
  const entries = [];
  for (const [sku, listingId] of ebayListingIdMap.entries()) {
    entries.push({ sku, listingId });
  }
  return {
    count: entries.length,
    entries: entries.sort((a, b) => a.sku.localeCompare(b.sku)),
  };
}

function getPersistedListingIdForSku(sku) {
  return ebayListingIdMap.get(normalizeSku(sku)) || null;
}

function setPersistedListingIdForSku(sku, listingId) {
  const safeSku = normalizeSku(sku);
  const safeListingId = normalizeListingId(listingId);

  if (!safeSku) {
    throw new Error("Cannot save listing mapping: missing sku");
  }

  if (!safeListingId) {
    throw new Error("Cannot save listing mapping: missing listingId");
  }

  ebayListingIdMap.set(safeSku, safeListingId);
  return safeListingId;
}

function deletePersistedListingIdForSku(sku) {
  ebayListingIdMap.delete(normalizeSku(sku));
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseEnvJsonMap(jsonText) {
  if (!jsonText) return {};
  try {
    const data = JSON.parse(jsonText);
    return isPlainObject(data) ? data : {};
  } catch {
    return {};
  }
}

const ENV_CATEGORY_MAP = parseEnvJsonMap(EBAY_CATEGORY_MAP_JSON);

function getConfiguredCategoryForMarketplace(sku, marketplaceId) {
  const bySku = ENV_CATEGORY_MAP[sku];
  if (isPlainObject(bySku) && bySku[marketplaceId]) {
    return String(bySku[marketplaceId]).trim();
  }

  const byMarketplace = ENV_CATEGORY_MAP[marketplaceId];
  if (typeof byMarketplace === "string" && byMarketplace.trim()) {
    return byMarketplace.trim();
  }

  return "";
}

function serializeOrderSyncState() {
  return {
    lastCheckedAt: ebayOrderSyncState.lastCheckedAt,
    processedOrderIds: Array.from(ebayOrderSyncState.processedOrderIds).sort(),
  };
}

function uniqueStrings(values = []) {
  const out = [];
  const seen = new Set();

  for (const value of values) {
    const txt = String(value || "").trim();
    if (!txt) continue;
    const key = txt.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(txt);
  }

  return out;
}

function getVariantSelectedOptionValue(selectedOptions, optionNames = []) {
  for (const option of safeArray(selectedOptions)) {
    const name = String(option?.name || "").trim().toLowerCase();
    const value = String(option?.value || "").trim();
    if (!name || !value) continue;

    if (optionNames.some((needle) => name.includes(String(needle).toLowerCase()))) {
      return value;
    }
  }

  return "";
}

function aspectNameMatches(name, needles = []) {
  const txt = String(name || "").trim().toLowerCase();
  return needles.some((needle) => txt.includes(String(needle).toLowerCase()));
}

function buildModelCandidate(shopifyVariant) {
  const sku = firstNonEmpty(shopifyVariant?.sku);
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);
  const title = firstNonEmpty(shopifyVariant?.product?.title);
  const productType = firstNonEmpty(shopifyVariant?.product?.productType);
  const categoryFullName = firstNonEmpty(shopifyVariant?.product?.categoryFullName);
  const barcode = firstNonEmpty(shopifyVariant?.barcode);

  return firstNonEmpty(
    sku,
    variantTitle,
    `${title} ${variantTitle}`.trim(),
    title,
    productType,
    categoryFullName,
    barcode,
    "Modello Generico"
  );
}

function buildTypeCandidate(shopifyVariant) {
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);
  const productType = firstNonEmpty(shopifyVariant?.product?.productType);
  const categoryFullName = firstNonEmpty(shopifyVariant?.product?.categoryFullName);
  const title = firstNonEmpty(shopifyVariant?.product?.title);

  return firstNonEmpty(
    productType,
    categoryFullName,
    variantTitle,
    title,
    "General"
  );
}

function inferColorCandidate(shopifyVariant) {
  return firstNonEmpty(
    getVariantSelectedOptionValue(shopifyVariant?.selectedOptions, [
      "color",
      "colour",
      "colore",
    ]),
    ""
  );
}

function inferSizeCandidate(shopifyVariant) {
  return firstNonEmpty(
    getVariantSelectedOptionValue(shopifyVariant?.selectedOptions, [
      "size",
      "taglia",
      "dimensione",
      "misura",
    ]),
    ""
  );
}

function inferMaterialCandidate(shopifyVariant) {
  const blob = [
    shopifyVariant?.product?.title,
    shopifyVariant?.product?.productType,
    shopifyVariant?.product?.categoryFullName,
    shopifyVariant?.product?.descriptionText,
    shopifyVariant?.variantTitle,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (blob.includes("plexiglass") || blob.includes("acril")) return "Plexiglass";
  if (blob.includes("legno") || blob.includes("wood") || blob.includes("multistrato")) return "Legno";
  if (blob.includes("metallo") || blob.includes("metal")) return "Metallo";
  if (blob.includes("cartoncino") || blob.includes("paper") || blob.includes("carta")) return "Carta";
  if (blob.includes("vinile") || blob.includes("vinyl")) return "Vinile";

  return "";
}

function inferStyleCandidate(shopifyVariant) {
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);
  const blob = [
    shopifyVariant?.product?.title,
    shopifyVariant?.product?.productType,
    shopifyVariant?.product?.categoryFullName,
    shopifyVariant?.product?.descriptionText,
    variantTitle,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (blob.includes("personalizz")) return "Personalizzato";
  if (blob.includes("artigian")) return "Artigianale";
  return firstNonEmpty(
    variantTitle &&
      !["default title", "titolo predefinito"].includes(variantTitle.toLowerCase())
      ? variantTitle
      : "",
    ""
  );
}

function buildAspectCandidates(aspectName, shopifyVariant) {
  const vendor = firstNonEmpty(shopifyVariant?.product?.vendor, "Generic");
  const sku = firstNonEmpty(shopifyVariant?.sku);
  const barcode = firstNonEmpty(shopifyVariant?.barcode);
  const model = buildModelCandidate(shopifyVariant);
  const type = buildTypeCandidate(shopifyVariant);
  const color = inferColorCandidate(shopifyVariant);
  const size = inferSizeCandidate(shopifyVariant);
  const material = inferMaterialCandidate(shopifyVariant);
  const style = inferStyleCandidate(shopifyVariant);
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);
  const title = firstNonEmpty(shopifyVariant?.product?.title);
  const categoryFullName = firstNonEmpty(shopifyVariant?.product?.categoryFullName);
  const productType = firstNonEmpty(shopifyVariant?.product?.productType);

  if (aspectNameMatches(aspectName, ["brand", "marca"])) {
    return uniqueStrings([vendor, "Generic"]);
  }

  if (aspectNameMatches(aspectName, ["model", "modello"])) {
    return uniqueStrings([model, sku, variantTitle, title]);
  }

  if (aspectNameMatches(aspectName, ["type", "tipo"])) {
    return uniqueStrings([type, productType, categoryFullName, title]);
  }

  if (
    aspectNameMatches(aspectName, [
      "mpn",
      "manufacturer part number",
      "part number",
      "codice produttore",
    ])
  ) {
    return uniqueStrings([sku, model, "Non applicabile"]);
  }

  if (
    aspectNameMatches(aspectName, [
      "ean",
      "gtin",
      "upc",
      "isbn",
      "barcode",
      "bar code",
    ])
  ) {
    return uniqueStrings([barcode, "Non applicabile"]);
  }

  if (aspectNameMatches(aspectName, ["color", "colour", "colore"])) {
    return uniqueStrings([color, variantTitle]);
  }

  if (aspectNameMatches(aspectName, ["size", "taglia", "dimensione", "misura"])) {
    return uniqueStrings([size, variantTitle]);
  }

  if (aspectNameMatches(aspectName, ["material", "materiale"])) {
    return uniqueStrings([material, productType]);
  }

  if (aspectNameMatches(aspectName, ["style", "stile"])) {
    return uniqueStrings([style, variantTitle, productType]);
  }

  if (aspectNameMatches(aspectName, ["shape", "forma"])) {
    return uniqueStrings([variantTitle, productType, title]);
  }

  if (aspectNameMatches(aspectName, ["theme", "tema"])) {
    return uniqueStrings([productType, categoryFullName, title]);
  }

  if (aspectNameMatches(aspectName, ["occasion", "occasione"])) {
    const blob = [
      title,
      productType,
      categoryFullName,
      shopifyVariant?.product?.descriptionText,
    ]
      .map((x) => String(x || "").toLowerCase())
      .join(" ");

    if (blob.includes("battesimo")) return ["Battesimo"];
    if (blob.includes("comunione")) return ["Comunione"];
    if (blob.includes("laurea")) return ["Laurea"];
    if (blob.includes("compleanno") || blob.includes("18")) return ["Compleanno"];
    if (blob.includes("matrimonio")) return ["Matrimonio"];
    if (blob.includes("nascita")) return ["Nascita"];
    return [];
  }

  for (const option of safeArray(shopifyVariant?.selectedOptions)) {
    const optionName = String(option?.name || "").trim();
    const optionValue = String(option?.value || "").trim();
    if (!optionName || !optionValue) continue;

    if (aspectNameMatches(aspectName, [optionName])) {
      return [optionValue];
    }
  }

  return uniqueStrings([
    variantTitle,
    productType,
    categoryFullName,
    title,
    model,
  ]);
}

function chooseBestAspectValue(aspectMeta, candidates = []) {
  const aspectName = String(aspectMeta?.localizedAspectName || "").trim();
  const values = uniqueStrings(candidates);
  if (!values.length) return null;

  const allowedValues = safeArray(aspectMeta?.aspectValues)
    .map((v) => String(v?.localizedValue || "").trim())
    .filter(Boolean);

  const lowerAllowed = new Set(allowedValues.map((v) => v.toLowerCase()));

  const exactAllowed = values.find((v) => lowerAllowed.has(v.toLowerCase()));
  if (exactAllowed) return [exactAllowed];

  if (
    aspectNameMatches(aspectName, ["ean", "gtin", "upc", "isbn", "barcode"])
  ) {
    const validCode = values.find((v) => /^[A-Za-z0-9\- ]{8,20}$/.test(v));
    return [validCode || "Non applicabile"];
  }

  if (
    aspectNameMatches(aspectName, [
      "mpn",
      "manufacturer part number",
      "part number",
      "codice produttore",
    ])
  ) {
    return [values[0] || "Non applicabile"];
  }

  return [truncateText(values[0], 65)];
}

// =========================
// EBAY FILE PERSISTENCE
// =========================

async function ensureEbayStateDir() {
  await fs.mkdir(EBAY_STATE_DIR, { recursive: true });
}

async function saveEbayConnectionToDisk() {
  await ensureEbayStateDir();

  const payload = {
    connected: ebayConnectionStore.connected,
    connectedAt: ebayConnectionStore.connectedAt,
    environment: ebayConnectionStore.environment,
    accessToken: ebayConnectionStore.accessToken,
    refreshToken: ebayConnectionStore.refreshToken,
    accessTokenExpiresAt: ebayConnectionStore.accessTokenExpiresAt,
    refreshTokenExpiresAt: ebayConnectionStore.refreshTokenExpiresAt,
    tokenType: ebayConnectionStore.tokenType,
    scope: ebayConnectionStore.scope,
    userInfo: ebayConnectionStore.userInfo,
    savedAt: nowIso(),
  };

  await fs.writeFile(EBAY_STATE_FILE, JSON.stringify(payload, null, 2), "utf8");
}

async function loadEbayConnectionFromDisk() {
  try {
    await ensureEbayStateDir();
    const raw = await fs.readFile(EBAY_STATE_FILE, "utf8");
    const data = JSON.parse(raw);

    ebayConnectionStore.connected = Boolean(data.connected);
    ebayConnectionStore.connectedAt = data.connectedAt || null;
    ebayConnectionStore.environment = data.environment || EBAY_ENVIRONMENT;
    ebayConnectionStore.accessToken = data.accessToken || null;
    ebayConnectionStore.refreshToken = data.refreshToken || null;
    ebayConnectionStore.accessTokenExpiresAt = data.accessTokenExpiresAt || null;
    ebayConnectionStore.refreshTokenExpiresAt =
      data.refreshTokenExpiresAt || null;
    ebayConnectionStore.tokenType = data.tokenType || null;
    ebayConnectionStore.scope = data.scope || null;
    ebayConnectionStore.userInfo = data.userInfo || null;

    console.log(
      "EBAY STATE LOADED FROM DISK",
      sanitizeEbayConnectionForResponse()
    );
  } catch (error) {
    if (error.code === "ENOENT") {
      console.log("EBAY STATE FILE NOT FOUND, STARTING CLEAN");
      return;
    }

    console.log("EBAY STATE LOAD ERROR", error.message);
  }
}

async function clearEbayConnectionOnDisk() {
  ebayConnectionStore.connected = false;
  ebayConnectionStore.connectedAt = null;
  ebayConnectionStore.accessToken = null;
  ebayConnectionStore.refreshToken = null;
  ebayConnectionStore.accessTokenExpiresAt = null;
  ebayConnectionStore.refreshTokenExpiresAt = null;
  ebayConnectionStore.tokenType = null;
  ebayConnectionStore.scope = null;
  ebayConnectionStore.userInfo = null;

  await saveEbayConnectionToDisk();
}

async function saveEbayListingMapToDisk() {
  await ensureEbayStateDir();

  const payload = {
    savedAt: nowIso(),
    mappings: Object.fromEntries(ebayListingIdMap.entries()),
  };

  await fs.writeFile(
    EBAY_LISTING_MAP_FILE,
    JSON.stringify(payload, null, 2),
    "utf8"
  );
}

async function loadEbayListingMapFromDisk() {
  try {
    await ensureEbayStateDir();

    if (EBAY_LISTING_ID_MAP_JSON) {
      try {
        const envMap = JSON.parse(EBAY_LISTING_ID_MAP_JSON);
        if (isPlainObject(envMap)) {
          for (const [sku, listingId] of Object.entries(envMap)) {
            const safeSku = normalizeSku(sku);
            const safeListingId = normalizeListingId(listingId);
            if (safeSku && safeListingId) {
              ebayListingIdMap.set(safeSku, safeListingId);
            }
          }
        }
      } catch (error) {
        console.log("EBAY LISTING MAP ENV PARSE ERROR", error.message);
      }
    }

    const raw = await fs.readFile(EBAY_LISTING_MAP_FILE, "utf8");
    const data = JSON.parse(raw);
    const mappings = isPlainObject(data?.mappings) ? data.mappings : {};

    for (const [sku, listingId] of Object.entries(mappings)) {
      const safeSku = normalizeSku(sku);
      const safeListingId = normalizeListingId(listingId);
      if (safeSku && safeListingId) {
        ebayListingIdMap.set(safeSku, safeListingId);
      }
    }

    console.log("EBAY LISTING MAP LOADED", sanitizeListingMapForResponse());
  } catch (error) {
    if (error.code === "ENOENT") {
      console.log("EBAY LISTING MAP FILE NOT FOUND, STARTING CLEAN");
      return;
    }

    console.log("EBAY LISTING MAP LOAD ERROR", error.message);
  }
}

async function saveEbayOrderSyncStateToDisk() {
  await ensureEbayStateDir();

  await fs.writeFile(
    EBAY_ORDER_SYNC_FILE,
    JSON.stringify(
      {
        savedAt: nowIso(),
        ...serializeOrderSyncState(),
      },
      null,
      2
    ),
    "utf8"
  );
}

async function loadEbayOrderSyncStateFromDisk() {
  try {
    await ensureEbayStateDir();
    const raw = await fs.readFile(EBAY_ORDER_SYNC_FILE, "utf8");
    const data = JSON.parse(raw);

    ebayOrderSyncState.lastCheckedAt = data?.lastCheckedAt || null;
    ebayOrderSyncState.processedOrderIds = new Set(
      safeArray(data?.processedOrderIds).map((x) => String(x))
    );

    console.log("EBAY ORDER SYNC STATE LOADED", {
      lastCheckedAt: ebayOrderSyncState.lastCheckedAt,
      processedCount: ebayOrderSyncState.processedOrderIds.size,
    });
  } catch (error) {
    if (error.code === "ENOENT") {
      console.log("EBAY ORDER SYNC FILE NOT FOUND, STARTING CLEAN");
      return;
    }

    console.log("EBAY ORDER SYNC LOAD ERROR", error.message);
  }
}

// =========================
// EBAY TOKENS
// =========================

async function ebayTokenRequest(bodyParams) {
  const response = await axios.post(
    `${EBAY_API_BASE}/identity/v1/oauth2/token`,
    new URLSearchParams(bodyParams),
    {
      headers: {
        Authorization: buildEbayBasicAuthHeader(),
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json",
      },
    }
  );

  return response.data;
}

async function getEbayApplicationAccessToken() {
  const response = await ebayTokenRequest({
    grant_type: "client_credentials",
    scope: "https://api.ebay.com/oauth/api_scope",
  });

  if (!response?.access_token) {
    throw new Error("Unable to obtain eBay application access token");
  }

  return response.access_token;
}

async function exchangeEbayCodeForTokens(code) {
  return ebayTokenRequest({
    grant_type: "authorization_code",
    code,
    redirect_uri: EBAY_RU_NAME,
  });
}

async function refreshEbayAccessToken() {
  if (!ebayConnectionStore.refreshToken) {
    throw new Error("No eBay refresh token stored");
  }

  const tokenData = await ebayTokenRequest({
    grant_type: "refresh_token",
    refresh_token: ebayConnectionStore.refreshToken,
    scope: EBAY_SCOPES,
  });

  ebayConnectionStore.connected = true;
  ebayConnectionStore.connectedAt = ebayConnectionStore.connectedAt || nowIso();
  ebayConnectionStore.environment = EBAY_ENVIRONMENT;
  ebayConnectionStore.accessToken = tokenData.access_token || null;
  ebayConnectionStore.accessTokenExpiresAt = futureIsoFromSeconds(
    tokenData.expires_in || 7200
  );
  ebayConnectionStore.tokenType = tokenData.token_type || "User Access Token";
  ebayConnectionStore.scope = tokenData.scope || EBAY_SCOPES;

  if (tokenData.refresh_token) {
    ebayConnectionStore.refreshToken = tokenData.refresh_token;
  }

  if (tokenData.refresh_token_expires_in) {
    ebayConnectionStore.refreshTokenExpiresAt = futureIsoFromSeconds(
      tokenData.refresh_token_expires_in
    );
  }

  await saveEbayConnectionToDisk();

  return sanitizeEbayConnectionForResponse();
}

async function saveEbayTokens(tokenData) {
  ebayConnectionStore.connected = true;
  ebayConnectionStore.connectedAt = nowIso();
  ebayConnectionStore.environment = EBAY_ENVIRONMENT;
  ebayConnectionStore.accessToken = tokenData.access_token || null;
  ebayConnectionStore.refreshToken = tokenData.refresh_token || null;
  ebayConnectionStore.accessTokenExpiresAt = futureIsoFromSeconds(
    tokenData.expires_in || 7200
  );
  ebayConnectionStore.refreshTokenExpiresAt = tokenData.refresh_token_expires_in
    ? futureIsoFromSeconds(tokenData.refresh_token_expires_in)
    : null;
  ebayConnectionStore.tokenType = tokenData.token_type || "User Access Token";
  ebayConnectionStore.scope = tokenData.scope || EBAY_SCOPES;

  await saveEbayConnectionToDisk();
}

async function getEbayUserInfo(accessToken) {
  try {
    const response = await axios.get(
      `${EBAY_API_BASE}/commerce/identity/v1/user/`,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Accept: "application/json",
        },
      }
    );

    return response.data;
  } catch (error) {
    console.log("EBAY USER INFO ERROR", error.response?.data || error.message);
    return null;
  }
}

async function ensureValidEbayAccessToken() {
  validateEbayEnv();

  if (!ebayConnectionStore.connected || !ebayConnectionStore.accessToken) {
    if (ebayConnectionStore.refreshToken) {
      await refreshEbayAccessToken();
    } else {
      throw new Error("eBay account not connected");
    }
  }

  const expiresAt = ebayConnectionStore.accessTokenExpiresAt
    ? new Date(ebayConnectionStore.accessTokenExpiresAt).getTime()
    : 0;

  const willExpireSoon = Date.now() > expiresAt - 60 * 1000;

  if (willExpireSoon) {
    await refreshEbayAccessToken();
  }

  return ebayConnectionStore.accessToken;
}

async function ebayGet(pathname, accessToken, params = {}, extraHeaders = {}) {
  const response = await axios.get(
    `${EBAY_API_BASE}${pathname}${getQueryString(params)}`,
    {
      headers: buildEbayApiHeaders(accessToken, extraHeaders),
    }
  );

  return response.data;
}

async function ebayPost(
  pathname,
  accessToken,
  body,
  params = {},
  extraHeaders = {}
) {
  const response = await axios.post(
    `${EBAY_API_BASE}${pathname}${getQueryString(params)}`,
    body,
    {
      headers: buildEbayApiHeaders(accessToken, extraHeaders),
    }
  );

  return response.data;
}

async function ebayPut(
  pathname,
  accessToken,
  body,
  params = {},
  extraHeaders = {}
) {
  const response = await axios.put(
    `${EBAY_API_BASE}${pathname}${getQueryString(params)}`,
    body,
    {
      headers: buildEbayApiHeaders(accessToken, extraHeaders),
      validateStatus: (status) => status >= 200 && status < 300,
    }
  );

  return {
