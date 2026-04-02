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
    "https://api.ebay.com/oauth/api_scope/commerce.taxonomy.readonly",
    "https://api.ebay.com/oauth/api_scope/commerce.translation",
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

function getSafeOfferIds(offers = []) {
  return offers
    .map((offer) => String(offer?.offerId || "").trim())
    .filter(Boolean);
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

function extractNumericOrderId(value) {
  return String(value || "")
    .trim()
    .replace(/[^\d]/g, "");
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
    scope:
      [
        "https://api.ebay.com/oauth/api_scope",
        "https://api.ebay.com/oauth/api_scope/commerce.taxonomy.readonly",
        "https://api.ebay.com/oauth/api_scope/commerce.translation",
      ].join(" "),
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
    status: response.status,
    data: response.data,
    headers: response.headers,
  };
}

async function ebayPostNoBody(
  pathname,
  accessToken,
  params = {},
  extraHeaders = {}
) {
  const response = await axios.post(
    `${EBAY_API_BASE}${pathname}${getQueryString(params)}`,
    null,
    {
      headers: buildEbayApiHeaders(accessToken, extraHeaders),
      validateStatus: (status) => status >= 200 && status < 300,
    }
  );

  return {
    status: response.status,
    data: response.data,
    headers: response.headers,
  };
}

// =========================
// EBAY TRANSLATION
// =========================

async function translateTextWithEbay({
  from,
  to,
  text,
  translationContext,
}) {
  const original = String(text || "").trim();
  if (!original) return "";

  if (from === to) return original;

  const appToken = await getEbayApplicationAccessToken();

  const response = await axios.post(
    `${EBAY_API_BASE}/commerce/translation/v1_beta/translate`,
    {
      from,
      to,
      text: [original],
      translationContext,
    },
    {
      headers: {
        Authorization: `Bearer ${appToken}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
    }
  );

  return response.data?.translations?.[0]?.translatedText || original;
}

async function buildMarketplaceTranslations({
  sourceLanguage,
  title,
  descriptionHtml,
}) {
  const result = {};

  for (const market of MARKETPLACES) {
    const translatedTitle = await translateTextWithEbay({
      from: sourceLanguage,
      to: market.language,
      text: title,
      translationContext: "ITEM_TITLE",
    });

    const translatedDescription = await translateTextWithEbay({
      from: sourceLanguage,
      to: market.language,
      text: descriptionHtml,
      translationContext: "ITEM_DESCRIPTION",
    });

    result[market.marketplaceId] = {
      marketplaceId: market.marketplaceId,
      locale: market.locale,
      language: market.language,
      site: market.site,
      translatedTitle,
      translatedDescription,
    };
  }

  return result;
}

// =========================
// EBAY ACCOUNT / LOCATION / TAXONOMY
// =========================

async function getEbayFulfillmentPolicies(marketplaceId) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayGet(
    "/sell/account/v1/fulfillment_policy",
    accessToken,
    { marketplace_id: marketplaceId }
  );
}

async function getEbayPaymentPolicies(marketplaceId) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayGet(
    "/sell/account/v1/payment_policy",
    accessToken,
    { marketplace_id: marketplaceId }
  );
}

async function getEbayReturnPolicies(marketplaceId) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayGet(
    "/sell/account/v1/return_policy",
    accessToken,
    { marketplace_id: marketplaceId }
  );
}

async function getAllEbayPolicies(marketplaceId) {
  const [fulfillmentPolicies, paymentPolicies, returnPolicies] =
    await Promise.all([
      getEbayFulfillmentPolicies(marketplaceId),
      getEbayPaymentPolicies(marketplaceId),
      getEbayReturnPolicies(marketplaceId),
    ]);

  return {
    marketplaceId,
    fulfillmentPolicies,
    paymentPolicies,
    returnPolicies,
  };
}

function pickBestPolicy(items = [], idKey) {
  if (!Array.isArray(items) || !items.length) return null;

  const preferred =
    items.find((p) => p?.categoryTypes?.some((c) => c?.default === true)) ||
    items.find((p) => p?.name && /default/i.test(String(p.name))) ||
    items[0];

  return preferred?.[idKey] || null;
}

function pickDefaultPolicyIds(policiesData) {
  const fulfillmentPolicies =
    policiesData?.fulfillmentPolicies?.fulfillmentPolicies || [];
  const paymentPolicies = policiesData?.paymentPolicies?.paymentPolicies || [];
  const returnPolicies = policiesData?.returnPolicies?.returnPolicies || [];

  const fulfillmentPolicyId = pickBestPolicy(
    fulfillmentPolicies,
    "fulfillmentPolicyId"
  );
  const paymentPolicyId = pickBestPolicy(paymentPolicies, "paymentPolicyId");
  const returnPolicyId = pickBestPolicy(returnPolicies, "returnPolicyId");

  return {
    paymentPolicyId,
    returnPolicyId,
    fulfillmentPolicyId,
  };
}

async function getEbayInventoryLocations() {
  const accessToken = await ensureValidEbayAccessToken();
  return ebayGet("/sell/inventory/v1/location", accessToken);
}

async function getEbayInventoryLocation(merchantLocationKey) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayGet(
    `/sell/inventory/v1/location/${encodeURIComponent(merchantLocationKey)}`,
    accessToken
  );
}

async function getDefaultCategoryTreeId(marketplaceId) {
  if (ebayCategoryTreeCache.has(marketplaceId)) {
    return ebayCategoryTreeCache.get(marketplaceId);
  }

  const appAccessToken = await getEbayApplicationAccessToken();

  const result = await ebayGet(
    "/commerce/taxonomy/v1/get_default_category_tree_id",
    appAccessToken,
    { marketplace_id: marketplaceId }
  );

  const treeId = result?.categoryTreeId || null;
  if (treeId) {
    ebayCategoryTreeCache.set(marketplaceId, result);
  }

  return result;
}

async function getCategorySuggestions({ categoryTreeId, q }) {
  const appAccessToken = await getEbayApplicationAccessToken();

  return ebayGet(
    `/commerce/taxonomy/v1/category_tree/${encodeURIComponent(
      categoryTreeId
    )}/get_category_suggestions`,
    appAccessToken,
    { q }
  );
}

async function getCategoryAspects({ categoryTreeId, categoryId }) {
  const cacheKey = `${categoryTreeId}::${categoryId}`;
  if (ebayCategoryAspectsCache.has(cacheKey)) {
    return ebayCategoryAspectsCache.get(cacheKey);
  }

  const appAccessToken = await getEbayApplicationAccessToken();

  const result = await ebayGet(
    `/commerce/taxonomy/v1/category_tree/${encodeURIComponent(
      categoryTreeId
    )}/get_item_aspects_for_category`,
    appAccessToken,
    { category_id: categoryId }
  );

  ebayCategoryAspectsCache.set(cacheKey, result);
  return result;
}

async function suggestEbayCategory({
  marketplaceId,
  title,
  shopifyCategory,
  productType,
}) {
  const tree = await getDefaultCategoryTreeId(marketplaceId);
  const categoryTreeId = tree?.categoryTreeId;

  if (!categoryTreeId) {
    throw new Error("Could not determine eBay default category tree");
  }

  const queries = [
    firstNonEmpty(shopifyCategory),
    firstNonEmpty(productType),
    firstNonEmpty(title),
  ].filter(Boolean);

  const allSuggestions = [];

  for (const q of queries) {
    const suggestions = await getCategorySuggestions({ categoryTreeId, q });
    const rows = safeArray(suggestions?.categorySuggestions).map((row) => ({
      query: q,
      categoryId: row?.category?.categoryId || row?.categoryId || null,
      categoryName: row?.category?.categoryName || row?.categoryName || null,
      categoryTreeId,
    }));

    allSuggestions.push(...rows);
  }

  const deduped = [];
  const seen = new Set();

  for (const row of allSuggestions) {
    const key = `${row.categoryId}::${row.categoryName}`;
    if (row.categoryId && !seen.has(key)) {
      seen.add(key);
      deduped.push(row);
    }
  }

  return {
    categoryTreeId,
    suggestions: deduped,
    best: deduped[0] || null,
  };
}

async function resolveCategoryIdForMarketplace({
  sku,
  marketplaceId,
  shopifyVariant,
}) {
  const configured = getConfiguredCategoryForMarketplace(sku, marketplaceId);
  if (configured) return configured;

  const suggestion = await suggestEbayCategory({
    marketplaceId,
    title: shopifyVariant.product.title,
    shopifyCategory: shopifyVariant.product.categoryFullName,
    productType: shopifyVariant.product.productType,
  });

  const categoryId = String(suggestion?.best?.categoryId || "").trim();
  if (!categoryId) {
    throw new Error(`No category found for ${marketplaceId}`);
  }

  return categoryId;
}

// =========================
// SHOPIFY HELPERS
// =========================

async function getShopifyAccessToken() {
  const response = await axios.post(
    `https://${SHOPIFY_SHOP_DOMAIN}/admin/oauth/access_token`,
    new URLSearchParams({
      client_id: SHOPIFY_CLIENT_ID,
      client_secret: SHOPIFY_CLIENT_SECRET,
      grant_type: "client_credentials",
    }),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json",
      },
    }
  );

  const token = response.data?.access_token;
  if (!token) {
    throw new Error(
      `Missing Shopify access token: ${JSON.stringify(response.data)}`
    );
  }

  return token;
}

async function shopifyGraphQL(query, variables = {}) {
  const token = await getShopifyAccessToken();

  const response = await axios.post(
    `https://${SHOPIFY_SHOP_DOMAIN}/admin/api/2026-01/graphql.json`,
    { query, variables },
    {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
      },
    }
  );

  if (response.data.errors) {
    throw new Error(JSON.stringify(response.data.errors));
  }

  return response.data.data;
}

async function findShopifyInventoryItemBySku(sku) {
  const query = `
    query FindInventoryItemBySku($query: String!) {
      inventoryItems(first: 1, query: $query) {
        edges {
          node {
            id
            sku
            tracked
          }
        }
      }
    }
  `;

  const data = await shopifyGraphQL(query, {
    query: `sku:${sku}`,
  });

  const edge = data?.inventoryItems?.edges?.[0];
  if (!edge) return null;

  return {
    inventoryItemId: edge.node.id,
    sku: edge.node.sku,
    tracked: edge.node.tracked,
  };
}

async function getShopifyVariantBySku(sku) {
  const query = `
    query GetVariantBySku($query: String!) {
      productVariants(first: 1, query: $query) {
        nodes {
          id
          sku
          price
          inventoryQuantity
          title
          barcode
          selectedOptions {
            name
            value
          }
          product {
            id
            title
            descriptionHtml
            vendor
            productType
            onlineStoreUrl
            category {
              fullName
            }
            images(first: 10) {
              nodes {
                url
                altText
              }
            }
          }
        }
      }
    }
  `;

  const data = await shopifyGraphQL(query, {
    query: `sku:${sku}`,
  });

  const variant = data?.productVariants?.nodes?.[0] || null;
  if (!variant) return null;

  const images = safeArray(variant.product?.images?.nodes)
    .map((img) => img?.url)
    .filter(Boolean);

  const descriptionHtml = cleanHtmlForEbay(variant.product?.descriptionHtml || "");
  const descriptionText = truncateText(stripHtml(descriptionHtml), 4000);

  return {
    sku: variant.sku,
    barcode: variant.barcode || "",
    price: normalizeMoney(variant.price),
    inventoryQuantity: Number(variant.inventoryQuantity || 0),
    variantTitle: variant.title || "",
    selectedOptions: variant.selectedOptions || [],
    product: {
      id: variant.product?.id || null,
      title: variant.product?.title || "",
      descriptionHtml,
      descriptionText,
      vendor: variant.product?.vendor || "",
      productType: variant.product?.productType || "",
      categoryFullName: variant.product?.category?.fullName || "",
      onlineStoreUrl: variant.product?.onlineStoreUrl || "",
      imageUrls: images,
    },
  };
}

// =========================
// EBAY INVENTORY / OFFER HELPERS
// =========================

function buildDefaultAspects(shopifyVariant) {
  const aspects = {};

  const vendor = firstNonEmpty(shopifyVariant?.product?.vendor, "Generic");
  aspects.Brand = [vendor];

  const productType = firstNonEmpty(
    shopifyVariant?.product?.productType,
    shopifyVariant?.product?.categoryFullName,
    "General"
  );
  aspects.Type = [productType];

  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);
  if (
    variantTitle &&
    !["Default Title", "Titolo predefinito"].includes(variantTitle)
  ) {
    aspects.Style = [variantTitle];
  }

  const modelCandidate = buildModelCandidate(shopifyVariant);
  if (modelCandidate) {
    aspects.Model = [modelCandidate];
  }

  if (shopifyVariant?.sku) {
    aspects.MPN = [String(shopifyVariant.sku)];
  }

  if (shopifyVariant?.barcode) {
    aspects.EAN = [String(shopifyVariant.barcode)];
  }

  const inferredColor = inferColorCandidate(shopifyVariant);
  if (inferredColor) aspects.Color = [inferredColor];

  const inferredSize = inferSizeCandidate(shopifyVariant);
  if (inferredSize) aspects.Size = [inferredSize];

  const inferredMaterial = inferMaterialCandidate(shopifyVariant);
  if (inferredMaterial) aspects.Material = [inferredMaterial];

  for (const option of safeArray(shopifyVariant?.selectedOptions)) {
    const name = firstNonEmpty(option?.name);
    const value = firstNonEmpty(option?.value);
    if (name && value && !aspects[name]) {
      aspects[name] = [value];
    }
  }

  return aspects;
}

async function buildDynamicAspectsForCategory({
  shopifyVariant,
  marketplaceId,
  categoryId,
}) {
  const tree = await getDefaultCategoryTreeId(marketplaceId);
  const categoryTreeId = tree?.categoryTreeId;

  if (!categoryTreeId) {
    return {
      aspects: buildDefaultAspects(shopifyVariant),
      aspectsMeta: [],
      categoryTreeId: null,
    };
  }

  const aspectsResponse = await getCategoryAspects({
    categoryTreeId,
    categoryId,
  });

  const aspectsMeta = safeArray(aspectsResponse?.aspects);
  const dynamicAspects = {};

  for (const aspectMeta of aspectsMeta) {
    const aspectName = String(aspectMeta?.localizedAspectName || "").trim();
    if (!aspectName) continue;

    const candidates = buildAspectCandidates(aspectName, shopifyVariant);
    const selectedValues = chooseBestAspectValue(aspectMeta, candidates);

    if (selectedValues?.length) {
      dynamicAspects[aspectName] = selectedValues;
    }
  }

  const fallbackDefaults = buildDefaultAspects(shopifyVariant);

  for (const [aspectName, values] of Object.entries(fallbackDefaults)) {
    if (!dynamicAspects[aspectName] && safeArray(values).length) {
      dynamicAspects[aspectName] = values;
    }
  }

  return {
    aspects: dynamicAspects,
    aspectsMeta,
    categoryTreeId,
  };
}

function buildInventoryItemPayload({
  shopifyVariant,
  translatedTitle = "",
  translatedDescription = "",
  dynamicAspects = null,
}) {
  const titleBase = firstNonEmpty(translatedTitle, shopifyVariant?.product?.title);
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);

  const title =
    variantTitle &&
    !["Default Title", "Titolo predefinito"].includes(variantTitle)
      ? `${titleBase} - ${variantTitle}`.slice(0, 80)
      : titleBase.slice(0, 80);

  const description = firstNonEmpty(
    stripHtml(translatedDescription),
    shopifyVariant?.product?.descriptionText,
    titleBase,
    shopifyVariant?.sku
  );

  const imageUrls = shopifyVariant?.product?.imageUrls || [];
  if (!imageUrls.length) {
    throw new Error("Shopify product has no images. eBay publish requires images.");
  }

  const payload = {
    availability: {
      shipToLocationAvailability: {
        quantity: Math.max(0, Number(shopifyVariant?.inventoryQuantity || 0)),
      },
    },
    condition: EBAY_DEFAULT_CONDITION,
    product: {
      title,
      description,
      aspects: dynamicAspects || buildDefaultAspects(shopifyVariant),
      imageUrls,
    },
  };

  if (shopifyVariant?.barcode) {
    payload.product.ean = [String(shopifyVariant.barcode)];
  }

  return payload;
}

async function getEbayInventoryItem(sku) {
  const accessToken = await ensureValidEbayAccessToken();
  return ebayGet(
    `/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`,
    accessToken
  );
}

async function createOrReplaceInventoryItem({
  sku,
  payload,
  contentLanguage,
}) {
  const accessToken = await ensureValidEbayAccessToken();
  const safeContentLanguage = normalizeContentLanguage(contentLanguage, "it-IT");

  return ebayPut(
    `/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`,
    accessToken,
    payload,
    {},
    {
      "Content-Language": safeContentLanguage,
    }
  );
}

async function getOffersBySku({ sku, marketplaceId, format }) {
  const accessToken = await ensureValidEbayAccessToken();

  const params = {
    sku,
    limit: 200,
    offset: 0,
  };

  if (marketplaceId) {
    params.marketplace_id = marketplaceId;
  }

  if (format) {
    params.format = format;
  }

  const result = await ebayGet("/sell/inventory/v1/offer", accessToken, params);
  return result?.offers || [];
}

async function getOffer(offerId) {
  const accessToken = await ensureValidEbayAccessToken();
  return ebayGet(
    `/sell/inventory/v1/offer/${encodeURIComponent(offerId)}`,
    accessToken
  );
}

async function createOffer({
  payload,
  contentLanguage,
}) {
  const accessToken = await ensureValidEbayAccessToken();
  const safeContentLanguage = normalizeContentLanguage(contentLanguage, "it-IT");

  return ebayPost(
    "/sell/inventory/v1/offer",
    accessToken,
    payload,
    {},
    {
      "Content-Language": safeContentLanguage,
    }
  );
}

async function updateOffer({
  offerId,
  payload,
  contentLanguage,
}) {
  const accessToken = await ensureValidEbayAccessToken();
  const safeContentLanguage = normalizeContentLanguage(contentLanguage, "it-IT");

  const response = await axios.put(
    `${EBAY_API_BASE}/sell/inventory/v1/offer/${encodeURIComponent(offerId)}`,
    payload,
    {
      headers: buildEbayApiHeaders(accessToken, {
        "Content-Language": safeContentLanguage,
      }),
      validateStatus: (status) => status >= 200 && status < 300,
    }
  );

  return {
    status: response.status,
    data: response.data,
    headers: response.headers,
  };
}

async function publishOffer({ offerId }) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayPostNoBody(
    `/sell/inventory/v1/offer/${encodeURIComponent(offerId)}/publish`,
    accessToken
  );
}

async function bulkMigrateListings({ requests }) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayPost(
    "/sell/inventory/v1/bulk_migrate_listing",
    accessToken,
    { requests }
  );
}

async function bulkUpdatePriceQuantity({ requests }) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayPost(
    "/sell/inventory/v1/bulk_update_price_quantity",
    accessToken,
    { requests }
  );
}

async function migrateListingToInventoryModel({
  sku,
  listingId,
}) {
  const safeSku = normalizeSku(sku);
  const safeListingId = normalizeListingId(listingId);

  if (!safeSku) {
    throw new Error("migrateListingToInventoryModel: missing sku");
  }
  if (!safeListingId) {
    throw new Error("migrateListingToInventoryModel: missing listingId");
  }

  const migrationResult = await bulkMigrateListings({
    requests: [{ listingId: safeListingId }],
  });

  const responses = Array.isArray(migrationResult?.responses)
    ? migrationResult.responses
    : [];
  const first = responses[0] || null;

  if (!first) {
    throw new Error(
      `bulk_migrate_listing returned no responses for listingId ${safeListingId}`
    );
  }

  const errors = Array.isArray(first.errors) ? first.errors : [];
  const ok =
    Number(first.statusCode || 0) >= 200 &&
    Number(first.statusCode || 0) < 300 &&
    errors.length === 0;

  if (!ok) {
    throw new Error(JSON.stringify(first));
  }

  setPersistedListingIdForSku(safeSku, safeListingId);
  await saveEbayListingMapToDisk();

  return {
    ok: true,
    sku: safeSku,
    listingId: safeListingId,
    migrationResult,
  };
}

async function bulkUpdatePublishedOffersForSku({
  sku,
  price,
  quantity,
  offers,
}) {
  const safeSku = normalizeSku(sku);

  if (!safeSku) {
    throw new Error("bulkUpdatePublishedOffersForSku: missing sku");
  }

  if (!offers.length) {
    throw new Error("bulkUpdatePublishedOffersForSku: no offerIds to update");
  }

  const requests = [
    {
      sku: safeSku,
      shipToLocationAvailability: {
        quantity: Math.max(0, Number(quantity || 0)),
      },
      offers: offers
        .filter((offer) => offer?.offerId)
        .map((offer) => {
          const meta = getMarketplaceMeta(offer?.marketplaceId);
          return {
            offerId: String(offer.offerId),
            availableQuantity: Math.max(0, Number(quantity || 0)),
            price: {
              value: String(normalizeMoney(price)),
              currency:
                offer?.pricingSummary?.price?.currency ||
                meta?.currency ||
                "EUR",
            },
          };
        }),
    },
  ];

  return bulkUpdatePriceQuantity({ requests });
}

function buildOfferPayload({
  sku,
  price,
  quantity,
  marketplaceId,
  categoryId,
  policyIds,
  listingDescription,
  currency,
}) {
  if (!policyIds?.paymentPolicyId) {
    throw new Error(`Missing paymentPolicyId for ${marketplaceId}`);
  }
  if (!policyIds?.returnPolicyId) {
    throw new Error(`Missing returnPolicyId for ${marketplaceId}`);
  }
  if (!policyIds?.fulfillmentPolicyId) {
    throw new Error(`Missing fulfillmentPolicyId for ${marketplaceId}`);
  }
  if (!categoryId) {
    throw new Error(`Missing categoryId for ${marketplaceId}`);
  }

  return {
    sku,
    marketplaceId,
    format: EBAY_DEFAULT_FORMAT,
    availableQuantity: Math.max(0, Number(quantity || 0)),
    categoryId,
    merchantLocationKey: getMerchantLocationKey(marketplaceId),
    pricingSummary: {
      price: {
        value: String(normalizeMoney(price)),
        currency: currency || "EUR",
      },
    },
    listingPolicies: {
      paymentPolicyId: policyIds.paymentPolicyId,
      returnPolicyId: policyIds.returnPolicyId,
      fulfillmentPolicyId: policyIds.fulfillmentPolicyId,
    },
    listingDuration: EBAY_DEFAULT_LISTING_DURATION,
    listingDescription: cleanHtmlForEbay(listingDescription || ""),
  };
}

async function upsertOfferForMarketplace({
  sku,
  price,
  quantity,
  marketplaceId,
  categoryId,
  translatedDescription,
}) {
  const meta = getMarketplaceMeta(marketplaceId);
  if (!meta) {
    throw new Error(`Unknown marketplaceId: ${marketplaceId}`);
  }

  const contentLanguage = normalizeContentLanguage(meta.locale, "it-IT");

  const policies = await getAllEbayPolicies(marketplaceId);
  const policyIds = pickDefaultPolicyIds(policies);

  const payload = buildOfferPayload({
    sku,
    price,
    quantity,
    marketplaceId,
    categoryId,
    policyIds,
    listingDescription: translatedDescription,
    currency: meta.currency,
  });

  let existingOffers = [];

  try {
    existingOffers = await getOffersBySku({ sku, marketplaceId });
  } catch (error) {
    if (isEbayOfferUnavailableError(error)) {
      existingOffers = [];
    } else {
      throw error;
    }
  }

  const existing =
    existingOffers.find((offer) => offer.marketplaceId === marketplaceId) || null;

  if (existing?.offerId) {
    const updateResult = await updateOffer({
      offerId: existing.offerId,
      payload,
      contentLanguage,
    });

    return {
      mode: "updated",
      offerId: existing.offerId,
      payload,
      contentLanguage,
      updateResult,
    };
  }

  const createResult = await createOffer({
    payload,
    contentLanguage,
  });
  const offerId = createResult?.offerId;

  if (!offerId) {
    throw new Error(
      `Offer create response missing offerId for ${marketplaceId}: ${JSON.stringify(
        createResult
      )}`
    );
  }

  return {
    mode: "created",
    offerId,
    payload,
    contentLanguage,
    createResult,
  };
}

async function ensureInventoryItemForMarketplace({
  sku,
  shopifyVariant,
  translation,
  marketplaceId,
  categoryId,
}) {
  const contentLanguage = normalizeContentLanguage(translation?.locale, "it-IT");

  const dynamicAspectResult = await buildDynamicAspectsForCategory({
    shopifyVariant,
    marketplaceId,
    categoryId,
  });

  const inventoryItemPayload = buildInventoryItemPayload({
    shopifyVariant,
    translatedTitle: translation?.translatedTitle || shopifyVariant.product.title,
    translatedDescription:
      translation?.translatedDescription || shopifyVariant.product.descriptionText,
    dynamicAspects: dynamicAspectResult.aspects,
  });

  const inventoryItemUpdate = await createOrReplaceInventoryItem({
    sku,
    payload: inventoryItemPayload,
    contentLanguage,
  });

  return {
    contentLanguage,
    inventoryItemPayload,
    inventoryItemUpdate,
    dynamicAspects: dynamicAspectResult.aspects,
    aspectsMeta: dynamicAspectResult.aspectsMeta,
  };
}

async function publishOrUpdateSkuOnMarketplace({
  sku,
  shopifyVariant,
  marketplaceId,
  sourceLanguage = "it",
  categoryId = "",
}) {
  const meta = getMarketplaceMeta(marketplaceId);
  if (!meta) {
    throw new Error(`Unknown marketplace: ${marketplaceId}`);
  }

  const translation = (
    await buildMarketplaceTranslations({
      sourceLanguage,
      title: shopifyVariant.product.title,
      descriptionHtml:
        shopifyVariant.product.descriptionHtml || shopifyVariant.product.descriptionText,
    })
  )[marketplaceId];

  const finalCategoryId =
    String(categoryId || "").trim() ||
    (await resolveCategoryIdForMarketplace({
      sku,
      marketplaceId,
      shopifyVariant,
    }));

  const inventoryData = await ensureInventoryItemForMarketplace({
    sku,
    shopifyVariant,
    translation,
    marketplaceId,
    categoryId: finalCategoryId,
  });

  const upsert = await upsertOfferForMarketplace({
    sku,
    price: shopifyVariant.price,
    quantity: shopifyVariant.inventoryQuantity,
    marketplaceId,
    categoryId: finalCategoryId,
    translatedDescription:
      translation?.translatedDescription || shopifyVariant.product.descriptionText,
  });

  const publishResult = await publishOffer({ offerId: upsert.offerId });

  return {
    ok: true,
    sku,
    marketplaceId,
    locale: meta.locale,
    site: meta.site,
    currency: meta.currency,
    categoryId: finalCategoryId,
    contentLanguage: upsert.contentLanguage,
    translatedTitlePreview: translation?.translatedTitle || shopifyVariant.product.title,
    translatedDescriptionPreview:
      translation?.translatedDescription || shopifyVariant.product.descriptionText,
    inventoryItemUpdate: inventoryData.inventoryItemUpdate,
    inventoryItemPayload: inventoryData.inventoryItemPayload,
    dynamicAspects: inventoryData.dynamicAspects,
    offerMode: upsert.mode,
    offerId: upsert.offerId,
    publishResult,
  };
}

async function publishSkuToMultipleEbayMarkets({
  sku,
  sourceLanguage = "it",
  categoryMap = {},
  marketplaces = MARKETPLACES.map((m) => m.marketplaceId),
}) {
  const shopifyVariant = await getShopifyVariantBySku(sku);
  if (!shopifyVariant) {
    throw new Error(`Shopify SKU not found: ${sku}`);
  }

  const marketsResult = [];

  for (const marketplaceId of marketplaces) {
    const market = getMarketplaceMeta(marketplaceId);
    if (!market) {
      marketsResult.push({
        marketplaceId,
        ok: false,
        error: "unknown marketplace",
      });
      continue;
    }

    try {
      const result = await publishOrUpdateSkuOnMarketplace({
        sku,
        shopifyVariant,
        marketplaceId,
        sourceLanguage,
        categoryId: String(categoryMap?.[marketplaceId] || "").trim(),
      });

      marketsResult.push(result);
    } catch (error) {
      marketsResult.push({
        marketplaceId,
        locale: market.locale,
        site: market.site,
        ok: false,
        error: errorToSerializable(error),
      });
    }
  }

  return {
    ok: marketsResult.every((m) => m.ok),
    sku,
    sourceLanguage,
    shopify: {
      title: shopifyVariant.product.title,
      descriptionText: shopifyVariant.product.descriptionText,
      vendor: shopifyVariant.product.vendor,
      productType: shopifyVariant.product.productType,
      categoryFullName: shopifyVariant.product.categoryFullName,
      price: shopifyVariant.price,
      inventoryQuantity: shopifyVariant.inventoryQuantity,
      imageCount: shopifyVariant.product.imageUrls.length,
    },
    markets: marketsResult,
  };
}

// =========================
// EBAY ORDER HELPERS
// =========================

async function getEbayOrders({
  filter,
  limit = 100,
  offset = 0,
}) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayGet(
    "/sell/fulfillment/v1/order",
    accessToken,
    {
      filter,
      limit,
      offset,
    }
  );
}

async function getEbayOrder(orderId) {
  const accessToken = await ensureValidEbayAccessToken();

  return ebayGet(
    `/sell/fulfillment/v1/order/${encodeURIComponent(orderId)}`,
    accessToken
  );
}

function buildEbayOrderFilterFromMinutes(minutes) {
  const start = isoMinutesAgo(minutes);
  return `creationdate:[${start}..]`;
}

function extractSkuQtyPairsFromEbayOrder(order) {
  const pairs = [];

  for (const line of safeArray(order?.lineItems)) {
    const sku = normalizeSku(line?.sku);
    const qty = Number(line?.quantity || 0);

    if (sku && qty > 0) {
      pairs.push({
        sku,
        qty,
        lineItemId: line?.lineItemId || null,
        title: line?.title || "",
      });
    }
  }

  return pairs;
}

async function processEbayOrderById(orderId) {
  const safeOrderId = String(orderId || "").trim();
  if (!safeOrderId) {
    return { ok: false, error: "missing orderId" };
  }

  if (ebayOrderSyncState.processedOrderIds.has(safeOrderId)) {
    return {
      ok: true,
      skipped: true,
      reason: "already_processed",
      orderId: safeOrderId,
    };
  }

  const order = await getEbayOrder(safeOrderId);
  const items = extractSkuQtyPairsFromEbayOrder(order);

  for (const item of items) {
    await adjustShopifyInventoryBySku({
      sku: item.sku,
      delta: -Math.abs(item.qty),
      reason: "correction",
      referenceDocumentUri: `gid://syncamzeby/ebay-order/${safeOrderId}`,
    });
  }

  ebayOrderSyncState.processedOrderIds.add(safeOrderId);
  ebayOrderSyncState.lastCheckedAt = nowIso();
  await saveEbayOrderSyncStateToDisk();

  return {
    ok: true,
    orderId: safeOrderId,
    itemCount: items.length,
    items,
  };
}

async function pullAndProcessRecentEbayOrders({
  minutes = EBAY_ORDER_PULL_WINDOW_MINUTES,
}) {
  const filter = buildEbayOrderFilterFromMinutes(minutes);
  const result = await getEbayOrders({ filter, limit: 100, offset: 0 });

  const orders = safeArray(result?.orders);
  const processed = [];

  for (const order of orders) {
    const orderId = String(order?.orderId || "").trim();
    if (!orderId) continue;

    try {
      const row = await processEbayOrderById(orderId);
      processed.push(row);
    } catch (error) {
      processed.push({
        ok: false,
        orderId,
        error: errorToSerializable(error),
      });
    }
  }

  ebayOrderSyncState.lastCheckedAt = nowIso();
  await saveEbayOrderSyncStateToDisk();

  return {
    ok: true,
    filter,
    received: orders.length,
    processedCount: processed.length,
    processed,
  };
}

// =========================
// EBAY SYNC FROM SHOPIFY
// =========================

async function syncShopifySkuToEbay({
  sku,
  sourceLanguage = "it",
  listingId = null,
  autoPublishAllMarkets = true,
}) {
  const safeSku = normalizeSku(sku);
  const safeListingId = normalizeListingId(
    listingId || getPersistedListingIdForSku(safeSku)
  );

  const shopifyVariant = await getShopifyVariantBySku(safeSku);
  if (!shopifyVariant) {
    return { ok: false, sku: safeSku, error: `Shopify SKU not found: ${safeSku}` };
  }

  const defaultMarketplaceId = EBAY_DEFAULT_MARKETPLACE_ID;
  const inventoryContentLanguage = normalizeContentLanguage(
    languageToLocale(sourceLanguage),
    "it-IT"
  );

  let inventoryItemUpdate = null;
  let inventoryItemPayload = null;
  let inventoryDynamicAspects = null;
  let inventoryCategoryId = null;

  try {
    inventoryCategoryId = await resolveCategoryIdForMarketplace({
      sku: safeSku,
      marketplaceId: defaultMarketplaceId,
      shopifyVariant,
    });

    const dynamicAspectResult = await buildDynamicAspectsForCategory({
      shopifyVariant,
      marketplaceId: defaultMarketplaceId,
      categoryId: inventoryCategoryId,
    });

    inventoryDynamicAspects = dynamicAspectResult.aspects;

    inventoryItemPayload = buildInventoryItemPayload({
      shopifyVariant,
      translatedTitle: shopifyVariant.product.title,
      translatedDescription:
        shopifyVariant.product.descriptionText ||
        shopifyVariant.product.descriptionHtml,
      dynamicAspects: inventoryDynamicAspects,
    });

    inventoryItemUpdate = await createOrReplaceInventoryItem({
      sku: safeSku,
      payload: inventoryItemPayload,
      contentLanguage: inventoryContentLanguage,
    });
  } catch (error) {
    return {
      ok: false,
      sku: safeSku,
      failedStep: "inventory_item_upsert",
      inventoryContentLanguage,
      inventoryCategoryId,
      error: errorToSerializable(error),
    };
  }

  let offers = [];
  let initialOfferLookupError = null;
  let initialOfferLookupUnavailable25713 = false;

  try {
    offers = await getOffersBySku({ sku: safeSku });
  } catch (error) {
    initialOfferLookupError = errorToSerializable(error);
    initialOfferLookupUnavailable25713 = isEbayOfferUnavailableError(error);
    offers = [];
  }

  let migrationAttempted = false;
  let migrationResult = null;
  let postMigrationOfferLookupError = null;

  if (!offers.length && safeListingId) {
    try {
      migrationAttempted = true;
      migrationResult = await migrateListingToInventoryModel({
        sku: safeSku,
        listingId: safeListingId,
      });
      offers = await getOffersBySku({ sku: safeSku });
    } catch (error) {
      postMigrationOfferLookupError = errorToSerializable(error);
    }
  }

  let autoPublishedMarkets = [];
  let autoPublishErrors = [];

  if (!offers.length && autoPublishAllMarkets) {
    for (const market of MARKETPLACES) {
      try {
        const categoryId = await resolveCategoryIdForMarketplace({
          sku: safeSku,
          marketplaceId: market.marketplaceId,
          shopifyVariant,
        });

        const publishResult = await publishOrUpdateSkuOnMarketplace({
          sku: safeSku,
          shopifyVariant,
          marketplaceId: market.marketplaceId,
          sourceLanguage,
          categoryId,
        });

        autoPublishedMarkets.push(publishResult);
      } catch (error) {
        autoPublishErrors.push({
          marketplaceId: market.marketplaceId,
          error: errorToSerializable(error),
        });
      }
    }

    try {
      offers = await getOffersBySku({ sku: safeSku });
    } catch {
      offers = [];
    }
  }

  if (!offers.length) {
    return {
      ok: false,
      sku: safeSku,
      inventorySynced: true,
      priceSynced: false,
      syncedPrice: shopifyVariant.price,
      syncedQuantity: shopifyVariant.inventoryQuantity,
      inventoryContentLanguage,
      inventoryCategoryId,
      inventoryItemUpdate,
      inventoryItemPayload,
      inventoryDynamicAspects,
      offersFound: 0,
      listingIdUsed: safeListingId || null,
      migrationAttempted,
      migrationResult,
      initialOfferLookupUnavailable25713,
      initialOfferLookupError,
      postMigrationOfferLookupError,
      autoPublishedMarkets,
      autoPublishErrors,
      warning:
        "Inventory aggiornato, ma non sono riuscito a ottenere offer accessibili. Se l'inserzione è nata fuori da Inventory API, va migrata oppure ricreata via app.",
      offerResults: [],
    };
  }

  let bulkUpdateResult = null;
  let bulkUpdateError = null;

  try {
    bulkUpdateResult = await bulkUpdatePublishedOffersForSku({
      sku: safeSku,
      price: shopifyVariant.price,
      quantity: shopifyVariant.inventoryQuantity,
      offers,
    });
  } catch (error) {
    bulkUpdateError = errorToSerializable(error);
  }

  const responses = safeArray(bulkUpdateResult?.responses);

  const offerResults = offers.map((offer) => {
    const responseRow =
      responses.find((r) => String(r?.offerId || "") === String(offer?.offerId || "")) ||
      null;

    const errors = safeArray(responseRow?.errors);
    const warnings = safeArray(responseRow?.warnings);

    return {
      offerId: offer?.offerId || null,
      marketplaceId: offer?.marketplaceId || null,
      format: offer?.format || null,
      statusCode: responseRow?.statusCode || null,
      ok:
        responseRow
          ? Number(responseRow.statusCode || 0) >= 200 &&
            Number(responseRow.statusCode || 0) < 300 &&
            errors.length === 0
          : !bulkUpdateError,
      errors,
      warnings,
    };
  });

  const realFailures = offerResults.filter((r) => r.ok === false);

  return {
    ok: realFailures.length === 0 && !bulkUpdateError,
    sku: safeSku,
    inventorySynced: true,
    priceSynced: realFailures.length === 0 && !bulkUpdateError,
    syncedPrice: shopifyVariant.price,
    syncedQuantity: shopifyVariant.inventoryQuantity,
    inventoryContentLanguage,
    inventoryCategoryId,
    inventoryItemUpdate,
    inventoryItemPayload,
    inventoryDynamicAspects,
    offersFound: offers.length,
    listingIdUsed: safeListingId || null,
    migrationAttempted,
    migrationResult,
    initialOfferLookupUnavailable25713,
    initialOfferLookupError,
    postMigrationOfferLookupError,
    autoPublishedMarkets,
    autoPublishErrors,
    bulkUpdateError,
    bulkUpdateResult,
    offerResults,
  };
}

// =========================
// AMAZON TOKEN
// =========================

async function getAmazonAccessToken() {
  const response = await axios.post(
    "https://api.amazon.com/auth/o2/token",
    new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: AMAZON_REFRESH_TOKEN,
      client_id: AMAZON_CLIENT_ID,
      client_secret: AMAZON_CLIENT_SECRET,
    }),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      },
    }
  );

  return response.data.access_token;
}

async function getAmazonGrantlessNotificationsToken() {
  const response = await axios.post(
    "https://api.amazon.com/auth/o2/token",
    new URLSearchParams({
      grant_type: "client_credentials",
      scope: "sellingpartnerapi::notifications",
      client_id: AMAZON_CLIENT_ID,
      client_secret: AMAZON_CLIENT_SECRET,
    }),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      },
    }
  );

  return response.data.access_token;
}

function amazonHeaders(accessToken) {
  return {
    "x-amz-access-token": accessToken,
    "user-agent": "SyncAmzEby/1.0",
    "Content-Type": "application/json",
    Accept: "application/json",
  };
}

async function amazonGet(pathname, accessToken, params = {}) {
  const response = await axios.get(`${AMAZON_SP_API_BASE}${pathname}`, {
    params,
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

async function amazonPost(pathname, accessToken, body) {
  const response = await axios.post(`${AMAZON_SP_API_BASE}${pathname}`, body, {
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

async function amazonPatch(pathname, accessToken, body, params = {}) {
  const response = await axios.patch(`${AMAZON_SP_API_BASE}${pathname}`, body, {
    params,
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

// =========================
// SHOPIFY INVENTORY ADJUSTMENT
// =========================

async function adjustShopifyInventoryBySku({
  sku,
  delta,
  reason = "correction",
  referenceDocumentUri = "gid://syncamzeby/manual",
}) {
  const found = await findShopifyInventoryItemBySku(sku);

  if (!found) {
    console.log("SHOPIFY SKU NOT FOUND", sku);
    return { ok: false, error: "SHOPIFY SKU NOT FOUND", sku };
  }

  if (!found.tracked) {
    console.log("SHOPIFY INVENTORY NOT TRACKED FOR SKU", sku);
    return { ok: false, error: "SHOPIFY INVENTORY NOT TRACKED", sku };
  }

  const mutation = `
    mutation AdjustInventory($input: InventoryAdjustQuantitiesInput!) {
      inventoryAdjustQuantities(input: $input) {
        userErrors {
          field
          message
        }
        inventoryAdjustmentGroup {
          createdAt
          reason
          referenceDocumentUri
          changes {
            name
            delta
          }
        }
      }
    }
  `;

  const variables = {
    input: {
      reason,
      name: "available",
      referenceDocumentUri,
      changes: [
        {
          inventoryItemId: found.inventoryItemId,
          locationId: `gid://shopify/Location/${SHOPIFY_LOCATION_ID}`,
          delta,
        },
      ],
    },
  };

  const data = await shopifyGraphQL(mutation, variables);
  const userErrors = data?.inventoryAdjustQuantities?.userErrors || [];

  if (userErrors.length) {
    throw new Error(JSON.stringify(userErrors));
  }

  return {
    ok: true,
    sku,
    delta,
    inventoryItemId: found.inventoryItemId,
    data,
  };
}

// =========================
// SHOPIFY -> AMAZON
// =========================

async function sendPriceQuantityToAmazon({ sku, price, quantity }) {
  try {
    const token = await getAmazonAccessToken();

    const body = {
      productType: "PRODUCT",
      patches: [
        {
          op: "replace",
          path: "/attributes/fulfillment_availability",
          value: [
            {
              fulfillment_channel_code: "DEFAULT",
              quantity: Number(quantity),
            },
          ],
        },
        {
          op: "replace",
          path: "/attributes/purchasable_offer",
          value: [
            {
              marketplace_id: AMAZON_MARKETPLACE_ID,
              currency: "EUR",
              our_price: [
                {
                  schedule: [
                    {
                      value_with_tax: Number(price),
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    };

    const pathname = `/listings/2021-08-01/items/${encodeURIComponent(
      AMAZON_SELLER_ID
    )}/${encodeURIComponent(sku)}`;

    const response = await amazonPatch(pathname, token, body, {
      marketplaceIds: AMAZON_MARKETPLACE_ID,
      issueLocale: "it_IT",
    });

    console.log("AMAZON UPDATE SUCCESS", JSON.stringify(response, null, 2));
    return response;
  } catch (error) {
    console.log("AMAZON UPDATE ERROR");

    if (error.response) {
      console.log(JSON.stringify(error.response.data, null, 2));
      return { ok: false, error: error.response.data };
    }

    console.log(error.message);
    return { ok: false, error: error.message };
  }
}

// =========================
// AMAZON ORDERS -> SHOPIFY
// =========================

async function getAmazonOrderItems(orderId) {
  const token = await getAmazonAccessToken();

  return amazonGet(
    `/orders/v0/orders/${encodeURIComponent(orderId)}/orderItems`,
    token
  );
}

async function processAmazonOrderById(orderId) {
  if (!orderId) {
    return { ok: false, error: "missing orderId" };
  }

  if (processedAmazonOrderEvents.has(orderId)) {
    return { ok: true, skipped: true, reason: "already_processed", orderId };
  }

  const itemsData = await getAmazonOrderItems(orderId);
  const items = itemsData?.payload?.OrderItems || [];

  console.log("PROCESS AMAZON ORDER", orderId, "ITEMS", items.length);

  for (const item of items) {
    const sku = item.SellerSKU;
    const qty = Number(item.QuantityOrdered || 0);

    if (!sku || qty <= 0) continue;

    console.log("AMAZON ORDER ITEM", { orderId, sku, qty });

    await adjustShopifyInventoryBySku({
      sku,
      delta: -Math.abs(qty),
      reason: "correction",
      referenceDocumentUri: `gid://syncamzeby/amazon-order/${orderId}`,
    });
  }

  processedAmazonOrderEvents.add(orderId);

  return { ok: true, orderId, itemCount: items.length };
}

// =========================
// AMAZON NOTIFICATIONS
// =========================

async function createAmazonDestination() {
  const token = await getAmazonGrantlessNotificationsToken();

  const body = {
    resourceSpecification: {
      sqs: {
        arn: AMAZON_SQS_QUEUE_ARN,
      },
    },
    name: "syncamzeby-order-change-destination",
  };

  return amazonPost("/notifications/v1/destinations", token, body);
}

async function listAmazonDestinations() {
  const token = await getAmazonGrantlessNotificationsToken();
  return amazonGet("/notifications/v1/destinations", token);
}

function getDestinationsArray(destinationsResponse) {
  if (Array.isArray(destinationsResponse?.payload)) {
    return destinationsResponse.payload;
  }

  if (Array.isArray(destinationsResponse?.payload?.destinations)) {
    return destinationsResponse.payload.destinations;
  }

  if (Array.isArray(destinationsResponse?.destinations)) {
    return destinationsResponse.destinations;
  }

  return [];
}

function findDestinationByArn(destinationsResponse, arnToFind) {
  const destinations = getDestinationsArray(destinationsResponse);

  return (
    destinations.find((destination) => {
      const arn =
        destination?.resource?.sqs?.arn ||
        destination?.resourceSpecification?.sqs?.arn ||
        destination?.resource?.arn ||
        null;

      return arn === arnToFind;
    }) || null
  );
}

async function getOrCreateAmazonDestination() {
  try {
    const created = await createAmazonDestination();

    const destinationId =
      created?.payload?.destinationId || created?.destinationId || null;

    if (!destinationId) {
      throw new Error(
        `Destination created but destinationId missing: ${JSON.stringify(created)}`
      );
    }

    return {
      mode: "created",
      destinationId,
      destinationResponse: created,
    };
  } catch (error) {
    const amazonError = error?.response?.data;
    const errors = amazonError?.errors || [];
    const hasConflict = errors.some((e) => e?.code === "Conflict");

    if (!hasConflict) {
      throw error;
    }

    console.log("DESTINATION ALREADY EXISTS, REUSING EXISTING ONE");

    const destinationsResponse = await listAmazonDestinations();
    const existingDestination = findDestinationByArn(
      destinationsResponse,
      AMAZON_SQS_QUEUE_ARN
    );

    const destinationId = existingDestination?.destinationId || null;

    if (!destinationId) {
      throw new Error(
        `Destination exists but could not be found in list: ${JSON.stringify(
          destinationsResponse
        )}`
      );
    }

    return {
      mode: "reused",
      destinationId,
      destinationResponse: destinationsResponse,
    };
  }
}

async function createOrderChangeSubscription(destinationId) {
  const token = await getAmazonAccessToken();

  const body = {
    payloadVersion: "1.0",
    destinationId,
    processingDirective: {
      eventFilter: {
        eventFilterType: "ORDER_CHANGE",
      },
    },
  };

  return amazonPost("/notifications/v1/subscriptions/ORDER_CHANGE", token, body);
}

async function getOrderChangeSubscription() {
  const token = await getAmazonAccessToken();
  return amazonGet("/notifications/v1/subscriptions/ORDER_CHANGE", token);
}

async function getOrCreateOrderChangeSubscription(destinationId) {
  try {
    const created = await createOrderChangeSubscription(destinationId);

    return {
      mode: "created",
      subscriptionResponse: created,
    };
  } catch (error) {
    const amazonError = error?.response?.data;
    const errors = amazonError?.errors || [];
    const hasConflict = errors.some((e) => e?.code === "Conflict");

    if (!hasConflict) {
      throw error;
    }

    console.log("ORDER_CHANGE SUBSCRIPTION ALREADY EXISTS, REUSING IT");

    const existing = await getOrderChangeSubscription();

    return {
      mode: "reused",
      subscriptionResponse: existing,
    };
  }
}

async function ensureAmazonOrderChangeSubscription() {
  const destinationData = await getOrCreateAmazonDestination();
  const subscriptionData = await getOrCreateOrderChangeSubscription(
    destinationData.destinationId
  );

  return {
    destinationMode: destinationData.mode,
    destinationId: destinationData.destinationId,
    destinationResponse: destinationData.destinationResponse,
    subscriptionMode: subscriptionData.mode,
    subscriptionResponse: subscriptionData.subscriptionResponse,
  };
}

// =========================
// SQS
// =========================

function extractOrderIdFromSqsBody(bodyString) {
  let parsed;

  try {
    parsed = JSON.parse(bodyString);
  } catch {
    return null;
  }

  if (parsed?.Payload?.OrderChangeNotification?.AmazonOrderId) {
    return parsed.Payload.OrderChangeNotification.AmazonOrderId;
  }

  if (parsed?.payload?.OrderChangeNotification?.AmazonOrderId) {
    return parsed.payload.OrderChangeNotification.AmazonOrderId;
  }

  if (parsed?.NotificationPayload?.AmazonOrderId) {
    return parsed.NotificationPayload.AmazonOrderId;
  }

  if (parsed?.notificationPayload?.AmazonOrderId) {
    return parsed.notificationPayload.AmazonOrderId;
  }

  if (parsed?.Message) {
    try {
      const inner = JSON.parse(parsed.Message);

      if (inner?.Payload?.OrderChangeNotification?.AmazonOrderId) {
        return inner.Payload.OrderChangeNotification.AmazonOrderId;
      }

      if (inner?.payload?.OrderChangeNotification?.AmazonOrderId) {
        return inner.payload.OrderChangeNotification.AmazonOrderId;
      }

      if (inner?.NotificationPayload?.AmazonOrderId) {
        return inner.NotificationPayload.AmazonOrderId;
      }

      if (inner?.notificationPayload?.AmazonOrderId) {
        return inner.notificationPayload.AmazonOrderId;
      }
    } catch {
      return null;
    }
  }

  return null;
}

async function pollSqsOnce() {
  const receiveResponse = await sqsClient.send(
    new ReceiveMessageCommand({
      QueueUrl: AMAZON_SQS_QUEUE_URL,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 10,
      VisibilityTimeout: 30,
      MessageAttributeNames: ["All"],
      AttributeNames: ["All"],
    })
  );

  const messages = receiveResponse.Messages || [];
  const results = [];

  for (const message of messages) {
    const receiptHandle = message.ReceiptHandle;
    const body = message.Body || "";
    const orderId = extractOrderIdFromSqsBody(body);

    try {
      if (!orderId) {
        console.log("SQS MESSAGE WITHOUT ORDER ID", body);

        if (receiptHandle) {
          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: AMAZON_SQS_QUEUE_URL,
              ReceiptHandle: receiptHandle,
            })
          );
        }

        results.push({
          ok: true,
          skipped: true,
          reason: "no_order_id",
        });
        continue;
      }

      console.log("SQS ORDER EVENT RECEIVED", orderId);

      const result = await processAmazonOrderById(orderId);

      if (receiptHandle) {
        await sqsClient.send(
          new DeleteMessageCommand({
            QueueUrl: AMAZON_SQS_QUEUE_URL,
            ReceiptHandle: receiptHandle,
          })
        );
      }

      results.push(result);
    } catch (error) {
      console.log("SQS PROCESS ERROR", error.message);

      results.push({
        ok: false,
        error: error.message,
      });
    }
  }

  return {
    ok: true,
    received: messages.length,
    results,
  };
}

// =========================
// UI / STATIC HTML
// =========================

app.get("/", (req, res) => {
  res.redirect("/ebay/publisher");
});

app.get("/ebay/publisher", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "ebay-publisher.html"));
});

app.get("/ebay/config/public", (req, res) => {
  res.json({
    ok: true,
    appBaseUrl: APP_BASE_URL,
    defaultMarketplaceId: EBAY_DEFAULT_MARKETPLACE_ID,
    locationKeys: EBAY_LOCATION_KEYS,
    marketplaces: MARKETPLACES,
  });
});

// =========================
// DEBUG / DATA ROUTES UI
// =========================

app.get("/ebay/shopify/product", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const product = await getShopifyVariantBySku(sku);
    if (!product) {
      return res
        .status(404)
        .json({ ok: false, error: `Shopify SKU not found: ${sku}` });
    }

    return res.json({
      ok: true,
      sku,
      product: {
        title: product.product.title,
        categoryFullName: product.product.categoryFullName,
        productType: product.product.productType,
        vendor: product.product.vendor,
        onlineStoreUrl: product.product.onlineStoreUrl,
        price: product.price,
        inventoryQuantity: product.inventoryQuantity,
        descriptionText: product.product.descriptionText,
        descriptionHtml: product.product.descriptionHtml,
        barcode: product.barcode,
        imageUrls: product.product.imageUrls,
      },
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/ebay/category/suggest-all", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const shopifyVariant = await getShopifyVariantBySku(sku);
    if (!shopifyVariant) {
      return res
        .status(404)
        .json({ ok: false, error: `Shopify SKU not found: ${sku}` });
    }

    const results = {};

    for (const market of MARKETPLACES) {
      try {
        const suggestion = await suggestEbayCategory({
          marketplaceId: market.marketplaceId,
          title: shopifyVariant.product.title,
          shopifyCategory: shopifyVariant.product.categoryFullName,
          productType: shopifyVariant.product.productType,
        });

        results[market.marketplaceId] = {
          ok: true,
          marketplaceId: market.marketplaceId,
          locale: market.locale,
          site: market.site,
          suggestion,
        };
      } catch (error) {
        results[market.marketplaceId] = {
          ok: false,
          marketplaceId: market.marketplaceId,
          locale: market.locale,
          site: market.site,
          error: errorToSerializable(error),
        };
      }
    }

    return res.json({
      ok: true,
      sku,
      shopify: {
        title: shopifyVariant.product.title,
        productType: shopifyVariant.product.productType,
        categoryFullName: shopifyVariant.product.categoryFullName,
      },
      results,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/category/aspects", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    const marketplaceId = getRequestedMarketplaceId(req);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const shopifyVariant = await getShopifyVariantBySku(sku);
    if (!shopifyVariant) {
      return res
        .status(404)
        .json({ ok: false, error: `Shopify SKU not found: ${sku}` });
    }

    const categoryId = await resolveCategoryIdForMarketplace({
      sku,
      marketplaceId,
      shopifyVariant,
    });

    const tree = await getDefaultCategoryTreeId(marketplaceId);
    const categoryTreeId = tree?.categoryTreeId;

    const aspectsResponse = await getCategoryAspects({
      categoryTreeId,
      categoryId,
    });

    const built = await buildDynamicAspectsForCategory({
      shopifyVariant,
      marketplaceId,
      categoryId,
    });

    return res.json({
      ok: true,
      sku,
      marketplaceId,
      categoryId,
      categoryTreeId,
      requiredAspects: safeArray(aspectsResponse?.aspects)
        .filter((a) => Boolean(a?.aspectConstraint?.aspectRequired))
        .map((a) => a?.localizedAspectName),
      builtAspects: built.aspects,
      aspectsMeta: built.aspectsMeta,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/translation/preview", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    const sourceLanguage = getSourceLanguage(req.query.sourceLanguage || "it");

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const shopifyVariant = await getShopifyVariantBySku(sku);
    if (!shopifyVariant) {
      return res
        .status(404)
        .json({ ok: false, error: `Shopify SKU not found: ${sku}` });
    }

    const inventoryItemPayload = buildInventoryItemPayload({
      shopifyVariant,
    });

    const translations = await buildMarketplaceTranslations({
      sourceLanguage,
      title: inventoryItemPayload.product.title,
      descriptionHtml:
        shopifyVariant.product.descriptionHtml || shopifyVariant.product.descriptionText,
    });

    return res.json({
      ok: true,
      sku,
      sourceLanguage,
      baseTitle: inventoryItemPayload.product.title,
      translations,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

// =========================
// EBAY ROUTES
// =========================

app.get("/ebay/health", async (req, res) => {
  try {
    validateEbayEnv();

    const connection = sanitizeEbayConnectionForResponse();

    let oauthConnected = false;
    let accessTokenCached = false;
    let policies = {};

    try {
      if (ebayConnectionStore.refreshToken) {
        const accessToken = await ensureValidEbayAccessToken();
        oauthConnected = Boolean(ebayConnectionStore.refreshToken);
        accessTokenCached = Boolean(accessToken);

        for (const market of MARKETPLACES) {
          try {
            const rawPolicies = await getAllEbayPolicies(market.marketplaceId);
            const selected = pickDefaultPolicyIds(rawPolicies);
            policies[market.marketplaceId] = {
              marketplaceId: market.marketplaceId,
              ...selected,
              merchantLocationKey: getMerchantLocationKey(market.marketplaceId),
            };
          } catch (error) {
            policies[market.marketplaceId] = {
              marketplaceId: market.marketplaceId,
              error: errorToSerializable(error),
            };
          }
        }
      }
    } catch (error) {
      oauthConnected = false;
      accessTokenCached = false;
    }

    return res.json({
      ok: true,
      ebayEnvironment: EBAY_ENVIRONMENT,
      enabled: EBAY_ENABLED,
      oauthConnected,
      accessTokenCached,
      redirectUri: EBAY_REDIRECT_URI,
      ruName: EBAY_RU_NAME,
      connection,
      listingMapCount: sanitizeListingMapForResponse().count,
      lastOrderPollByMarketplace: {
        EBAY_IT: ebayOrderSyncState.lastCheckedAt,
        EBAY_DE: ebayOrderSyncState.lastCheckedAt,
        EBAY_FR: ebayOrderSyncState.lastCheckedAt,
        EBAY_ES: ebayOrderSyncState.lastCheckedAt,
        EBAY_GB: ebayOrderSyncState.lastCheckedAt,
      },
      policies,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/ebay/oauth/start", async (req, res) => {
  try {
    validateEbayEnv();

    const state = crypto.randomBytes(24).toString("hex");

    ebayOAuthStates.set(state, {
      createdAt: Date.now(),
      ip: req.ip,
    });

    const url = buildEbayOAuthStartUrl(state);

    return res.redirect(url);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/ebay/oauth/callback", async (req, res) => {
  try {
    validateEbayEnv();

    const code = String(req.query.code || "");
    const state = String(req.query.state || "");
    const errorName = String(req.query.error || "");
    const errorDescription = String(req.query.error_description || "");

    if (errorName) {
      console.log("EBAY OAUTH ERROR", errorName, errorDescription);
      return res.send("OAuth eBay fallito.");
    }

    if (!code) {
      return res.status(400).send("Code mancante");
    }

    if (state && ebayOAuthStates.has(state)) {
      ebayOAuthStates.delete(state);
    }

    const tokenData = await exchangeEbayCodeForTokens(code);
    await saveEbayTokens(tokenData);

    const userInfo = await getEbayUserInfo(ebayConnectionStore.accessToken);
    ebayConnectionStore.userInfo = userInfo;
    await saveEbayConnectionToDisk();

    return res.send("OAuth eBay completato correttamente.");
  } catch (error) {
    console.log("EBAY CALLBACK ERROR", errorToSerializable(error));
    return res.status(500).send("OAuth eBay fallito.");
  }
});

app.get("/ebay/account/policies", async (req, res) => {
  try {
    const marketplaceId = getRequestedMarketplaceId(req);
    const result = await getAllEbayPolicies(marketplaceId);
    const selected = pickDefaultPolicyIds(result);

    return res.json({
      ok: true,
      policies: {
        [marketplaceId]: {
          marketplaceId,
          ...selected,
          selected: {
            payment:
              safeArray(result?.paymentPolicies?.paymentPolicies).find(
                (x) => x?.paymentPolicyId === selected.paymentPolicyId
              ) || null,
            fulfillment:
              safeArray(result?.fulfillmentPolicies?.fulfillmentPolicies).find(
                (x) => x?.fulfillmentPolicyId === selected.fulfillmentPolicyId
              ) || null,
            return:
              safeArray(result?.returnPolicies?.returnPolicies).find(
                (x) => x?.returnPolicyId === selected.returnPolicyId
              ) || null,
          },
          raw: result,
        },
      },
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/location/get", async (req, res) => {
  try {
    const merchantLocationKey = String(
      req.query.merchantLocationKey || getMerchantLocationKey(EBAY_DEFAULT_MARKETPLACE_ID)
    );

    if (!merchantLocationKey) {
      return res
        .status(400)
        .json({ ok: false, error: "missing merchantLocationKey" });
    }

    const result = await getEbayInventoryLocation(merchantLocationKey);

    return res.json({
      ok: true,
      merchantLocationKey,
      result,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/location/list", async (req, res) => {
  try {
    const result = await getEbayInventoryLocations();

    return res.json({
      ok: true,
      result,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/category/suggest", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const shopifyVariant = await getShopifyVariantBySku(sku);
    if (!shopifyVariant) {
      return res
        .status(404)
        .json({ ok: false, error: `Shopify SKU not found: ${sku}` });
    }

    const marketplaceId = getRequestedMarketplaceId(req);
    const suggestion = await suggestEbayCategory({
      marketplaceId,
      title: shopifyVariant.product.title,
      shopifyCategory: shopifyVariant.product.categoryFullName,
      productType: shopifyVariant.product.productType,
    });

    return res.json({
      ok: true,
      sku,
      marketplaceId,
      shopify: {
        title: shopifyVariant.product.title,
        productType: shopifyVariant.product.productType,
        categoryFullName: shopifyVariant.product.categoryFullName,
      },
      suggestion,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/offers/by-sku", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    const marketplaceId = String(req.query.marketplaceId || "").trim();
    const format = String(req.query.format || "").trim();

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const offers = await getOffersBySku({
      sku,
      marketplaceId: marketplaceId || undefined,
      format: format || undefined,
    });

    return res.json({
      ok: true,
      sku,
      marketplaceId: marketplaceId || null,
      format: format || null,
      offersFound: offers.length,
      offers,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/listing-map", async (req, res) => {
  try {
    return res.json({
      ok: true,
      listingMap: sanitizeListingMapForResponse(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.post("/ebay/listing-map/save", async (req, res) => {
  try {
    const sku = normalizeSku(req.body?.sku);
    const listingId = normalizeListingId(req.body?.listingId);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }
    if (!listingId) {
      return res.status(400).json({ ok: false, error: "missing listingId" });
    }

    setPersistedListingIdForSku(sku, listingId);
    await saveEbayListingMapToDisk();

    return res.json({
      ok: true,
      sku,
      listingId,
      listingMap: sanitizeListingMapForResponse(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.post("/ebay/listing-map/delete", async (req, res) => {
  try {
    const sku = normalizeSku(req.body?.sku);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    deletePersistedListingIdForSku(sku);
    await saveEbayListingMapToDisk();

    return res.json({
      ok: true,
      sku,
      listingMap: sanitizeListingMapForResponse(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.post("/ebay/migrate/listing", async (req, res) => {
  try {
    const sku = normalizeSku(req.body?.sku);
    const listingId = normalizeListingId(req.body?.listingId);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }
    if (!listingId) {
      return res.status(400).json({ ok: false, error: "missing listingId" });
    }

    const result = await migrateListingToInventoryModel({ sku, listingId });
    const offers = await getOffersBySku({ sku });

    return res.json({
      ok: true,
      sku,
      listingId,
      migration: result,
      offersFound: offers.length,
      offers,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.post("/ebay/publish/multi", async (req, res) => {
  try {
    const sku = String(req.body?.sku || "").trim();
    const sourceLanguage = getSourceLanguage(req.body?.sourceLanguage || "it");
    const categoryMap = isPlainObject(req.body?.categoryMap) ? req.body.categoryMap : {};
    const marketplaces = Array.isArray(req.body?.marketplaces)
      ? req.body.marketplaces
      : MARKETPLACES.map((m) => m.marketplaceId);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const result = await publishSkuToMultipleEbayMarkets({
      sku,
      sourceLanguage,
      categoryMap,
      marketplaces,
    });

    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.post("/ebay/sync/sku", async (req, res) => {
  try {
    const sku = normalizeSku(req.body?.sku);
    const sourceLanguage = getSourceLanguage(req.body?.sourceLanguage || "it");
    const listingId = normalizeListingId(req.body?.listingId);
    const autoPublishAllMarkets =
      String(req.body?.autoPublishAllMarkets ?? "true") !== "false";

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    if (listingId) {
      setPersistedListingIdForSku(sku, listingId);
      await saveEbayListingMapToDisk();
    }

    const result = await syncShopifySkuToEbay({
      sku,
      sourceLanguage,
      listingId: listingId || null,
      autoPublishAllMarkets,
    });

    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/orders/pull", async (req, res) => {
  try {
    const minutes = Number(req.query.minutes || EBAY_ORDER_PULL_WINDOW_MINUTES);
    const result = await pullAndProcessRecentEbayOrders({ minutes });
    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/orders/get", async (req, res) => {
  try {
    const orderId = String(req.query.orderId || "").trim();
    if (!orderId) {
      return res.status(400).json({ ok: false, error: "missing orderId" });
    }

    const result = await getEbayOrder(orderId);
    return res.json({
      ok: true,
      orderId,
      result,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

// =========================
// EBAY NOTIFICATIONS
// =========================

app.post("/ebay/notifications", express.raw({ type: "*/*" }), (req, res) => {
  try {
    const parsed = parsePossiblyJsonBody(req.body);
    const rawBody =
      typeof parsed === "string" ? parsed : JSON.stringify(parsed || {});

    console.log("EBAY NOTIFICATION RECEIVED");
    console.log("EBAY RAW BODY", rawBody || "<empty>");

    return res.status(200).send("OK");
  } catch (error) {
    console.log("EBAY NOTIFICATION ERROR", error.message);
    return res.status(500).send("error");
  }
});

app.get("/ebay/notifications", (req, res) => {
  try {
    validateEbayVerificationConfig();

    const challengeCode = String(req.query.challenge_code || "");

    if (!challengeCode) {
      return res.status(200).send("SyncAmzEby eBay notifications endpoint OK");
    }

    const challengeResponse = buildEbayChallengeResponse({
      challengeCode,
      verificationToken: EBAY_VERIFICATION_TOKEN,
      endpoint: EBAY_NOTIFICATION_ENDPOINT,
    });

    return res.status(200).json({ challengeResponse });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

// =========================
// AMAZON ROUTES
// =========================

app.get("/amazon/notifications/setup", async (req, res) => {
  try {
    const result = await ensureAmazonOrderChangeSubscription();
    res.json({ ok: true, ...result });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/amazon/notifications/destinations", async (req, res) => {
  try {
    const result = await listAmazonDestinations();
    res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/amazon/notifications/subscription", async (req, res) => {
  try {
    const result = await getOrderChangeSubscription();
    res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/amazon/notifications/pull", async (req, res) => {
  try {
    const result = await pollSqsOnce();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/amazon/order-items", async (req, res) => {
  try {
    const orderId = String(req.query.orderId || "");
    if (!orderId) {
      return res.status(400).json({ ok: false, error: "missing orderId" });
    }

    const result = await getAmazonOrderItems(orderId);
    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

// =========================
// SHOPIFY UTILITY ROUTES
// =========================

app.get("/shopify/test-adjust", async (req, res) => {
  try {
    const sku = String(req.query.sku || "");
    const qty = Number(req.query.qty || 1);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const result = await adjustShopifyInventoryBySku({
      sku,
      delta: -Math.abs(qty),
      reason: "correction",
      referenceDocumentUri: `gid://syncamzeby/manual-adjust/${sku}`,
    });

    return res.json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

// =========================
// SHOPIFY WEBHOOKS
// =========================

app.post("/webhooks/products", express.raw({ type: "*/*" }), async (req, res) => {
  try {
    const parsed = parsePossiblyJsonBody(req.body);
    const payload = typeof parsed === "string" ? JSON.parse(parsed) : parsed;

    for (const variant of safeArray(payload?.variants)) {
      const existing = inventoryMap.get(String(variant.inventory_item_id)) || {};

      inventoryMap.set(String(variant.inventory_item_id), {
        sku: variant.sku,
        price: variant.price,
        quantity: existing.quantity ?? variant.inventory_quantity ?? 0,
      });

      if (existing.quantity !== undefined && variant.sku) {
        await sendPriceQuantityToAmazon({
          sku: variant.sku,
          price: variant.price,
          quantity: existing.quantity,
        });
      }

      if (variant.sku && EBAY_ENABLED) {
        try {
          const ebayResult = await syncShopifySkuToEbay({
            sku: variant.sku,
            sourceLanguage: "it",
            listingId: getPersistedListingIdForSku(variant.sku),
            autoPublishAllMarkets: true,
          });
          console.log(
            "EBAY SYNC FROM PRODUCT WEBHOOK",
            JSON.stringify(ebayResult, null, 2)
          );
        } catch (error) {
          console.log(
            "EBAY SYNC FROM PRODUCT WEBHOOK ERROR",
            errorToSerializable(error)
          );
        }
      }
    }

    return res.sendStatus(200);
  } catch (error) {
    console.log("PRODUCT WEBHOOK ERROR", error.message);
    return res.status(500).send("error");
  }
});

app.post("/webhooks/inventory", express.raw({ type: "*/*" }), async (req, res) => {
  try {
    const parsed = parsePossiblyJsonBody(req.body);
    const payload = typeof parsed === "string" ? JSON.parse(parsed) : parsed;

    let mapped = inventoryMap.get(String(payload.inventory_item_id));

    if (!mapped) {
      inventoryMap.set(String(payload.inventory_item_id), {
        quantity: payload.available,
      });

      return res.sendStatus(200);
    }

    mapped.quantity = payload.available;

    const sku = mapped.sku;
    const price = mapped.price;
    const quantity = payload.available;

    if (!sku) {
      return res.sendStatus(200);
    }

    await sendPriceQuantityToAmazon({ sku, price, quantity });

    if (EBAY_ENABLED) {
      try {
        const ebayResult = await syncShopifySkuToEbay({
          sku,
          sourceLanguage: "it",
          listingId: getPersistedListingIdForSku(sku),
          autoPublishAllMarkets: true,
        });
        console.log(
          "EBAY SYNC FROM INVENTORY WEBHOOK",
          JSON.stringify(ebayResult, null, 2)
        );
      } catch (error) {
        console.log(
          "EBAY SYNC FROM INVENTORY WEBHOOK ERROR",
          errorToSerializable(error)
        );
      }
    }

    return res.sendStatus(200);
  } catch (error) {
    console.log("INVENTORY WEBHOOK ERROR", error.message);
    return res.status(500).send("error");
  }
});

// =========================
// HEALTH
// =========================

app.get("/health", async (req, res) => {
  let sqsAttrs = null;

  try {
    const attrs = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: AMAZON_SQS_QUEUE_URL,
        AttributeNames: ["QueueArn"],
      })
    );
    sqsAttrs = attrs.Attributes || null;
  } catch (error) {
    sqsAttrs = { error: error.message };
  }

  return res.json({
    ok: true,
    now: nowIso(),
    ebayEnabled: EBAY_ENABLED,
    ebayEnvironment: EBAY_ENVIRONMENT,
    ebayStateDir: EBAY_STATE_DIR,
    ebayStateFile: EBAY_STATE_FILE,
    ebayListingMapFile: EBAY_LISTING_MAP_FILE,
    ebayOrderSyncFile: EBAY_ORDER_SYNC_FILE,
    ebayConnected: ebayConnectionStore.connected,
    locationKeys: EBAY_LOCATION_KEYS,
    marketplaceId: EBAY_DEFAULT_MARKETPLACE_ID,
    marketplaces: MARKETPLACES,
    listingMap: sanitizeListingMapForResponse(),
    orderSync: serializeOrderSyncState(),
    sqs: sqsAttrs,
  });
});

// =========================
// STARTUP
// =========================

async function start() {
  try {
    validateBaseEnv();

    if (EBAY_ENABLED) {
      await loadEbayConnectionFromDisk();
      await loadEbayListingMapFromDisk();
      await loadEbayOrderSyncStateFromDisk();
    }

    app.listen(PORT, () => {
      console.log(`SyncAmzEby listening on port ${PORT}`);
      console.log(`eBay module enabled: ${EBAY_ENABLED}`);
      console.log(`eBay environment: ${EBAY_ENVIRONMENT}`);
      console.log(
        `eBay verification token configured: ${Boolean(EBAY_VERIFICATION_TOKEN)}`
      );
      console.log(`eBay state dir: ${EBAY_STATE_DIR}`);
      console.log(`eBay state file: ${EBAY_STATE_FILE}`);
      console.log(`eBay listing map file: ${EBAY_LISTING_MAP_FILE}`);
      console.log(`eBay order sync file: ${EBAY_ORDER_SYNC_FILE}`);
      console.log(`Publisher UI: ${APP_BASE_URL}/ebay/publisher`);
    });

    if (AUTO_POLL_SQS) {
      console.log(`AUTO_POLL_SQS enabled every ${SQS_POLL_INTERVAL_MS} ms`);

      setInterval(async () => {
        try {
          const result = await pollSqsOnce();
          if (result.received > 0) {
            console.log("AUTO POLL RESULT", JSON.stringify(result, null, 2));
          }
        } catch (error) {
          console.log("AUTO POLL ERROR", error.message);
        }
      }, SQS_POLL_INTERVAL_MS);
    }

    if (EBAY_ENABLED && AUTO_POLL_EBAY_ORDERS) {
      console.log(
        `AUTO_POLL_EBAY_ORDERS enabled every ${EBAY_ORDER_POLL_INTERVAL_MS} ms`
      );

      setInterval(async () => {
        try {
          const result = await pullAndProcessRecentEbayOrders({
            minutes: EBAY_ORDER_PULL_WINDOW_MINUTES,
          });
          if (result.received > 0) {
            console.log("AUTO EBAY ORDER POLL RESULT", JSON.stringify(result, null, 2));
          }
        } catch (error) {
          console.log("AUTO EBAY ORDER POLL ERROR", error.message);
        }
      }, EBAY_ORDER_POLL_INTERVAL_MS);
    }
  } catch (error) {
    console.error("STARTUP ERROR:", error.message);
    process.exit(1);
  }
}

start();
