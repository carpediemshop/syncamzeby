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
  return express.json({ limit: "2mb" })(req, res, next);
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
  "https://syncamzeby.onrender.com/ebay/notifications";
const EBAY_SCOPES =
  process.env.EBAY_SCOPES ||
  "https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment";
const EBAY_DEFAULT_MARKETPLACE_ID =
  process.env.EBAY_DEFAULT_MARKETPLACE_ID || "EBAY_IT";
const EBAY_MERCHANT_LOCATION_KEY =
  process.env.EBAY_MERCHANT_LOCATION_KEY || "Default-EBAY_IT";
const EBAY_DEFAULT_LISTING_DURATION =
  process.env.EBAY_DEFAULT_LISTING_DURATION || "GTC";
const EBAY_DEFAULT_CONDITION = process.env.EBAY_DEFAULT_CONDITION || "NEW";
const EBAY_DEFAULT_FORMAT = process.env.EBAY_DEFAULT_FORMAT || "FIXED_PRICE";

// eBay persistence
const EBAY_STATE_DIR =
  process.env.EBAY_STATE_DIR || path.join(__dirname, "data");
const EBAY_STATE_FILE = path.join(EBAY_STATE_DIR, "ebay-connection.json");

// Polling
const AUTO_POLL_SQS = String(process.env.AUTO_POLL_SQS || "false") === "true";
const SQS_POLL_INTERVAL_MS = Number(process.env.SQS_POLL_INTERVAL_MS || 20000);

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
    label: "Italia",
  },
  {
    marketplaceId: "EBAY_DE",
    site: "ebay.de",
    locale: "de-DE",
    language: "de",
    currency: "EUR",
    label: "Germania",
  },
  {
    marketplaceId: "EBAY_FR",
    site: "ebay.fr",
    locale: "fr-FR",
    language: "fr",
    currency: "EUR",
    label: "Francia",
  },
  {
    marketplaceId: "EBAY_ES",
    site: "ebay.es",
    locale: "es-ES",
    language: "es",
    currency: "EUR",
    label: "Spagna",
  },
  {
    marketplaceId: "EBAY_GB",
    site: "ebay.co.uk",
    locale: "en-GB",
    language: "en",
    currency: "GBP",
    label: "Regno Unito",
  },
];

function getMarketplaceMeta(marketplaceId) {
  return (
    MARKETPLACES.find((m) => m.marketplaceId === String(marketplaceId || "").trim()) ||
    null
  );
}

// =========================
// IN-MEMORY STATE
// =========================

const inventoryMap = new Map();
const processedAmazonOrderEvents = new Set();

const ebayOAuthStates = new Map();
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
  requireEnv("EBAY_REDIRECT_URI", EBAY_REDIRECT_URI);
  requireEnv("EBAY_MERCHANT_LOCATION_KEY", EBAY_MERCHANT_LOCATION_KEY);
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

    console.log("EBAY STATE LOADED FROM DISK", sanitizeEbayConnectionForResponse());
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

function pickDefaultPolicyIds(policiesData) {
  const fulfillmentPolicyId =
    policiesData?.fulfillmentPolicies?.fulfillmentPolicies?.[0]
      ?.fulfillmentPolicyId || null;

  const paymentPolicyId =
    policiesData?.paymentPolicies?.paymentPolicies?.[0]?.paymentPolicyId || null;

  const returnPolicyId =
    policiesData?.returnPolicies?.returnPolicies?.[0]?.returnPolicyId || null;

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
  const appAccessToken = await getEbayApplicationAccessToken();

  return ebayGet(
    "/commerce/taxonomy/v1/get_default_category_tree_id",
    appAccessToken,
    { marketplace_id: marketplaceId }
  );
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
    const rows = (suggestions?.categorySuggestions || []).map((row) => ({
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

  const edge = data.inventoryItems.edges[0];
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

  const images = (variant.product?.images?.nodes || [])
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
      onlineStoreUrl: variant.product?.onlineStoreUrl || "",
      categoryFullName: variant.product?.category?.fullName || "",
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

  for (const option of shopifyVariant?.selectedOptions || []) {
    const name = firstNonEmpty(option?.name);
    const value = firstNonEmpty(option?.value);
    if (name && value && !aspects[name]) {
      aspects[name] = [value];
    }
  }

  return aspects;
}

function buildInventoryItemPayload(shopifyVariant) {
  const titleBase = firstNonEmpty(shopifyVariant?.product?.title);
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);
  const title =
    variantTitle &&
    !["Default Title", "Titolo predefinito"].includes(variantTitle)
      ? `${titleBase} - ${variantTitle}`.slice(0, 80)
      : titleBase.slice(0, 80);

  const description = firstNonEmpty(
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
      aspects: buildDefaultAspects(shopifyVariant),
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

async function getOffersBySku({ sku }) {
  const accessToken = await ensureValidEbayAccessToken();

  const result = await ebayGet(
    "/sell/inventory/v1/offer",
    accessToken,
    {
      sku,
      format: EBAY_DEFAULT_FORMAT,
      limit: 200,
      offset: 0,
    }
  );

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
    throw new Error("Missing paymentPolicyId");
  }
  if (!policyIds?.returnPolicyId) {
    throw new Error("Missing returnPolicyId");
  }
  if (!policyIds?.fulfillmentPolicyId) {
    throw new Error("Missing fulfillmentPolicyId");
  }
  if (!categoryId) {
    throw new Error("Missing categoryId");
  }

  return {
    sku,
    marketplaceId,
    format: EBAY_DEFAULT_FORMAT,
    availableQuantity: Math.max(0, Number(quantity || 0)),
    categoryId,
    merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
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
  const contentLanguage = normalizeContentLanguage(meta?.locale, "it-IT");

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
    currency: meta?.currency || "EUR",
  });

  const existingOffers = await getOffersBySku({ sku });
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

async function publishSkuToMultipleEbayMarkets({
  sku,
  sourceLanguage = "it",
  categoryMap = {},
}) {
  const shopifyVariant = await getShopifyVariantBySku(sku);
  if (!shopifyVariant) {
    throw new Error(`Shopify SKU not found: ${sku}`);
  }

  const inventoryItemPayload = buildInventoryItemPayload(shopifyVariant);
  const inventoryContentLanguage = normalizeContentLanguage(
    languageToLocale(sourceLanguage),
    "it-IT"
  );

  let inventoryItemResult;
  try {
    inventoryItemResult = await createOrReplaceInventoryItem({
      sku,
      payload: inventoryItemPayload,
      contentLanguage: inventoryContentLanguage,
    });
  } catch (error) {
    return {
      ok: false,
      failedStep: "inventory_item_upsert",
      sku,
      inventoryContentLanguage,
      error: errorToSerializable(error),
    };
  }

  let translations;
  try {
    translations = await buildMarketplaceTranslations({
      sourceLanguage,
      title: inventoryItemPayload.product.title,
      descriptionHtml:
        shopifyVariant.product.descriptionHtml || shopifyVariant.product.descriptionText,
    });
  } catch (error) {
    return {
      ok: false,
      failedStep: "translation",
      sku,
      error: errorToSerializable(error),
    };
  }

  const marketsResult = [];

  for (const market of MARKETPLACES) {
    const categoryId = String(categoryMap?.[market.marketplaceId] || "").trim();

    if (!categoryId) {
      marketsResult.push({
        marketplaceId: market.marketplaceId,
        locale: market.locale,
        site: market.site,
        ok: false,
        error: "missing categoryId for marketplace",
      });
      continue;
    }

    try {
      const translation = translations[market.marketplaceId];
      const upsert = await upsertOfferForMarketplace({
        sku,
        price: shopifyVariant.price,
        quantity: shopifyVariant.inventoryQuantity,
        marketplaceId: market.marketplaceId,
        categoryId,
        translatedDescription: translation.translatedDescription,
      });

      const publishResult = await publishOffer({ offerId: upsert.offerId });

      marketsResult.push({
        marketplaceId: market.marketplaceId,
        locale: market.locale,
        site: market.site,
        ok: true,
        categoryId,
        contentLanguage: upsert.contentLanguage,
        currency: market.currency,
        translatedTitlePreview: translation.translatedTitle,
        translatedDescriptionPreview: translation.translatedDescription,
        offerMode: upsert.mode,
        offerId: upsert.offerId,
        publishResult,
      });
    } catch (error) {
      marketsResult.push({
        marketplaceId: market.marketplaceId,
        locale: market.locale,
        site: market.site,
        ok: false,
        categoryId,
        contentLanguage: normalizeContentLanguage(market.locale, "it-IT"),
        currency: market.currency,
        error: errorToSerializable(error),
      });
    }
  }

  return {
    ok: marketsResult.every((m) => m.ok),
    sku,
    note:
      "Inventory API usa un titolo condiviso sull'inventory item; la descrizione viene localizzata per marketplace. Il titolo tradotto qui è preview/log.",
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
    inventoryContentLanguage,
    inventoryItemPayload,
    inventoryItemResult,
    translations,
    markets: marketsResult,
  };
}

// =========================
// EBAY SYNC FROM SHOPIFY
// =========================

async function syncShopifySkuToEbay({ sku, sourceLanguage = "it" }) {
  const shopifyVariant = await getShopifyVariantBySku(sku);
  if (!shopifyVariant) {
    return { ok: false, sku, error: `Shopify SKU not found: ${sku}` };
  }

  const inventoryContentLanguage = normalizeContentLanguage(
    languageToLocale(sourceLanguage),
    "it-IT"
  );

  const inventoryItemPayload = buildInventoryItemPayload(shopifyVariant);

  const inventoryItemUpdate = await createOrReplaceInventoryItem({
    sku,
    payload: inventoryItemPayload,
    contentLanguage: inventoryContentLanguage,
  });

  const offers = await getOffersBySku({ sku });

  const offerResults = [];

  for (const offerSummary of offers) {
    const offerId = offerSummary.offerId;
    if (!offerId) continue;

    try {
      const currentOffer = await getOffer(offerId);
      const meta = getMarketplaceMeta(currentOffer.marketplaceId);
      const contentLanguage = normalizeContentLanguage(meta?.locale, "it-IT");

      const updatedOfferPayload = {
        ...currentOffer,
        availableQuantity: Math.max(
          0,
          Number(shopifyVariant.inventoryQuantity || 0)
        ),
        pricingSummary: {
          ...(currentOffer.pricingSummary || {}),
          price: {
            value: String(normalizeMoney(shopifyVariant.price)),
            currency:
              currentOffer?.pricingSummary?.price?.currency ||
              meta?.currency ||
              "EUR",
          },
        },
      };

      delete updatedOfferPayload.offerId;
      delete updatedOfferPayload.listing;
      delete updatedOfferPayload.status;
      delete updatedOfferPayload.marketplaceItemCondition;
      delete updatedOfferPayload.tax;
      delete updatedOfferPayload.shippingCostOverrides;
      delete updatedOfferPayload.charity;
      delete updatedOfferPayload.bestOfferTerms;
      delete updatedOfferPayload.auctionTerms;
      delete updatedOfferPayload.includeCatalogProductDetails;

      const updateResult = await updateOffer({
        offerId,
        payload: updatedOfferPayload,
        contentLanguage,
      });

      offerResults.push({
        ok: true,
        offerId,
        marketplaceId: currentOffer.marketplaceId,
        contentLanguage,
        updateResult,
      });
    } catch (error) {
      offerResults.push({
        ok: false,
        offerId,
        error: errorToSerializable(error),
      });
    }
  }

  return {
    ok: offerResults.every((r) => r.ok),
    sku,
    syncedPrice: shopifyVariant.price,
    syncedQuantity: shopifyVariant.inventoryQuantity,
    inventoryContentLanguage,
    inventoryItemUpdate,
    offersFound: offers.length,
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
  referenceDocumentUri = "gid://syncamzeby/amazon-order-sync/manual",
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
    merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
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

    const inventoryItemPayload = buildInventoryItemPayload(shopifyVariant);

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

    return res.json({
      ok: true,
      enabled: EBAY_ENABLED,
      environment: EBAY_ENVIRONMENT,
      authBase: EBAY_AUTH_BASE,
      apiBase: EBAY_API_BASE,
      scopes: EBAY_SCOPES.split(" "),
      redirectUri: EBAY_REDIRECT_URI,
      ruName: EBAY_RU_NAME,
      defaultMarketplaceId: EBAY_DEFAULT_MARKETPLACE_ID,
      merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
      marketplaces: MARKETPLACES,
      hasVerificationToken: Boolean(EBAY_VERIFICATION_TOKEN),
      notificationEndpoint: EBAY_NOTIFICATION_ENDPOINT,
      stateDir: EBAY_STATE_DIR,
      stateFile: EBAY_STATE_FILE,
      connection: sanitizeEbayConnectionForResponse(),
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
      return res.redirect("/ebay/publisher?error=oauth");
    }

    if (!code) {
      return res.redirect("/ebay/publisher?error=oauth");
    }

    if (!state || !ebayOAuthStates.has(state)) {
      return res.redirect("/ebay/publisher?error=oauth");
    }

    ebayOAuthStates.delete(state);

    const tokenData = await exchangeEbayCodeForTokens(code);
    await saveEbayTokens(tokenData);

    const userInfo = await getEbayUserInfo(ebayConnectionStore.accessToken);
    ebayConnectionStore.userInfo = userInfo;
    await saveEbayConnectionToDisk();

    return res.redirect("/ebay/publisher?connected=1");
  } catch (error) {
    console.log("EBAY CALLBACK ERROR", errorToSerializable(error));
    return res.redirect("/ebay/publisher?error=oauth");
  }
});

app.get("/ebay/connection", async (req, res) => {
  try {
    validateEbayEnv();

    if (ebayConnectionStore.refreshToken) {
      try {
        await ensureValidEbayAccessToken();
      } catch (error) {
        console.log("EBAY CONNECTION CHECK ERROR", error.message);
      }
    }

    return res.json({
      ok: true,
      connection: sanitizeEbayConnectionForResponse(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/ebay/token/refresh", async (req, res) => {
  try {
    const result = await refreshEbayAccessToken();

    const accessToken = await ensureValidEbayAccessToken();
    const userInfo = await getEbayUserInfo(accessToken);
    ebayConnectionStore.userInfo = userInfo;
    await saveEbayConnectionToDisk();

    return res.json({
      ok: true,
      message: "eBay token refreshed successfully",
      connection: result,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/disconnect", async (req, res) => {
  try {
    validateEbayEnv();

    await clearEbayConnectionOnDisk();

    return res.json({
      ok: true,
      message: "eBay connection cleared from disk and memory",
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/ebay/account/policies", async (req, res) => {
  try {
    const marketplaceId = getRequestedMarketplaceId(req);
    const result = await getAllEbayPolicies(marketplaceId);

    return res.json({
      ok: true,
      marketplaceId,
      result,
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
      req.query.merchantLocationKey || EBAY_MERCHANT_LOCATION_KEY
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

app.post("/ebay/publish/multi", async (req, res) => {
  try {
    const sku = String(req.body?.sku || "").trim();
    const sourceLanguage = getSourceLanguage(req.body?.sourceLanguage || "it");
    const categoryMap = req.body?.categoryMap || {};

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const result = await publishSkuToMultipleEbayMarkets({
      sku,
      sourceLanguage,
      categoryMap,
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
    const sku = String(req.body?.sku || "").trim();
    const sourceLanguage = getSourceLanguage(req.body?.sourceLanguage || "it");

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const result = await syncShopifySkuToEbay({ sku, sourceLanguage });
    return res.json(result);
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

    for (const variant of payload.variants || []) {
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

      if (variant.sku) {
        try {
          const ebayResult = await syncShopifySkuToEbay({
            sku: variant.sku,
            sourceLanguage: "it",
          });
          console.log("EBAY SYNC FROM PRODUCT WEBHOOK", JSON.stringify(ebayResult));
        } catch (error) {
          console.log("EBAY SYNC FROM PRODUCT WEBHOOK ERROR", errorToSerializable(error));
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

    try {
      const ebayResult = await syncShopifySkuToEbay({
        sku,
        sourceLanguage: "it",
      });
      console.log("EBAY SYNC FROM INVENTORY WEBHOOK", JSON.stringify(ebayResult));
    } catch (error) {
      console.log("EBAY SYNC FROM INVENTORY WEBHOOK ERROR", errorToSerializable(error));
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
    ebayConnected: ebayConnectionStore.connected,
    merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
    marketplaceId: EBAY_DEFAULT_MARKETPLACE_ID,
    marketplaces: MARKETPLACES,
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
  } catch (error) {
    console.error("STARTUP ERROR:", error.message);
    process.exit(1);
  }
}

start();
