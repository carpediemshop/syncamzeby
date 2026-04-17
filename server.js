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

const ebayCategoryRepairState = {
  lastRunAt: null,
  cursor: 0,
  processedSkus: new Set(),
  lastBatch: [],
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

function normalizeEbayAPlusBody(content = "") {
  const raw = cleanHtmlForEbay(String(content || "").trim());
  if (!raw) return "";

  const hasHtml = /<\/?[a-z][\s\S]*>/i.test(raw);
  if (hasHtml) {
    return raw;
  }

  return raw
    .split(/\n+/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map(
      (part) =>
        `<p style="margin:0 0 12px 0;font-size:14px;line-height:1.7;color:#333;">${part}</p>`
    )
    .join("");
}

function getEbayAPlusLabels(marketplaceId) {
  const id = String(marketplaceId || "").trim();

  if (id === "EBAY_DE") {
    return {
      sectionTitle: "PRODUKT-HIGHLIGHTS",
      original: "Originalprodukt",
      shipping: "Schneller Versand",
      support: "Kundenservice",
      closing: "Sicher einkaufen. Schneller Versand aus Italien.",
    };
  }

  if (id === "EBAY_FR") {
    return {
      sectionTitle: "POINTS FORTS DU PRODUIT",
      original: "Produit original",
      shipping: "Expédition rapide",
      support: "Service client dédié",
      closing: "Achetez en toute sécurité. Expédition rapide depuis l’Italie.",
    };
  }

  if (id === "EBAY_ES") {
    return {
      sectionTitle: "PUNTOS FUERTES DEL PRODUCTO",
      original: "Producto original",
      shipping: "Envío rápido",
      support: "Atención al cliente dedicada",
      closing: "Compra con seguridad. Envío rápido desde Italia.",
    };
  }

  if (id === "EBAY_GB") {
    return {
      sectionTitle: "PRODUCT HIGHLIGHTS",
      original: "Original product",
      shipping: "Fast shipping",
      support: "Dedicated customer support",
      closing: "Shop with confidence. Fast shipping from Italy.",
    };
  }

  return {
    sectionTitle: "PUNTI DI FORZA DEL PRODOTTO",
    original: "Prodotto originale",
    shipping: "Spedizione veloce",
    support: "Assistenza clienti dedicata",
    closing: "Acquista in sicurezza. Spedizione rapida dall’Italia.",
  };
}

function toPositiveNumber(value, fallback = 0) {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return n;
  return fallback;
}

function toNonNegativeNumber(value, fallback = 0) {
  const n = Number(value);
  if (Number.isFinite(n) && n >= 0) return n;
  return fallback;
}

function buildPackageDimensions(shopifyVariant, fallbackCm = 20) {
  const fallback = toPositiveNumber(fallbackCm, 20);

  const product = shopifyVariant?.product || {};

  const rawHeight =
    product.height ??
    product.packageHeight ??
    product.shippingHeight ??
    shopifyVariant?.height ??
    shopifyVariant?.packageHeight;

  const rawWidth =
    product.width ??
    product.packageWidth ??
    product.shippingWidth ??
    shopifyVariant?.width ??
    shopifyVariant?.packageWidth;

  const rawLength =
    product.length ??
    product.depth ??
    product.packageLength ??
    product.shippingLength ??
    shopifyVariant?.length ??
    shopifyVariant?.packageLength;

  return {
    height: toPositiveNumber(rawHeight, fallback),
    width: toPositiveNumber(rawWidth, fallback),
    length: toPositiveNumber(rawLength, fallback),
    unit: "CENTIMETER",
  };
}

function buildEbayAPlusGallery(imageUrls = []) {
  const images = safeArray(imageUrls).filter(Boolean).slice(0, 6);
  if (!images.length) return "";

  const hero = String(images[0]).trim();
  const secondary = images.slice(1);

  let secondaryRows = "";
  for (let i = 0; i < secondary.length; i += 2) {
    const left = secondary[i] ? String(secondary[i]).trim() : "";
    const right = secondary[i + 1] ? String(secondary[i + 1]).trim() : "";

    secondaryRows += `
      <tr>
        <td valign="top" style="width:50%;padding:0 7px 14px 0;">
          ${
            left
              ? `
          <div style="border:1px solid #e5e5e5;border-radius:12px;overflow:hidden;background:#fff;">
            <img src="${left}" alt="Product image ${i + 2}" style="display:block;width:100%;height:auto;border:0;">
          </div>
          `
              : ""
          }
        </td>
        <td valign="top" style="width:50%;padding:0 0 14px 7px;">
          ${
            right
              ? `
          <div style="border:1px solid #e5e5e5;border-radius:12px;overflow:hidden;background:#fff;">
            <img src="${right}" alt="Product image ${i + 3}" style="display:block;width:100%;height:auto;border:0;">
          </div>
          `
              : ""
          }
        </td>
      </tr>
    `;
  }

  return `
    <div style="margin:0 0 24px 0;">
      <div style="border:1px solid #e5e5e5;border-radius:14px;overflow:hidden;background:#fff;margin-bottom:14px;">
        <img src="${hero}" alt="Product hero image" style="display:block;width:100%;height:auto;border:0;">
      </div>

      ${
        secondaryRows
          ? `
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
        ${secondaryRows}
      </table>
      `
          : ""
      }
    </div>
  `.trim();
}

function buildEbayAPlusDescription({
  marketplaceId,
  title,
  translatedDescription,
  imageUrls = [],
}) {
  const labels = getEbayAPlusLabels(marketplaceId);
  const safeTitle = cleanHtmlForEbay(String(title || "").trim());
  const bodyHtml = normalizeEbayAPlusBody(translatedDescription);
  const galleryHtml = buildEbayAPlusGallery(imageUrls);

  return `
<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.6;color:#222;background:#ffffff;">
  <div style="max-width:980px;margin:0 auto;background:#fff;border:1px solid #d9d9d9;border-radius:16px;overflow:hidden;">

    <div style="background:#f7f7f7;padding:22px 24px;border-bottom:1px solid #ececec;">
      <div style="font-size:12px;letter-spacing:1px;color:#777;text-transform:uppercase;font-weight:700;margin-bottom:10px;">
        ${labels.sectionTitle}
      </div>
      <h2 style="margin:0;font-size:28px;line-height:1.25;color:#111;font-weight:700;">
        ${safeTitle}
      </h2>
    </div>

    <div style="padding:24px;">

      ${galleryHtml}

      <div style="background:#fafafa;border:1px solid #ececec;border-radius:14px;padding:20px;margin-bottom:22px;">
        <div style="font-size:18px;line-height:1.4;color:#111;font-weight:700;margin:0 0 14px 0;">
          ${labels.sectionTitle}
        </div>
        <div style="font-size:14px;line-height:1.7;color:#333;">
          ${bodyHtml}
        </div>
      </div>

      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin:0 0 22px 0;">
        <tr>
          <td valign="top" style="width:33.33%;padding:0 8px 0 0;">
            <div style="height:100%;background:#ffffff;border:1px solid #e8e8e8;border-radius:14px;padding:18px;text-align:center;">
              <div style="font-size:26px;line-height:1;margin-bottom:10px;">✓</div>
              <div style="font-size:14px;font-weight:700;color:#111;line-height:1.4;">
                ${labels.original}
              </div>
            </div>
          </td>
          <td valign="top" style="width:33.33%;padding:0 4px;">
            <div style="height:100%;background:#ffffff;border:1px solid #e8e8e8;border-radius:14px;padding:18px;text-align:center;">
              <div style="font-size:26px;line-height:1;margin-bottom:10px;">🚚</div>
              <div style="font-size:14px;font-weight:700;color:#111;line-height:1.4;">
                ${labels.shipping}
              </div>
            </div>
          </td>
          <td valign="top" style="width:33.33%;padding:0 0 0 8px;">
            <div style="height:100%;background:#ffffff;border:1px solid #e8e8e8;border-radius:14px;padding:18px;text-align:center;">
              <div style="font-size:26px;line-height:1;margin-bottom:10px;">★</div>
              <div style="font-size:14px;font-weight:700;color:#111;line-height:1.4;">
                ${labels.support}
              </div>
            </div>
          </td>
        </tr>
      </table>

      <div style="background:#f6f6f6;border:1px solid #e9e9e9;border-radius:14px;padding:16px 18px;font-size:13px;line-height:1.6;color:#555;text-align:center;">
        ${labels.closing}
      </div>

    </div>
  </div>
</div>`.trim();
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

function serializeCategoryRepairState() {
  return {
    lastRunAt: ebayCategoryRepairState.lastRunAt,
    cursor: ebayCategoryRepairState.cursor,
    processedSkus: Array.from(ebayCategoryRepairState.processedSkus).sort(),
    lastBatch: Array.isArray(ebayCategoryRepairState.lastBatch)
      ? ebayCategoryRepairState.lastBatch
      : [],
  };
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

async function resetEbayCategoryRepairState() {
  ebayCategoryRepairState.lastRunAt = null;
  ebayCategoryRepairState.cursor = 0;
  ebayCategoryRepairState.processedSkus = new Set();
  ebayCategoryRepairState.lastBatch = [];
  await saveEbayCategoryRepairStateToDisk();
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

async function saveEbayCategoryRepairStateToDisk() {
  await ensureEbayStateDir();

  await fs.writeFile(
    EBAY_CATEGORY_REPAIR_STATE_FILE,
    JSON.stringify(
      {
        savedAt: nowIso(),
        ...serializeCategoryRepairState(),
      },
      null,
      2
    ),
    "utf8"
  );
}

async function loadEbayCategoryRepairStateFromDisk() {
  try {
    await ensureEbayStateDir();
    const raw = await fs.readFile(EBAY_CATEGORY_REPAIR_STATE_FILE, "utf8");
    const data = JSON.parse(raw);

    ebayCategoryRepairState.lastRunAt = data?.lastRunAt || null;
    ebayCategoryRepairState.cursor = Number(data?.cursor || 0);
    ebayCategoryRepairState.processedSkus = new Set(
      safeArray(data?.processedSkus).map((x) => String(x).trim()).filter(Boolean)
    );
    ebayCategoryRepairState.lastBatch = safeArray(data?.lastBatch).map((x) =>
      String(x).trim()
    );

    console.log("EBAY CATEGORY REPAIR STATE LOADED", {
      lastRunAt: ebayCategoryRepairState.lastRunAt,
      cursor: ebayCategoryRepairState.cursor,
      processedCount: ebayCategoryRepairState.processedSkus.size,
      lastBatchCount: ebayCategoryRepairState.lastBatch.length,
    });
  } catch (error) {
    if (error.code === "ENOENT") {
      console.log("EBAY CATEGORY REPAIR STATE FILE NOT FOUND, STARTING CLEAN");
      return;
    }

    console.log("EBAY CATEGORY REPAIR STATE LOAD ERROR", error.message);
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
  const entries = await Promise.all(
    MARKETPLACES.map(async (market) => {
      const [translatedTitle, translatedDescription] = await Promise.all([
        translateTextWithEbay({
          from: sourceLanguage,
          to: market.language,
          text: title,
          translationContext: "ITEM_TITLE",
        }),
        translateTextWithEbay({
          from: sourceLanguage,
          to: market.language,
          text: descriptionHtml,
          translationContext: "ITEM_DESCRIPTION",
        }),
      ]);

      return [
        market.marketplaceId,
        {
          marketplaceId: market.marketplaceId,
          locale: market.locale,
          language: market.language,
          site: market.site,
          translatedTitle,
          translatedDescription,
        },
      ];
    })
  );

  return Object.fromEntries(entries);
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
  vendor = "",
}) {
  const tree = await getDefaultCategoryTreeId(marketplaceId);
  const categoryTreeId = tree?.categoryTreeId;

  if (!categoryTreeId) {
    throw new Error("Could not determine eBay default category tree");
  }

  const fakeVariantForFiltering = {
    product: {
      title,
      productType,
      categoryFullName: shopifyCategory,
    },
  };

  const queries = buildCategoryQueries({
    title,
    shopifyCategory,
    productType,
    vendor,
  });

  const allSuggestions = [];

  for (const q of queries) {
    const suggestions = await getCategorySuggestions({ categoryTreeId, q });

    const rows = safeArray(suggestions?.categorySuggestions)
      .map((row) => ({
        query: q,
        categoryId: row?.category?.categoryId || row?.categoryId || null,
        categoryName: row?.category?.categoryName || row?.categoryName || null,
        categoryTreeId,
      }))
      .filter(
        (row) =>
          row.categoryId &&
          row.categoryName &&
          !isWrongCategorySuggestionForProduct(
            fakeVariantForFiltering,
            row.categoryName
          )
      );

    allSuggestions.push(...rows);
  }

  const deduped = [];
  const seen = new Set();

  for (const row of allSuggestions) {
    const key = `${row.categoryId}::${row.categoryName}`;
    if (!seen.has(key)) {
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

function normalizeRuleText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function includesOneOf(text, keywords = []) {
  const safe = normalizeRuleText(text);
  return keywords.some((keyword) => safe.includes(normalizeRuleText(keyword)));
}

const CATEGORY_RULES = [
  {
    key: "macchine_per_pasta",
    keywords: [
      "marcato",
      "macchina per pasta",
      "macchine per pasta",
      "pasta machine",
      "sfogliatrice",
      "tagliapasta",
      "atlas 150",
      "atlas 180",
      "ampia 150",
      "regina marcato",
    ],
    searchQuery: "macchine stampi presse per pasta",
  },
  {
    key: "caffe_capsule_cialde",
    keywords: [
      "capsule",
      "cialde",
      "caffe",
      "caffè",
      "iperespresso",
      "illy capsule",
      "illy cialde",
      "caffe macinato",
      "caffe in grani",
    ],
    searchQuery: "capsule cialde caffe",
  },
  {
    key: "macchine_caffe",
    keywords: [
      "macchina caffe",
      "macchine caffe",
      "macchina da caffe",
      "macchine da caffe",
      "espresso machine",
      "macchina espresso",
      "illy machine",
      "bialetti elettrica",
      "caffettiera elettrica",
    ],
    searchQuery: "macchine da caffe espresso",
  },
  {
    key: "moka_caffettiere",
    keywords: [
      "moka",
      "caffettiera",
      "caffettiere",
      "bialetti moka",
      "moka express",
      "venus",
      "brikka",
      "kitty",
      "fiammetta",
      "rainbow",
    ],
    searchQuery: "caffettiere moka",
  },
  {
    key: "ricambi_caffettiere",
    keywords: [
      "guarnizione moka",
      "ricambio moka",
      "ricambi moka",
      "ricambi caffettiera",
      "imbuto moka",
      "manico moka",
      "pomolo moka",
      "filtro moka",
      "guarnizioni bialetti",
    ],
    searchQuery: "ricambi caffettiere moka",
  },
  {
    key: "tazze",
    keywords: [
      "set tazzine",
      "piattino",
      "cucchiaino caffe",
      "illy art collection",
    ],
    searchQuery: "tazze tazzine caffe",
  },
  {
    key: "utensili_faidate",
    keywords: [
      "ryobi",
      "milwaukee",
      "beta",
      "trapano",
      "avvitatore",
      "smerigliatrice",
      "utensile",
      "elettroutensile",
      "chiave",
      "cacciavite",
      "martello",
      "sega",
      "batteria utensile",
    ],
    searchQuery: "utensili elettrici fai da te",
  },
  {
    key: "elettrodomestici",
    keywords: [
      "ariete",
      "stella",
      "black+decker",
      "friggitrice",
      "frullatore",
      "tostapane",
      "bollitore",
      "aspirapolvere",
      "elettrodomestico",
      "elettrodomestici",
    ],
    searchQuery: "piccoli elettrodomestici",
  },
  {
    key: "accessori_cucina",
    keywords: [
      "calder",
      "fackelmann",
      "ghidini",
      "accessori cucina",
      "utensili cucina",
      "mestolo",
      "frusta",
      "pelapatate",
      "tagliere",
      "grattugia",
      "apriscatole",
    ],
    searchQuery: "accessori utensili cucina",
  },
  {
    key: "posate_pentole_hotellerie",
    keywords: [
      "lagostina",
      "sanelli ambrogio",
      "pentola",
      "pentole",
      "padella",
      "padelle",
      "coltello cucina",
      "coltelli cucina",
      "posate",
      "forchetta",
      "cucchiaio",
      "hotellerie",
    ],
    searchQuery: "pentole posate coltelli cucina",
  },
  {
    key: "infanzia",
    keywords: [
      "chicco",
      "infanzia",
      "neonato",
      "bambino",
      "bambini",
      "passeggino",
      "passeggio",
      "seggiolino",
      "sicurezza bambino",
      "allattamento",
      "giocattolo chicco",
      "giocattoli chicco",
    ],
    searchQuery: "prima infanzia bambini",
  },
  {
    key: "igiene_bambini",
    keywords: [
      "fiocchi di riso",
      "olio shampoo",
      "detergente",
      "crema corpo",
      "talco",
      "bagnetto",
      "igiene bambini",
      "igiene neonato",
    ],
    searchQuery: "igiene bambini neonato",
  },
  {
    key: "igiene_persona",
    keywords: [
      "igiene persona",
      "detergente corpo",
      "sapone",
      "shampoo",
      "bagnodoccia",
      "cura persona",
    ],
    searchQuery: "igiene cura persona",
  },
  {
    key: "giardino_piscina",
    keywords: [
      "hozelock",
      "gf garden",
      "divina garden",
      "piscina",
      "giardino",
      "irrigazione",
      "tubo giardino",
      "lancia irrigazione",
      "pompa piscina",
      "accessori piscina",
    ],
    searchQuery: "giardino piscina irrigazione",
  },
  {
    key: "caminetti_elettrici",
    keywords: [
      "divina fire",
      "caminetto elettrico",
      "caminetti elettrici",
      "fireplace",
      "stufa elettrica",
    ],
    searchQuery: "caminetti elettrici",
  },
  {
    key: "orologi",
    keywords: [
      "fumagazzi",
      "orologio",
      "orologi",
      "watch",
      "wrist watch",
      "cronografo",
    ],
    searchQuery: "orologi da polso",
  },
  {
    key: "personalizzati",
    keywords: [
      "carpe diem art",
      "carpe diem shop",
      "personalizzato",
      "personalizzata",
      "personalizzati",
      "incisione",
      "laser",
      "plexiglass",
      "legno",
      "bomboniera",
      "cake topper",
      "segnatavolo",
      "tavolo torta",
    ],
    searchQuery: "articoli personalizzati regalo",
  },
];

function getShopifyCategoryContext(shopifyVariant) {
  const product = shopifyVariant?.product || {};

  const vendor = String(product.vendor || "").trim();
  const productType = String(product.productType || "").trim();
  const categoryFullName = String(product.categoryFullName || "").trim();
  const title = String(product.title || "").trim();
  const variantTitle = String(shopifyVariant?.variantTitle || "").trim();
  const descriptionText = String(product.descriptionText || "").trim();

  return {
    vendor,
    productType,
    categoryFullName,
    title,
    variantTitle,
    descriptionText,
    combined: [
      vendor,
      productType,
      categoryFullName,
      title,
      variantTitle,
      descriptionText,
    ]
      .filter(Boolean)
      .join(" | "),
  };
}

function detectCategoryRule(shopifyVariant) {
  const ctx = getShopifyCategoryContext(shopifyVariant);

  for (const rule of CATEGORY_RULES) {
    if (includesOneOf(ctx.combined, rule.keywords)) {
      return rule;
    }
  }

  return null;
}

function buildCategorySearchQueries(shopifyVariant) {
  const ctx = getShopifyCategoryContext(shopifyVariant);
  const detectedRule = detectCategoryRule(shopifyVariant);

  const queries = [];

  if (detectedRule?.searchQuery) {
    queries.push(`${ctx.vendor} ${detectedRule.searchQuery}`.trim());
    queries.push(`${ctx.productType} ${detectedRule.searchQuery}`.trim());
  }

  if (ctx.categoryFullName) queries.push(ctx.categoryFullName);
  if (ctx.productType) queries.push(ctx.productType);
  if (ctx.vendor && ctx.title) queries.push(`${ctx.vendor} ${ctx.title}`.trim());
  if (ctx.title) queries.push(ctx.title);

  const deduped = [];
  const seen = new Set();

  for (const q of queries.map((x) => String(x || "").trim()).filter(Boolean)) {
    const key = normalizeRuleText(q);
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(q);
    }
  }

  return deduped.slice(0, 6);
}

async function suggestEbayCategoryEnhanced({
  marketplaceId,
  shopifyVariant,
}) {
  const tree = await getDefaultCategoryTreeId(marketplaceId);
  const categoryTreeId = tree?.categoryTreeId;

  if (!categoryTreeId) {
    throw new Error("Could not determine eBay default category tree");
  }

  const queries = buildCategorySearchQueries(shopifyVariant);
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
    queries,
    suggestions: deduped,
    best: deduped[0] || null,
  };
}

async function resolveCategoryIdForMarketplace({
  sku,
  marketplaceId,
  shopifyVariant,
}) {
  const configured = getConfiguredCategoryForMarketplace(
    sku,
    marketplaceId,
    shopifyVariant
  );

  if (configured) {
    return configured;
  }

  const suggestion = await suggestEbayCategoryEnhanced({
    marketplaceId,
    shopifyVariant,
  });

  const categoryId = String(suggestion?.best?.categoryId || "").trim();

  if (!categoryId) {
    throw new Error(
      `No category found for ${marketplaceId} and SKU ${sku || ""}`
    );
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
          inventoryItem {
            id
            measurement {
              weight {
                unit
                value
              }
            }
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

  const weightValue = toPositiveNumber(
    variant?.inventoryItem?.measurement?.weight?.value
  ) || 0.5;

  const rawWeightUnit = String(
    variant?.inventoryItem?.measurement?.weight?.unit || ""
  )
    .trim()
    .toUpperCase();

  let weightUnit = "KILOGRAM";
  if (["GRAMS", "GRAM", "G"].includes(rawWeightUnit)) weightUnit = "GRAM";
  if (["KILOGRAMS", "KILOGRAM", "KG"].includes(rawWeightUnit)) weightUnit = "KILOGRAM";
  if (["POUNDS", "POUND", "LBS", "LB"].includes(rawWeightUnit)) weightUnit = "POUND";
  if (["OUNCES", "OUNCE", "OZ"].includes(rawWeightUnit)) weightUnit = "OUNCE";

  return {
    sku: variant.sku,
    barcode: variant.barcode || "",
    price: normalizeMoney(variant.price),
    inventoryQuantity: Number(variant.inventoryQuantity || 0),
    variantTitle: variant.title || "",
    selectedOptions: variant.selectedOptions || [],
    weight: weightValue,
    weightUnit,
    packageHeight: 20,
    packageWidth: 20,
    packageLength: 20,
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

async function getShopifyVariantsBatch({ first = 100, after = null }) {
  const query = `
    query GetVariantsBatch($first: Int!, $after: String) {
      productVariants(first: $first, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          sku
          title
          barcode
          price
          inventoryQuantity
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
              }
            }
          }
        }
      }
    }
  `;

  const data = await shopifyGraphQL(query, {
    first,
    after,
  });

  const root = data?.productVariants || {};
  const nodes = safeArray(root?.nodes);

  return {
    nodes: nodes
      .map((variant) => {
        const images = safeArray(variant?.product?.images?.nodes)
          .map((img) => img?.url)
          .filter(Boolean);

        const descriptionHtml = cleanHtmlForEbay(
          variant?.product?.descriptionHtml || ""
        );
        const descriptionText = truncateText(stripHtml(descriptionHtml), 4000);

        return {
          sku: String(variant?.sku || "").trim(),
          barcode: variant?.barcode || "",
          price: normalizeMoney(variant?.price),
          inventoryQuantity: Number(variant?.inventoryQuantity || 0),
          variantTitle: variant?.title || "",
          selectedOptions: safeArray(variant?.selectedOptions),
          product: {
            id: variant?.product?.id || null,
            title: variant?.product?.title || "",
            descriptionHtml,
            descriptionText,
            vendor: variant?.product?.vendor || "",
            productType: variant?.product?.productType || "",
            categoryFullName: variant?.product?.category?.fullName || "",
            onlineStoreUrl: variant?.product?.onlineStoreUrl || "",
            imageUrls: images,
          },
        };
      })
      .filter((x) => x.sku),
    hasNextPage: Boolean(root?.pageInfo?.hasNextPage),
    endCursor: root?.pageInfo?.endCursor || null,
  };
}

async function getShopifyVariantsPage({ after = null, first = 100 } = {}) {
  const query = `
    query GetVariantsPage($first: Int!, $after: String) {
      productVariants(first: $first, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          sku
        }
      }
    }
  `;

  const data = await shopifyGraphQL(query, { first, after });
  const page = data?.productVariants || {};

  return {
    nodes: Array.isArray(page?.nodes) ? page.nodes : [],
    hasNextPage: Boolean(page?.pageInfo?.hasNextPage),
    endCursor: page?.pageInfo?.endCursor || null,
  };
}

async function getAllShopifySkus({ limit = 0 } = {}) {
  const skus = [];
  const seen = new Set();

  let after = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const page = await getShopifyVariantsPage({ after, first: 100 });

    for (const node of safeArray(page.nodes)) {
      const sku = normalizeSku(node?.sku);
      if (!sku || seen.has(sku)) continue;

      seen.add(sku);
      skus.push(sku);

      if (limit > 0 && skus.length >= limit) {
        return skus;
      }
    }

    hasNextPage = Boolean(page.hasNextPage);
    after = page.endCursor || null;

    if (!after) break;
  }

  return skus;
}

// =========================
// EBAY INVENTORY / OFFER HELPERS
// =========================

function truncateAspectValue(value, max = 65) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.length > max ? text.slice(0, max).trim() : text;
}

function sanitizeAspectToken(value) {
  return truncateAspectValue(
    String(value || "")
      .replace(/[|/\\]+/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
    65
  );
}

function buildBrandValue(shopifyVariant) {
  return sanitizeAspectToken(
    firstNonEmpty(
      shopifyVariant?.product?.vendor,
      shopifyVariant?.vendor,
      ""
    )
  );
}

function buildProductValue(shopifyVariant) {
  return sanitizeAspectToken(
    firstNonEmpty(
      shopifyVariant?.product?.productType,
      shopifyVariant?.product?.categoryFullName,
      shopifyVariant?.productType,
      shopifyVariant?.product?.title,
      ""
    )
  );
}

function buildModelValue(shopifyVariant) {
  const optionModel = safeArray(shopifyVariant?.selectedOptions).find((opt) =>
    /modello|model|modell|mod[eè]le/i.test(String(opt?.name || "").trim())
  );

  const optionModelValue = sanitizeAspectToken(optionModel?.value || "");
  if (optionModelValue) return optionModelValue;

  const barcode = sanitizeAspectToken(shopifyVariant?.barcode || "");
  if (barcode) return barcode;

  const variantTitle = String(shopifyVariant?.variantTitle || "").trim();
  if (
    variantTitle &&
    !["Default Title", "Titolo predefinito"].includes(variantTitle)
  ) {
    return sanitizeAspectToken(variantTitle);
  }

  return sanitizeAspectToken(shopifyVariant?.sku || "");
}

function buildColorValue(shopifyVariant) {
  const colorOption = safeArray(shopifyVariant?.selectedOptions).find((opt) =>
    /colore|color|colour|farbe|couleur/i.test(String(opt?.name || "").trim())
  );

  const optionColorValue = sanitizeAspectToken(colorOption?.value || "");
  if (optionColorValue) return optionColorValue;

  const title = String(shopifyVariant?.product?.title || "").toLowerCase();

  if (/\bblu|blue|bleu|azul\b/.test(title)) return "Blu";
  if (/\bnero|black|noir|schwarz\b/.test(title)) return "Nero";
  if (/\bbianco|white|blanc|weiß|weiss\b/.test(title)) return "Bianco";
  if (/\brosso|red|rouge|rot\b/.test(title)) return "Rosso";
  if (/\bverde|green|vert|grün|grun\b/.test(title)) return "Verde";
  if (/\bgiallo|yellow|jaune|gelb\b/.test(title)) return "Giallo";
  if (/\brosa|pink|rose\b/.test(title)) return "Rosa";
  if (/\bgrigio|grey|gray|gris|grau\b/.test(title)) return "Grigio";

  return "";
}

function buildCompatibleBrandValue(shopifyVariant) {
  return sanitizeAspectToken(
    firstNonEmpty(
      shopifyVariant?.product?.vendor,
      ""
    )
  );
}

function buildTypeValue(shopifyVariant) {
  return sanitizeAspectToken(
    firstNonEmpty(
      shopifyVariant?.product?.productType,
      shopifyVariant?.product?.categoryFullName,
      shopifyVariant?.product?.title,
      ""
    )
  );
}

function setAspectIfValue(aspects, keys, value) {
  const safeValue = sanitizeAspectToken(value);
  if (!safeValue) return;

  for (const key of keys) {
    if (!aspects[key]) {
      aspects[key] = [safeValue];
    }
  }
}

function buildDefaultAspects(shopifyVariant) {
  const aspects = {};

  const brandValue = buildBrandValue(shopifyVariant);
  const productValue = buildProductValue(shopifyVariant);
  const modelValue = buildModelValue(shopifyVariant);
  const colorValue = buildColorValue(shopifyVariant);
  const compatibleBrandValue = buildCompatibleBrandValue(shopifyVariant);
  const typeValue = buildTypeValue(shopifyVariant);

  setAspectIfValue(aspects, ["Brand", "Marca", "Marke", "Marque"], brandValue);

  setAspectIfValue(
    aspects,
    ["Product", "Prodotto", "Producto", "Produit", "Produkt", "Produktart"],
    productValue
  );

  setAspectIfValue(
    aspects,
    ["Model", "Modello", "Modelo", "Modell", "Modèle"],
    modelValue
  );

  setAspectIfValue(
    aspects,
    ["Compatible Brand", "Marca compatibile", "Compatible Brand/Model", "Markenkompatibilität", "Marque compatible"],
    compatibleBrandValue
  );

  setAspectIfValue(
    aspects,
    ["Type", "Tipo", "Tipo de producto", "Typ"],
    typeValue
  );

  setAspectIfValue(
    aspects,
    ["Color", "Colour", "Colore", "Farbe", "Couleur"],
    colorValue
  );

  for (const option of safeArray(shopifyVariant?.selectedOptions)) {
    const name = String(option?.name || "").trim();
    const value = sanitizeAspectToken(option?.value || "");
    if (!name || !value) continue;

    if (!aspects[name]) {
      aspects[name] = [value];
    }
  }

  if (shopifyVariant?.barcode) {
    const ean = sanitizeAspectToken(shopifyVariant.barcode);
    if (ean) {
      aspects.EAN = [ean];
    }
  }

  return aspects;
}

function sanitizeEbayTitle(value, max = 80) {
  let text = String(value || "").trim();
  if (!text) return "";

  text = text
    .replace(/[|/\\]+/g, " ")
    .replace(/[-–—,:;.!?()[\]{}"'`´]+/g, " ")
    .replace(/\s*&\s*/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (text.length <= max) return text;

  const words = text.split(" ");
  let out = "";

  for (const word of words) {
    const next = out ? `${out} ${word}` : word;
    if (next.length > max) break;
    out = next;
  }

  return out || text.slice(0, max).trim();
}

function buildInventoryItemPayload(shopifyVariant, translatedTitle = "") {
  const titleBase = firstNonEmpty(translatedTitle, shopifyVariant?.product?.title);
  const variantTitle = firstNonEmpty(shopifyVariant?.variantTitle);

  const title =
    variantTitle &&
    !["Default Title", "Titolo predefinito"].includes(variantTitle)
      ? `${titleBase} ${variantTitle}`.slice(0, 80)
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

async function repairCategoryForPublishedOffersBySku({
  sku,
  sourceLanguage = "it",
  marketplaces = MARKETPLACES.map((m) => m.marketplaceId),
}) {
  const safeSku = normalizeSku(sku);
  if (!safeSku) {
    throw new Error("repairCategoryForPublishedOffersBySku: missing sku");
  }

  const shopifyVariant = await getShopifyVariantBySku(safeSku);
  if (!shopifyVariant) {
    throw new Error(`Shopify SKU not found: ${safeSku}`);
  }

  const translations = await buildMarketplaceTranslations({
    sourceLanguage,
    title: shopifyVariant.product.title,
    descriptionHtml:
      shopifyVariant.product.descriptionHtml || shopifyVariant.product.descriptionText,
  });

  const results = [];

  for (const marketplaceId of marketplaces) {
    const market = getMarketplaceMeta(marketplaceId);

    if (!market) {
      results.push({
        marketplaceId,
        ok: false,
        error: "unknown marketplace",
      });
      continue;
    }

    try {
      const offers = await getOffersBySku({
        sku: safeSku,
        marketplaceId,
      });

      const existingOffer =
        safeArray(offers).find((o) => String(o?.marketplaceId || "") === marketplaceId) ||
        null;

      if (!existingOffer?.offerId) {
        results.push({
          marketplaceId,
          locale: market.locale,
          site: market.site,
          ok: false,
          error: "offer not found for marketplace",
        });
        continue;
      }

      const offerId = String(existingOffer.offerId);
      const fullOffer = await getOffer(offerId);

      const finalCategoryId = await resolveCategoryIdForMarketplace({
        sku: safeSku,
        marketplaceId,
        shopifyVariant,
      });

      const translation = translations[marketplaceId] || null;

      const marketplaceDescriptionHtml = buildEbayAPlusDescription({
        marketplaceId,
        title: translation?.translatedTitle || shopifyVariant.product.title,
        translatedDescription:
          translation?.translatedDescription ||
          shopifyVariant.product.descriptionHtml ||
          shopifyVariant.product.descriptionText,
        imageUrls: shopifyVariant.product.imageUrls || [],
      });

      const updatedPayload = {
        sku: fullOffer?.sku || safeSku,
        marketplaceId,
        format: fullOffer?.format || EBAY_DEFAULT_FORMAT,
        availableQuantity: Math.max(
          0,
          Number(shopifyVariant.inventoryQuantity ?? fullOffer?.availableQuantity ?? 0)
        ),
        categoryId: finalCategoryId,
        merchantLocationKey:
          fullOffer?.merchantLocationKey || getMerchantLocationKey(marketplaceId),
        pricingSummary: {
          price: {
            value: String(normalizeMoney(shopifyVariant.price)),
            currency:
              fullOffer?.pricingSummary?.price?.currency ||
              market.currency ||
              "EUR",
          },
        },
        listingPolicies: {
          paymentPolicyId:
            fullOffer?.listingPolicies?.paymentPolicyId ||
            fullOffer?.paymentPolicyId,
          returnPolicyId:
            fullOffer?.listingPolicies?.returnPolicyId ||
            fullOffer?.returnPolicyId,
          fulfillmentPolicyId:
            fullOffer?.listingPolicies?.fulfillmentPolicyId ||
            fullOffer?.fulfillmentPolicyId,
        },
        listingDuration: fullOffer?.listingDuration || EBAY_DEFAULT_LISTING_DURATION,
        listingDescription: cleanHtmlForEbay(marketplaceDescriptionHtml),
      };

      const updateResult = await updateOffer({
        offerId,
        payload: updatedPayload,
        contentLanguage: normalizeContentLanguage(market.locale, "it-IT"),
      });

      let publishResult = null;
      let publishError = null;

      try {
        publishResult = await publishOffer({ offerId });
      } catch (error) {
        publishError = errorToSerializable(error);
      }

      const refreshedOffer = await getOffer(offerId).catch(() => null);

      results.push({
        marketplaceId,
        locale: market.locale,
        site: market.site,
        ok: !publishError,
        offerId,
        oldCategoryId: fullOffer?.categoryId || null,
        newCategoryId: finalCategoryId,
        previousStatus: fullOffer?.status || null,
        currentStatus: refreshedOffer?.status || null,
        updateResult,
        publishResult,
        publishError,
      });
    } catch (error) {
      results.push({
        marketplaceId,
        locale: market?.locale || null,
        site: market?.site || null,
        ok: false,
        error: errorToSerializable(error),
      });
    }
  }

  return {
    ok: results.every((r) => r.ok),
    sku: safeSku,
    sourceLanguage,
    results,
  };
}

async function ensureInventoryItemForMarketplace({
  sku,
  shopifyVariant,
  translation,
}) {
  const contentLanguage = normalizeContentLanguage(translation?.locale, "it-IT");

  const inventoryItemPayload = buildInventoryItemPayload(
    shopifyVariant,
    translation?.translatedTitle || shopifyVariant.product.title
  );

  const inventoryItemUpdate = await createOrReplaceInventoryItem({
    sku,
    payload: inventoryItemPayload,
    contentLanguage,
  });

  return {
    contentLanguage,
    inventoryItemPayload,
    inventoryItemUpdate,
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
        shopifyVariant.product.descriptionHtml ||
        shopifyVariant.product.descriptionText,
    })
  )[marketplaceId];

  const inventoryData = await ensureInventoryItemForMarketplace({
    sku,
    shopifyVariant,
    translation,
  });

  const finalCategoryId =
    String(categoryId || "").trim() ||
    (await resolveCategoryIdForMarketplace({
      sku,
      marketplaceId,
      shopifyVariant,
    }));

  const marketplaceDescriptionHtml = buildEbayAPlusDescription({
    marketplaceId,
    title: translation?.translatedTitle || shopifyVariant.product.title,
    translatedDescription:
      translation?.translatedDescription ||
      shopifyVariant.product.descriptionHtml ||
      shopifyVariant.product.descriptionText,
    imageUrls: shopifyVariant.product.imageUrls || [],
  });

  const upsert = await upsertOfferForMarketplace({
    sku,
    price: shopifyVariant.price,
    quantity: shopifyVariant.inventoryQuantity,
    marketplaceId,
    categoryId: finalCategoryId,
    translatedDescription: marketplaceDescriptionHtml,
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
    translatedTitlePreview:
      translation?.translatedTitle || shopifyVariant.product.title,
    translatedDescriptionPreview:
      translation?.translatedDescription || shopifyVariant.product.descriptionText,
    inventoryItemUpdate: inventoryData.inventoryItemUpdate,
    inventoryItemPayload: inventoryData.inventoryItemPayload,
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

async function repairAllEbayListings({
  sourceLanguage = "it",
  marketplaces = MARKETPLACES.map((m) => m.marketplaceId),
  limit = 0,
}) {
  const skus = await getAllShopifySkus({ limit });

  const results = [];

  for (const sku of skus) {
    try {
      const result = await publishSkuToMultipleEbayMarkets({
        sku,
        sourceLanguage,
        categoryMap: {},
        marketplaces,
      });

      results.push({
        sku,
        ok: result?.ok === true,
        result,
      });
    } catch (error) {
      results.push({
        sku,
        ok: false,
        error: errorToSerializable(error),
      });
    }
  }

  return {
    ok: results.every((r) => r.ok),
    total: skus.length,
    processed: results.length,
    results,
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

  const inventoryContentLanguage = normalizeContentLanguage(
    languageToLocale(sourceLanguage),
    "it-IT"
  );

  const inventoryItemPayload = buildInventoryItemPayload(shopifyVariant);

  let inventoryItemUpdate = null;
  try {
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

  if (autoPublishAllMarkets) {
    const existingMarketplaceIds = new Set(
      safeArray(offers)
        .map((offer) => String(offer?.marketplaceId || "").trim())
        .filter(Boolean)
    );

    const missingMarkets = MARKETPLACES.filter(
      (market) => !existingMarketplaceIds.has(market.marketplaceId)
    );

    for (const market of missingMarkets) {
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
      inventoryItemUpdate,
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

  const unpublishedOffers = offers.filter((offer) => {
    const status = String(offer?.status || "").trim().toUpperCase();
    return Boolean(offer?.offerId) && status && status !== "PUBLISHED";
  });

  const republishResults = [];

  for (const offer of unpublishedOffers) {
    try {
      const publishResult = await publishOffer({ offerId: offer.offerId });
      republishResults.push({
        offerId: offer.offerId,
        marketplaceId: offer?.marketplaceId || null,
        previousStatus: offer?.status || null,
        ok: true,
        publishResult,
      });
    } catch (error) {
      republishResults.push({
        offerId: offer.offerId,
        marketplaceId: offer?.marketplaceId || null,
        previousStatus: offer?.status || null,
        ok: false,
        error: errorToSerializable(error),
      });
    }
  }

  const offerResults = offers.map((offer) => {
    const responseRow =
      responses.find((r) => String(r?.offerId || "") === String(offer?.offerId || "")) ||
      null;

    const republishRow =
      republishResults.find(
        (r) => String(r?.offerId || "") === String(offer?.offerId || "")
      ) || null;

    const errors = safeArray(responseRow?.errors);
    const warnings = safeArray(responseRow?.warnings);

    const bulkOk = responseRow
      ? Number(responseRow.statusCode || 0) >= 200 &&
        Number(responseRow.statusCode || 0) < 300 &&
        errors.length === 0
      : !bulkUpdateError;

    const republishOk = republishRow ? republishRow.ok === true : true;

    return {
      offerId: offer?.offerId || null,
      marketplaceId: offer?.marketplaceId || null,
      format: offer?.format || null,
      status: offer?.status || null,
      statusCode: responseRow?.statusCode || null,
      ok: bulkOk && republishOk,
      errors,
      warnings,
      republished: Boolean(republishRow),
      republishOk,
      republishError: republishRow?.ok === false ? republishRow.error : null,
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
    inventoryItemUpdate,
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
    unpublishedOffersFound: unpublishedOffers.length,
    republishResults,
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
      locationKeys: EBAY_LOCATION_KEYS,
      marketplaces: MARKETPLACES,
      hasVerificationToken: Boolean(EBAY_VERIFICATION_TOKEN),
      notificationEndpoint: EBAY_NOTIFICATION_ENDPOINT,
      stateDir: EBAY_STATE_DIR,
      stateFile: EBAY_STATE_FILE,
      listingMapFile: EBAY_LISTING_MAP_FILE,
      orderSyncFile: EBAY_ORDER_SYNC_FILE,
      connection: sanitizeEbayConnectionForResponse(),
      listingMap: sanitizeListingMapForResponse(),
      orderSync: serializeOrderSyncState(),
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

app.get("/ebay/repair/category", async (req, res) => {
  try {
    const sku = String(req.query.sku || "").trim();
    const sourceLanguage = getSourceLanguage(req.query.sourceLanguage || "it");
    const marketplaces = String(req.query.marketplaces || "")
      .split(",")
      .map((x) => String(x || "").trim())
      .filter(Boolean);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const result = await publishSkuToMultipleEbayMarkets({
      sku,
      sourceLanguage,
      categoryMap: {},
      marketplaces: marketplaces.length
        ? marketplaces
        : MARKETPLACES.map((m) => m.marketplaceId),
    });

    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

async function runEbayCategoryRepairBatch({
  limit = 10,
  sourceLanguage = "it",
  reset = false,
}) {
  if (reset) {
    await resetEbayCategoryRepairState();
  }

  const safeLimit = Math.max(1, Math.min(100, Number(limit || 10)));

  let cursor = null;
  let pageIndex = 0;
  let skippedAlreadyProcessed = 0;
  let processed = [];
  let scanned = 0;
  let done = false;

  while (processed.length < safeLimit) {
    const batch = await getShopifyVariantsBatch({
      first: 100,
      after: cursor,
    });

    const nodes = safeArray(batch?.nodes);
    if (!nodes.length) {
      done = true;
      break;
    }

    for (const variant of nodes) {
      scanned += 1;
      const sku = String(variant?.sku || "").trim();
      if (!sku) continue;

      if (ebayCategoryRepairState.processedSkus.has(sku)) {
        skippedAlreadyProcessed += 1;
        continue;
      }

      try {
        const result = await repairEbayCategoryForSku({
          sku,
          sourceLanguage,
        });

        processed.push({
          sku,
          ok: Boolean(result?.ok),
          result,
        });

        ebayCategoryRepairState.processedSkus.add(sku);
        ebayCategoryRepairState.lastRunAt = nowIso();
        ebayCategoryRepairState.lastBatch = processed.map((x) => x.sku);
        await saveEbayCategoryRepairStateToDisk();
      } catch (error) {
        processed.push({
          sku,
          ok: false,
          error: errorToSerializable(error),
        });

        ebayCategoryRepairState.processedSkus.add(sku);
        ebayCategoryRepairState.lastRunAt = nowIso();
        ebayCategoryRepairState.lastBatch = processed.map((x) => x.sku);
        await saveEbayCategoryRepairStateToDisk();
      }

      if (processed.length >= safeLimit) {
        break;
      }
    }

    pageIndex += 1;
    cursor = batch?.endCursor || null;

    ebayCategoryRepairState.cursor = pageIndex;
    await saveEbayCategoryRepairStateToDisk();

    if (!batch?.hasNextPage) {
      done = true;
      break;
    }
  }

  return {
    ok: true,
    limit: safeLimit,
    processedCount: processed.length,
    skippedAlreadyProcessed,
    scanned,
    done,
    state: serializeCategoryRepairState(),
    results: processed,
  };
}

app.get("/ebay/repair/all", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 10);
    const sourceLanguage = getSourceLanguage(req.query.sourceLanguage || "it");
    const reset = String(req.query.reset || "false") === "true";

    const result = await runEbayCategoryRepairBatch({
      limit,
      sourceLanguage,
      reset,
    });

    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: errorToSerializable(error),
    });
  }
});

app.get("/ebay/repair/state", async (req, res) => {
  try {
    return res.json({
      ok: true,
      state: serializeCategoryRepairState(),
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

app.post("/ebay/repair/category", async (req, res) => {
  try {
    const sku = normalizeSku(req.body?.sku);
    const sourceLanguage = getSourceLanguage(req.body?.sourceLanguage || "it");
    const marketplaces = Array.isArray(req.body?.marketplaces)
      ? req.body.marketplaces
      : MARKETPLACES.map((m) => m.marketplaceId);

    if (!sku) {
      return res.status(400).json({ ok: false, error: "missing sku" });
    }

    const result = await repairCategoryForPublishedOffersBySku({
      sku,
      sourceLanguage,
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
  await loadEbayCategoryRepairStateFromDisk();
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
