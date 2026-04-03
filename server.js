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
} from "@aws-sdk/client-sqs";

const app = express();
const PORT = Number(process.env.PORT || 3000);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =========================
// CONFIG
// =========================

const EBAY_API_BASE = "https://api.ebay.com";
const EBAY_OAUTH_BASE = "https://api.ebay.com/identity/v1/oauth2/token";

const EBAY_SCOPES = [
  "https://api.ebay.com/oauth/api_scope/sell.inventory",
  "https://api.ebay.com/oauth/api_scope/sell.account",
  "https://api.ebay.com/oauth/api_scope/sell.fulfillment",
  "https://api.ebay.com/oauth/api_scope/commerce.identity.readonly",
];

const DATA_DIR = "/var/data/syncamzeby";
const EBAY_STATE_FILE = path.join(DATA_DIR, "ebay-connection.json");

// =========================
// MIDDLEWARE
// =========================

const RAW_BODY_PATHS = new Set([
  "/webhooks/products",
  "/webhooks/inventory",
]);

app.use((req, res, next) => {
  if (RAW_BODY_PATHS.has(req.path)) return next();
  return express.json({ limit: "4mb" })(req, res, next);
});

// =========================
// UTILS
// =========================

async function loadState() {
  try {
    const raw = await fs.readFile(EBAY_STATE_FILE, "utf8");
    const data = JSON.parse(raw);

    return {
      connected: Boolean(data.connected),
      connectedAt: data.connectedAt || null,
      environment: data.environment || "production",
      accessToken: data.accessToken || null,
      refreshToken: data.refreshToken || null,
      expiresAt:
        data.expiresAt ||
        (data.accessTokenExpiresAt ? new Date(data.accessTokenExpiresAt).getTime() : 0),
      accessTokenExpiresAt: data.accessTokenExpiresAt || null,
      refreshTokenExpiresAt: data.refreshTokenExpiresAt || null,
      tokenType: data.tokenType || null,
      scope: data.scope || "",
      userInfo: data.userInfo || null,
    };
  } catch {
    return {};
  }
}

async function getAccessToken() {
  const state = await loadState();

  if (!state.refreshToken) {
    throw new Error("No eBay refresh token found");
  }

  if (state.accessToken && state.expiresAt && Date.now() < state.expiresAt - 60000) {
    return state.accessToken;
  }

  const basic = Buffer.from(
    `${process.env.EBAY_CLIENT_ID}:${process.env.EBAY_CLIENT_SECRET}`
  ).toString("base64");

  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: state.refreshToken,
  });

  const savedScope = String(state.scope || "").trim();
  if (savedScope) {
    body.set("scope", savedScope);
  }

  try {
    const res = await axios.post(EBAY_OAUTH_BASE, body, {
      headers: {
        Authorization: `Basic ${basic}`,
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json",
      },
    });

    state.accessToken = res.data.access_token;
    state.expiresAt = Date.now() + Number(res.data.expires_in || 7200) * 1000;
    state.tokenType = res.data.token_type || "User Access Token";
    state.scope = res.data.scope || savedScope || EBAY_SCOPES.join(" ");

    await saveState(state);

    return state.accessToken;
  } catch (error) {
    console.log(
      "EBAY TOKEN REFRESH ERROR",
      JSON.stringify(error?.response?.data || error.message, null, 2)
    );
    throw error;
  }
}

async function saveState(data) {
  await fs.mkdir(DATA_DIR, { recursive: true });

  const payload = {
    connected: data.connected ?? true,
    connectedAt: data.connectedAt || null,
    environment: data.environment || "production",
    accessToken: data.accessToken || null,
    refreshToken: data.refreshToken || null,
    expiresAt: data.expiresAt || 0,
    accessTokenExpiresAt:
      data.accessTokenExpiresAt ||
      (data.expiresAt ? new Date(data.expiresAt).toISOString() : null),
    refreshTokenExpiresAt: data.refreshTokenExpiresAt || null,
    tokenType: data.tokenType || null,
    scope: data.scope || "",
    userInfo: data.userInfo || null,
  };

  await fs.writeFile(EBAY_STATE_FILE, JSON.stringify(payload, null, 2), "utf8");
}

// =========================
// EBAY API WRAPPER
// =========================

async function ebayRequest(method, url, data = null) {
  const token = await getAccessToken();

  try {
    const res = await axios({
      method,
      url: `${EBAY_API_BASE}${url}`,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      data,
    });

    return res.data;
  } catch (err) {
    console.error("❌ EBAY ERROR:", err.response?.data || err.message);
    return {
      error: err.response?.data || err.message,
    };
  }
}

// =========================
// TEMPLATE DESCRIZIONE PROFESSIONALE
// =========================

function buildDescription(product) {
  return `
  <div style="font-family:Arial; font-size:14px; line-height:1.5;">
    <h2 style="color:#111;">${product.title}</h2>

    <p>${product.description}</p>

    <ul>
      <li>✔ Prodotto originale</li>
      <li>✔ Spedizione veloce</li>
      <li>✔ Assistenza clienti dedicata</li>
    </ul>

    <hr>

    <p style="font-size:12px;color:#666;">
      Acquista in sicurezza. Spedizione rapida dall’Italia.
    </p>
  </div>
  `;
}

// =========================
// HEALTH
// =========================

app.get("/health", async (req, res) => {
  const state = await loadState();

  res.json({
    ok: true,
    ebayEnabled: true,
    hasRefreshToken: !!state.refreshToken,
  });
});

// =========================
// EBAY HEALTH
// =========================

app.get("/ebay/health", async (req, res) => {
  const state = await loadState();

  res.json({
    enabled: true,
    oauthConnected: !!state.refreshToken,
  });
});

// =========================
// GET OFFERS BY SKU
// =========================

app.get("/ebay/offers/by-sku", async (req, res) => {
  const { sku, marketplaceId } = req.query;

  const data = await ebayRequest(
    "GET",
    `/sell/inventory/v1/offer?sku=${sku}`
  );

  res.json(data);
});

//
// =========================
// SHOPIFY
// =========================
//

async function getShopifyAccessToken() {
  const response = await axios.post(
    `https://${process.env.SHOPIFY_SHOP_DOMAIN}/admin/oauth/access_token`,
    new URLSearchParams({
      client_id: process.env.SHOPIFY_CLIENT_ID,
      client_secret: process.env.SHOPIFY_CLIENT_SECRET,
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
    throw new Error("Missing Shopify access token");
  }

  return token;
}

async function shopifyGraphQL(query, variables = {}) {
  const token = await getShopifyAccessToken();

  const response = await axios.post(
    `https://${process.env.SHOPIFY_SHOP_DOMAIN}/admin/api/2026-01/graphql.json`,
    { query, variables },
    {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
      },
    }
  );

  if (response.data?.errors) {
    throw new Error(JSON.stringify(response.data.errors));
  }

  return response.data.data;
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

  const imageUrls = Array.isArray(variant.product?.images?.nodes)
    ? variant.product.images.nodes.map((x) => x?.url).filter(Boolean)
    : [];

  const descriptionHtml = String(variant.product?.descriptionHtml || "").trim();
  const descriptionText = descriptionHtml
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<\/?[^>]+(>|$)/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return {
    id: variant.id,
    sku: variant.sku,
    price: Number(variant.price || 0),
    inventoryQuantity: Number(variant.inventoryQuantity || 0),
    variantTitle: variant.title || "",
    barcode: variant.barcode || "",
    product: {
      id: variant.product?.id || null,
      title: variant.product?.title || "",
      descriptionHtml,
      descriptionText,
      vendor: variant.product?.vendor || "",
      productType: variant.product?.productType || "",
      onlineStoreUrl: variant.product?.onlineStoreUrl || "",
      categoryFullName: variant.product?.category?.fullName || "",
      imageUrls,
    },
  };
}

//
// =========================
// AMAZON
// =========================
//

const AMAZON_SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com";

async function getAmazonAccessToken() {
  const response = await axios.post(
    "https://api.amazon.com/auth/o2/token",
    new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: process.env.AMAZON_REFRESH_TOKEN,
      client_id: process.env.AMAZON_LWA_CLIENT_ID,
      client_secret: process.env.AMAZON_LWA_CLIENT_SECRET,
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

async function amazonPatch(pathname, accessToken, body, params = {}) {
  const response = await axios.patch(`${AMAZON_SP_API_BASE}${pathname}`, body, {
    params,
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

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
              marketplace_id: process.env.AMAZON_MARKETPLACE_ID,
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
      process.env.AMAZON_SELLER_ID
    )}/${encodeURIComponent(sku)}`;

    const response = await amazonPatch(pathname, token, body, {
      marketplaceIds: process.env.AMAZON_MARKETPLACE_ID,
      issueLocale: "it_IT",
    });

    console.log("AMAZON UPDATE SUCCESS", JSON.stringify(response, null, 2));
    return { ok: true, response };
  } catch (error) {
    console.log("AMAZON UPDATE ERROR", error?.response?.data || error.message);
    return { ok: false, error: error?.response?.data || error.message };
  }
}

//
// =========================
// EBAY MARKETPLACES / POLICIES
// =========================
//

const MARKETPLACES = [
  {
    marketplaceId: "EBAY_IT",
    locale: "it-IT",
    currency: "EUR",
    site: "ebay.it",
  },
  {
    marketplaceId: "EBAY_DE",
    locale: "de-DE",
    currency: "EUR",
    site: "ebay.de",
  },
  {
    marketplaceId: "EBAY_FR",
    locale: "fr-FR",
    currency: "EUR",
    site: "ebay.fr",
  },
  {
    marketplaceId: "EBAY_ES",
    locale: "es-ES",
    currency: "EUR",
    site: "ebay.es",
  },
  {
    marketplaceId: "EBAY_GB",
    locale: "en-GB",
    currency: "GBP",
    site: "ebay.co.uk",
  },
];

function getMarketplaceMeta(marketplaceId) {
  return (
    MARKETPLACES.find((m) => m.marketplaceId === String(marketplaceId || "").trim()) ||
    null
  );
}

function getMerchantLocationKey(marketplaceId) {
  const map = {
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

  return map[String(marketplaceId || "").trim()] || map.EBAY_IT;
}

async function getEbayPolicies(marketplaceId) {
  const token = await getAccessToken();

  const [payment, fulfillment, ret] = await Promise.all([
    axios.get(`${EBAY_API_BASE}/sell/account/v1/payment_policy`, {
      headers: { Authorization: `Bearer ${token}` },
      params: { marketplace_id: marketplaceId },
    }),
    axios.get(`${EBAY_API_BASE}/sell/account/v1/fulfillment_policy`, {
      headers: { Authorization: `Bearer ${token}` },
      params: { marketplace_id: marketplaceId },
    }),
    axios.get(`${EBAY_API_BASE}/sell/account/v1/return_policy`, {
      headers: { Authorization: `Bearer ${token}` },
      params: { marketplace_id: marketplaceId },
    }),
  ]);

  return {
    paymentPolicies: payment.data?.paymentPolicies || [],
    fulfillmentPolicies: fulfillment.data?.fulfillmentPolicies || [],
    returnPolicies: ret.data?.returnPolicies || [],
  };
}

function pickDefaultPolicy(items = [], idKey) {
  if (!Array.isArray(items) || !items.length) return null;

  const preferred =
    items.find((p) => p?.categoryTypes?.some((c) => c?.default === true)) ||
    items.find((p) => /default/i.test(String(p?.name || ""))) ||
    items[0];

  return preferred?.[idKey] || null;
}

async function getPolicyIdsForMarketplace(marketplaceId) {
  const policies = await getEbayPolicies(marketplaceId);

  return {
    paymentPolicyId: pickDefaultPolicy(policies.paymentPolicies, "paymentPolicyId"),
    fulfillmentPolicyId: pickDefaultPolicy(
      policies.fulfillmentPolicies,
      "fulfillmentPolicyId"
    ),
    returnPolicyId: pickDefaultPolicy(policies.returnPolicies, "returnPolicyId"),
  };
}

//
// =========================
// EBAY CATEGORY / INVENTORY / OFFER
// =========================
//

async function getOffersBySku({ sku, marketplaceId }) {
  const token = await getAccessToken();

  const response = await axios.get(`${EBAY_API_BASE}/sell/inventory/v1/offer`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    },
    params: {
      sku,
      limit: 200,
      offset: 0,
      ...(marketplaceId ? { marketplace_id: marketplaceId } : {}),
    },
  });

  return Array.isArray(response.data?.offers) ? response.data.offers : [];
}

function buildInventoryPayload(shopifyVariant, locale) {
  const payload = {
    availability: {
      shipToLocationAvailability: {
        quantity: Math.max(0, Number(shopifyVariant.inventoryQuantity || 0)),
      },
    },
    condition: "NEW",
    product: {
      title: String(shopifyVariant.product.title || "").slice(0, 80),
      description:
        shopifyVariant.product.descriptionText || shopifyVariant.product.title,
      imageUrls: shopifyVariant.product.imageUrls || [],
      aspects: {
        Brand: [shopifyVariant.product.vendor || "Generic"],
        MPN: [shopifyVariant.sku || "Non applicabile"],
        Type: [shopifyVariant.product.productType || "General"],
      },
    },
    locale,
  };

  const barcode = String(shopifyVariant.barcode || "").trim();
  if (barcode) {
    payload.product.ean = [barcode];
    payload.product.aspects.EAN = [barcode];
  }

  return payload;
}

async function upsertInventoryItem({ sku, payload, locale }) {
  const token = await getAccessToken();

  const response = await axios.put(
    `${EBAY_API_BASE}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`,
    payload,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        "Content-Language": locale,
      },
      validateStatus: (status) => status >= 200 && status < 300,
    }
  );

  return {
    status: response.status,
    data: response.data,
    headers: response.headers,
  };
}

function buildOfferPayload({
  sku,
  marketplaceId,
  price,
  quantity,
  currency,
  policyIds,
  descriptionHtml,
}) {
  return {
    sku,
    marketplaceId,
    format: "FIXED_PRICE",
    availableQuantity: Math.max(0, Number(quantity || 0)),
    categoryId: process.env.EBAY_DEFAULT_CATEGORY_ID || "348",
    merchantLocationKey: getMerchantLocationKey(marketplaceId),
    pricingSummary: {
      price: {
        value: String(Number(price || 0).toFixed(2)),
        currency,
      },
    },
    listingPolicies: {
      paymentPolicyId: policyIds.paymentPolicyId,
      fulfillmentPolicyId: policyIds.fulfillmentPolicyId,
      returnPolicyId: policyIds.returnPolicyId,
    },
    listingDescription: descriptionHtml,
    listingDuration: "GTC",
    includeCatalogProductDetails: true,
    hideBuyerDetails: false,
  };
}

async function createOffer({ payload, locale }) {
  const token = await getAccessToken();

  const response = await axios.post(
    `${EBAY_API_BASE}/sell/inventory/v1/offer`,
    payload,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        "Content-Language": locale,
      },
    }
  );

  return response.data;
}

async function updateOffer({ offerId, payload, locale }) {
  const token = await getAccessToken();

  const response = await axios.put(
    `${EBAY_API_BASE}/sell/inventory/v1/offer/${encodeURIComponent(offerId)}`,
    payload,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        "Content-Language": locale,
      },
      validateStatus: (status) => status >= 200 && status < 300,
    }
  );

  return {
    status: response.status,
    data: response.data,
    headers: response.headers,
  };
}

async function publishOffer(offerId) {
  const token = await getAccessToken();

  const response = await axios.post(
    `${EBAY_API_BASE}/sell/inventory/v1/offer/${encodeURIComponent(offerId)}/publish`,
    null,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
      validateStatus: (status) => status >= 200 && status < 300,
    }
  );

  return {
    status: response.status,
    data: response.data,
    headers: response.headers,
  };
}

async function syncSkuToOneMarketplace({ sku, marketplaceId }) {
  const market = getMarketplaceMeta(marketplaceId);
  if (!market) {
    return {
      ok: false,
      marketplaceId,
      error: "Unknown marketplace",
    };
  }

  const shopifyVariant = await getShopifyVariantBySku(sku);
  if (!shopifyVariant) {
    return {
      ok: false,
      marketplaceId,
      error: `Shopify SKU not found: ${sku}`,
    };
  }

  const descriptionHtml = buildDescription({
    title: shopifyVariant.product.title,
    description:
      shopifyVariant.product.descriptionText || shopifyVariant.product.title,
  });

  await upsertInventoryItem({
    sku,
    payload: buildInventoryPayload(shopifyVariant, market.locale),
    locale: market.locale,
  });

  const policyIds = await getPolicyIdsForMarketplace(marketplaceId);

  const offerPayload = buildOfferPayload({
    sku,
    marketplaceId,
    price: shopifyVariant.price,
    quantity: shopifyVariant.inventoryQuantity,
    currency: market.currency,
    policyIds,
    descriptionHtml,
  });

  const existingOffers = await getOffersBySku({ sku, marketplaceId });
  const existing = existingOffers.find((x) => x.marketplaceId === marketplaceId);

  let offerId = null;
  let mode = null;

  if (existing?.offerId) {
    await updateOffer({
      offerId: existing.offerId,
      payload: offerPayload,
      locale: market.locale,
    });
    offerId = existing.offerId;
    mode = "updated";
  } else {
    const created = await createOffer({
      payload: offerPayload,
      locale: market.locale,
    });
    offerId = created?.offerId || null;
    mode = "created";
  }

  let publishResult = null;
  let publishError = null;

  if (offerId) {
    try {
      publishResult = await publishOffer(offerId);
    } catch (error) {
      publishError = error?.response?.data || error.message;
      console.log(
        "EBAY PUBLISH ERROR",
        JSON.stringify(
          {
            sku,
            marketplaceId,
            offerId,
            error: publishError,
          },
          null,
          2
        )
      );
    }
  }

  return {
    ok: !publishError,
    marketplaceId,
    locale: market.locale,
    site: market.site,
    sku,
    offerId,
    mode,
    publishResult,
    publishError,
  };
}

//
// =========================
// EBAY MULTI PUBLISH / SYNC
// =========================
//

async function syncSkuToAllMarketplaces(sku) {
  const results = [];

  for (const market of MARKETPLACES) {
    try {
      const row = await syncSkuToOneMarketplace({
        sku,
        marketplaceId: market.marketplaceId,
      });
      results.push(row);
    } catch (error) {
      results.push({
        ok: false,
        marketplaceId: market.marketplaceId,
        sku,
        error: error?.response?.data || error.message,
      });
    }
  }

  return {
    ok: results.every((x) => x.ok),
    sku,
    results,
  };
}

app.post("/ebay/publish/multi", async (req, res) => {
  try {
    const sku = String(req.body?.sku || "").trim();
    const marketplaces = Array.isArray(req.body?.marketplaces)
      ? req.body.marketplaces
      : MARKETPLACES.map((m) => m.marketplaceId);

    if (!sku) {
      return res.status(400).json({
        ok: false,
        error: "missing sku",
      });
    }

    const results = [];

    for (const marketplaceId of marketplaces) {
      try {
        const row = await syncSkuToOneMarketplace({ sku, marketplaceId });
        results.push(row);
      } catch (error) {
        results.push({
          ok: false,
          marketplaceId,
          sku,
          error: error?.response?.data || error.message,
        });
      }
    }

    return res.json({
      ok: results.every((x) => x.ok),
      sku,
      results,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message,
    });
  }
});

app.post("/ebay/sync/sku", async (req, res) => {
  try {
    const sku = String(req.body?.sku || "").trim();

    if (!sku) {
      return res.status(400).json({
        ok: false,
        error: "missing sku",
      });
    }

    const shopifyVariant = await getShopifyVariantBySku(sku);
    if (!shopifyVariant) {
      return res.status(404).json({
        ok: false,
        error: `Shopify SKU not found: ${sku}`,
      });
    }

    const ebayResult = await syncSkuToAllMarketplaces(sku);

    return res.json({
      ok: ebayResult.ok,
      sku,
      syncedPrice: shopifyVariant.price,
      syncedQuantity: shopifyVariant.inventoryQuantity,
      ebay: ebayResult,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message,
    });
  }
});

//
// =========================
// SHOPIFY WEBHOOK HELPERS
// =========================
//

async function syncSkuEverywhere(sku) {
  const shopifyVariant = await getShopifyVariantBySku(sku);

  if (!shopifyVariant) {
    console.log("SYNC SKU EVERYWHERE - SHOPIFY SKU NOT FOUND", sku);
    return {
      ok: false,
      error: "Shopify SKU not found",
      sku,
    };
  }

  const amazonResult = await sendPriceQuantityToAmazon({
    sku,
    price: shopifyVariant.price,
    quantity: shopifyVariant.inventoryQuantity,
  });

  const ebayResult = await syncSkuToAllMarketplaces(sku);

  return {
    ok: amazonResult.ok && ebayResult.ok,
    sku,
    price: shopifyVariant.price,
    quantity: shopifyVariant.inventoryQuantity,
    amazonResult,
    ebayResult,
  };
}

//
// =========================
// SHOPIFY WEBHOOKS
// ROBUST VERSION:
// no fragile dependency on in-memory inventory map
// each webhook re-reads SKU directly from Shopify
// =========================
//

app.post("/webhooks/products", express.raw({ type: "*/*" }), async (req, res) => {
  try {
    const rawText = Buffer.isBuffer(req.body)
      ? req.body.toString("utf8")
      : String(req.body || "{}");

    const payload = JSON.parse(rawText || "{}");
    const variants = Array.isArray(payload?.variants) ? payload.variants : [];

    console.log(
      "SHOPIFY PRODUCTS WEBHOOK RECEIVED",
      JSON.stringify(
        {
          productId: payload?.id || null,
          variantsCount: variants.length,
        },
        null,
        2
      )
    );

    for (const variant of variants) {
      const sku = String(variant?.sku || "").trim();
      if (!sku) continue;

      try {
        const result = await syncSkuEverywhere(sku);

        console.log(
          "SHOPIFY PRODUCT WEBHOOK SYNC RESULT",
          JSON.stringify(result, null, 2)
        );
      } catch (error) {
        console.log(
          "SHOPIFY PRODUCT WEBHOOK SYNC ERROR",
          JSON.stringify(
            {
              sku,
              error: error?.response?.data || error.message,
            },
            null,
            2
          )
        );
      }
    }

    return res.sendStatus(200);
  } catch (error) {
    console.log(
      "PRODUCT WEBHOOK ERROR",
      JSON.stringify(error?.response?.data || error.message, null, 2)
    );
    return res.status(500).send("error");
  }
});

app.post("/webhooks/inventory", express.raw({ type: "*/*" }), async (req, res) => {
  try {
    const rawText = Buffer.isBuffer(req.body)
      ? req.body.toString("utf8")
      : String(req.body || "{}");

    const payload = JSON.parse(rawText || "{}");

    console.log(
      "SHOPIFY INVENTORY WEBHOOK RECEIVED",
      JSON.stringify(payload, null, 2)
    );

    const inventoryItemId = payload?.inventory_item_id || null;

    if (!inventoryItemId) {
      return res.sendStatus(200);
    }

    const query = `
      query FindInventoryItem($id: ID!) {
        inventoryItem(id: $id) {
          id
          sku
          variant {
            id
            sku
          }
        }
      }
    `;

    const gid = String(inventoryItemId).startsWith("gid://shopify/InventoryItem/")
      ? String(inventoryItemId)
      : `gid://shopify/InventoryItem/${inventoryItemId}`;

    let sku = "";

    try {
      const data = await shopifyGraphQL(query, { id: gid });
      sku =
        String(data?.inventoryItem?.sku || "").trim() ||
        String(data?.inventoryItem?.variant?.sku || "").trim();
    } catch (error) {
      console.log(
        "SHOPIFY INVENTORY WEBHOOK LOOKUP ERROR",
        JSON.stringify(error?.response?.data || error.message, null, 2)
      );
    }

    if (!sku) {
      console.log(
        "SHOPIFY INVENTORY WEBHOOK - SKU NOT FOUND FOR INVENTORY ITEM",
        gid
      );
      return res.sendStatus(200);
    }

    try {
      const result = await syncSkuEverywhere(sku);

      console.log(
        "SHOPIFY INVENTORY WEBHOOK SYNC RESULT",
        JSON.stringify(result, null, 2)
      );
    } catch (error) {
      console.log(
        "SHOPIFY INVENTORY WEBHOOK SYNC ERROR",
        JSON.stringify(
          {
            sku,
            error: error?.response?.data || error.message,
          },
          null,
          2
        )
      );
    }

    return res.sendStatus(200);
  } catch (error) {
    console.log(
      "INVENTORY WEBHOOK ERROR",
      JSON.stringify(error?.response?.data || error.message, null, 2)
    );
    return res.status(500).send("error");
  }
});

//
// =========================
// MANUAL TEST ROUTES
// =========================
//

app.get("/shopify/sku", async (req, res) => {
  try {
    const sku = String(req.query?.sku || "").trim();

    if (!sku) {
      return res.status(400).json({
        ok: false,
        error: "missing sku",
      });
    }

    const product = await getShopifyVariantBySku(sku);

    if (!product) {
      return res.status(404).json({
        ok: false,
        error: `Shopify SKU not found: ${sku}`,
      });
    }

    return res.json({
      ok: true,
      sku,
      product,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message,
    });
  }
});

app.post("/sync/all/sku", async (req, res) => {
  try {
    const sku = String(req.body?.sku || "").trim();

    if (!sku) {
      return res.status(400).json({
        ok: false,
        error: "missing sku",
      });
    }

    const result = await syncSkuEverywhere(sku);

    return res.json(result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message,
    });
  }
});

//
// =========================
// AMAZON SQS ORDER POLLING
// optional support retained
// =========================
//

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

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

app.get("/amazon/notifications/pull", async (req, res) => {
  try {
    const queueUrl = process.env.AMAZON_SQS_QUEUE_URL;

    if (!queueUrl) {
      return res.status(400).json({
        ok: false,
        error: "missing AMAZON_SQS_QUEUE_URL",
      });
    }

    const receiveResponse = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
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

      results.push({
        orderId: orderId || null,
        body,
      });

      if (receiptHandle) {
        await sqsClient.send(
          new DeleteMessageCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: receiptHandle,
          })
        );
      }
    }

    return res.json({
      ok: true,
      received: messages.length,
      results,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      error: error?.response?.data || error.message,
    });
  }
});

//
// =========================
// STARTUP
// =========================
//

async function start() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });

    app.listen(PORT, () => {
      console.log(`SyncAmzEby listening on port ${PORT}`);
      console.log("Server ready");
      console.log(`Health: http://localhost:${PORT}/health`);
    });
  } catch (error) {
    console.error("STARTUP ERROR:", error?.response?.data || error.message);
    process.exit(1);
  }
}

start();
