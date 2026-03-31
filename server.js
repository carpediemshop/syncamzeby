'use strict';

require('dotenv').config();

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

/* ============================================================================
 * CONFIG
 * ========================================================================== */

const APP_BASE_URL = (process.env.APP_BASE_URL || '').replace(/\/+$/, '');
const EBAY_ENV = (process.env.EBAY_ENV || 'production').toLowerCase();

const EBAY_CLIENT_ID = process.env.EBAY_CLIENT_ID || '';
const EBAY_CLIENT_SECRET = process.env.EBAY_CLIENT_SECRET || '';
const EBAY_RU_NAME = process.env.EBAY_RU_NAME || ''; // es: tuoRuNameProduzione
const EBAY_SCOPES = (process.env.EBAY_SCOPES || [
  'https://api.ebay.com/oauth/api_scope/sell.inventory',
  'https://api.ebay.com/oauth/api_scope/sell.account',
  'https://api.ebay.com/oauth/api_scope/sell.fulfillment'
].join(' ')).trim();

const EBAY_MARKETPLACE_ID = process.env.EBAY_MARKETPLACE_ID || 'EBAY_IT';
const EBAY_MERCHANT_LOCATION_KEY = process.env.EBAY_MERCHANT_LOCATION_KEY || 'Default-EBAY_IT';

const EBAY_PAYMENT_POLICY_ID = process.env.EBAY_PAYMENT_POLICY_ID || 'default';
const EBAY_RETURN_POLICY_ID = process.env.EBAY_RETURN_POLICY_ID || 'default';
const EBAY_FULFILLMENT_POLICY_ID = process.env.EBAY_FULFILLMENT_POLICY_ID || 'default';

const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN || '';
const SHOPIFY_ADMIN_ACCESS_TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN || '';

const EBAY_STATE_DIR = process.env.EBAY_STATE_DIR || path.join(__dirname, 'data');
const EBAY_STATE_FILE = path.join(EBAY_STATE_DIR, 'ebay-auth-state.json');

const ebayBase = EBAY_ENV === 'sandbox' ? 'https://api.sandbox.ebay.com' : 'https://api.ebay.com';
const ebayAuthBase = EBAY_ENV === 'sandbox' ? 'https://auth.sandbox.ebay.com' : 'https://auth.ebay.com';

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

/* ============================================================================
 * SIMPLE LOGGER
 * ========================================================================== */

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function logError(...args) {
  console.error(new Date().toISOString(), ...args);
}

/* ============================================================================
 * FILE STATE
 * ========================================================================== */

let memoryCsrfStates = new Map();

async function ensureStateDir() {
  await fsp.mkdir(EBAY_STATE_DIR, { recursive: true });
}

async function readJsonSafe(filePath, fallback = null) {
  try {
    const raw = await fsp.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (err) {
    if (err.code === 'ENOENT') return fallback;
    throw err;
  }
}

async function writeJsonAtomic(filePath, data) {
  await ensureStateDir();
  const tempPath = `${filePath}.tmp`;
  await fsp.writeFile(tempPath, JSON.stringify(data, null, 2), 'utf8');
  await fsp.rename(tempPath, filePath);
}

async function loadEbayState() {
  const state = await readJsonSafe(EBAY_STATE_FILE, {
    ebayAuth: null,
    meta: {
      updatedAt: null
    }
  });

  return state;
}

async function saveEbayState(nextState) {
  nextState.meta = nextState.meta || {};
  nextState.meta.updatedAt = new Date().toISOString();
  await writeJsonAtomic(EBAY_STATE_FILE, nextState);
}

async function clearEbayState() {
  const blank = {
    ebayAuth: null,
    meta: {
      updatedAt: new Date().toISOString()
    }
  };
  await saveEbayState(blank);
}

/* ============================================================================
 * HELPERS
 * ========================================================================== */

function requireEnv(name, value) {
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function stripTags(html) {
  return String(html || '')
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, '')
    .replace(/<\/?(?!br\b)[^>]+>/gi, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeHtmlForEbay(html) {
  const cleaned = String(html || '')
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, '')
    .trim();

  if (!cleaned) return '';
  return cleaned;
}

function nowTs() {
  return Date.now();
}

function buildBasicAuth(clientId, clientSecret) {
  return Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
}

function buildAppUrl(p) {
  if (!APP_BASE_URL) return p;
  return `${APP_BASE_URL}${p}`;
}

function getEbayAuthUrl(returnTo = '/ebay/publisher') {
  requireEnv('APP_BASE_URL', APP_BASE_URL);
  requireEnv('EBAY_CLIENT_ID', EBAY_CLIENT_ID);
  requireEnv('EBAY_RU_NAME', EBAY_RU_NAME);

  const stateToken = crypto.randomBytes(24).toString('hex');
  memoryCsrfStates.set(stateToken, {
    returnTo,
    createdAt: nowTs()
  });

  const params = new URLSearchParams({
    client_id: EBAY_CLIENT_ID,
    response_type: 'code',
    redirect_uri: EBAY_RU_NAME,
    scope: EBAY_SCOPES,
    state: stateToken,
    prompt: 'login'
  });

  return `${ebayAuthBase}/oauth2/authorize?${params.toString()}`;
}

function cleanupOldCsrfStates() {
  const cutoff = nowTs() - (30 * 60 * 1000);
  for (const [key, value] of memoryCsrfStates.entries()) {
    if (!value || value.createdAt < cutoff) {
      memoryCsrfStates.delete(key);
    }
  }
}

function getRemainingSeconds(expiresAt) {
  if (!expiresAt) return 0;
  return Math.max(0, Math.floor((new Date(expiresAt).getTime() - nowTs()) / 1000));
}

/* ============================================================================
 * EBAY OAUTH
 * ========================================================================== */

async function exchangeCodeForTokens(code) {
  requireEnv('EBAY_CLIENT_ID', EBAY_CLIENT_ID);
  requireEnv('EBAY_CLIENT_SECRET', EBAY_CLIENT_SECRET);
  requireEnv('EBAY_RU_NAME', EBAY_RU_NAME);

  const form = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: EBAY_RU_NAME
  });

  const response = await axios.post(`${ebayBase}/identity/v1/oauth2/token`, form.toString(), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': `Basic ${buildBasicAuth(EBAY_CLIENT_ID, EBAY_CLIENT_SECRET)}`
    },
    timeout: 30000
  });

  return response.data;
}

async function refreshUserToken(refreshToken) {
  requireEnv('EBAY_CLIENT_ID', EBAY_CLIENT_ID);
  requireEnv('EBAY_CLIENT_SECRET', EBAY_CLIENT_SECRET);

  const form = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: refreshToken,
    scope: EBAY_SCOPES
  });

  const response = await axios.post(`${ebayBase}/identity/v1/oauth2/token`, form.toString(), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': `Basic ${buildBasicAuth(EBAY_CLIENT_ID, EBAY_CLIENT_SECRET)}`
    },
    timeout: 30000
  });

  return response.data;
}

async function saveOAuthTokenPayload(tokenPayload) {
  const state = await loadEbayState();

  const expiresIn = Number(tokenPayload.expires_in || 0);
  const refreshExpiresIn = Number(tokenPayload.refresh_token_expires_in || 0);

  state.ebayAuth = {
    environment: EBAY_ENV,
    tokenType: tokenPayload.token_type || 'User Access Token',
    scope: tokenPayload.scope || EBAY_SCOPES,
    accessToken: tokenPayload.access_token,
    refreshToken: tokenPayload.refresh_token || (state.ebayAuth?.refreshToken || null),
    accessTokenExpiresAt: new Date(nowTs() + expiresIn * 1000).toISOString(),
    refreshTokenExpiresAt: refreshExpiresIn
      ? new Date(nowTs() + refreshExpiresIn * 1000).toISOString()
      : (state.ebayAuth?.refreshTokenExpiresAt || null),
    obtainedAt: new Date().toISOString()
  };

  await saveEbayState(state);
  return state.ebayAuth;
}

async function getValidEbayUserToken() {
  const state = await loadEbayState();
  const auth = state.ebayAuth;

  if (!auth || !auth.refreshToken) {
    return {
      connected: false,
      reason: 'No refresh token saved'
    };
  }

  const expiresAtMs = auth.accessTokenExpiresAt ? new Date(auth.accessTokenExpiresAt).getTime() : 0;
  const needsRefresh = !auth.accessToken || !expiresAtMs || (expiresAtMs - nowTs()) < (5 * 60 * 1000);

  if (!needsRefresh) {
    return {
      connected: true,
      accessToken: auth.accessToken,
      auth
    };
  }

  try {
    log('[eBay] Refreshing access token...');
    const refreshed = await refreshUserToken(auth.refreshToken);
    const saved = await saveOAuthTokenPayload(refreshed);

    return {
      connected: true,
      accessToken: saved.accessToken,
      auth: saved
    };
  } catch (err) {
    logError('[eBay] Refresh token failed:', err.response?.data || err.message);
    return {
      connected: false,
      reason: 'Refresh token failed',
      error: err.response?.data || err.message
    };
  }
}

async function ebayRequest(method, urlPath, data = undefined, extraHeaders = {}) {
  const tokenState = await getValidEbayUserToken();
  if (!tokenState.connected) {
    const error = new Error('eBay account not connected');
    error.statusCode = 401;
    error.details = tokenState;
    throw error;
  }

  const response = await axios({
    method,
    url: `${ebayBase}${urlPath}`,
    data,
    timeout: 45000,
    headers: {
      'Authorization': `Bearer ${tokenState.accessToken}`,
      'Content-Type': 'application/json',
      'Content-Language': 'it-IT',
      'Accept': 'application/json',
      ...extraHeaders
    }
  });

  return response.data;
}

/* ============================================================================
 * SHOPIFY
 * ========================================================================== */

async function shopifyGraphQL(query, variables = {}) {
  requireEnv('SHOPIFY_STORE_DOMAIN', SHOPIFY_STORE_DOMAIN);
  requireEnv('SHOPIFY_ADMIN_ACCESS_TOKEN', SHOPIFY_ADMIN_ACCESS_TOKEN);

  const endpoint = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/2025-01/graphql.json`;

  const response = await axios.post(endpoint, { query, variables }, {
    headers: {
      'X-Shopify-Access-Token': SHOPIFY_ADMIN_ACCESS_TOKEN,
      'Content-Type': 'application/json'
    },
    timeout: 45000
  });

  if (response.data.errors?.length) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(response.data.errors)}`);
  }

  if (response.data.data?.productVariants?.edges == null) {
    throw new Error(`Unexpected Shopify response: ${JSON.stringify(response.data)}`);
  }

  return response.data.data;
}

async function fetchShopifyProductBySku(sku) {
  const query = `
    query ProductBySku($query: String!) {
      productVariants(first: 1, query: $query) {
        edges {
          node {
            id
            sku
            price
            inventoryQuantity
            barcode
            product {
              id
              title
              descriptionHtml
              vendor
              productType
              onlineStoreUrl
              featuredImage {
                url
              }
              images(first: 10) {
                edges {
                  node {
                    url
                  }
                }
              }
            }
          }
        }
      }
    }
  `;

  const data = await shopifyGraphQL(query, {
    query: `sku:${sku}`
  });

  const edge = data.productVariants.edges[0];
  if (!edge) return null;

  const variant = edge.node;
  const product = variant.product;

  return {
    sku: variant.sku,
    price: Number(variant.price || 0),
    quantity: Number(variant.inventoryQuantity || 0),
    ean: variant.barcode || '',
    title: product.title || '',
    descriptionHtml: product.descriptionHtml || '',
    descriptionText: stripTags(product.descriptionHtml || ''),
    vendor: product.vendor || '',
    productType: product.productType || '',
    onlineStoreUrl: product.onlineStoreUrl || '',
    imageUrls: [
      ...(product.featuredImage?.url ? [product.featuredImage.url] : []),
      ...product.images.edges.map(e => e.node.url).filter(Boolean)
    ].filter(Boolean)
  };
}

/* ============================================================================
 * EBAY INVENTORY / OFFER HELPERS
 * ========================================================================== */

async function getOffersBySku(sku) {
  const params = new URLSearchParams({
    sku,
    marketplace_id: EBAY_MARKETPLACE_ID,
    format: 'FIXED_PRICE'
  });

  try {
    const result = await ebayRequest('get', `/sell/inventory/v1/offer?${params.toString()}`);
    return Array.isArray(result.offers) ? result.offers : [];
  } catch (err) {
    if (err.response?.status === 404) return [];
    throw err;
  }
}

async function upsertInventoryItemFromShopifyProduct(product) {
  const payload = {
    availability: {
      shipToLocationAvailability: {
        quantity: Math.max(0, Number(product.quantity || 0))
      }
    },
    condition: 'NEW',
    product: {
      title: String(product.title || '').slice(0, 80),
      description: normalizeHtmlForEbay(product.descriptionHtml || product.descriptionText || ''),
      imageUrls: product.imageUrls?.slice(0, 24) || [],
      aspects: {},
      ...(product.vendor ? { brand: product.vendor } : {})
    }
  };

  if (product.ean) {
    payload.product.ean = [String(product.ean)];
  }

  if (product.vendor) {
    payload.product.aspects.Brand = [product.vendor];
  }

  await ebayRequest('put', `/sell/inventory/v1/inventory_item/${encodeURIComponent(product.sku)}`, payload);
}

async function createOfferForSku({ sku, categoryId, price, quantity, listingDescription }) {
  const payload = {
    sku,
    marketplaceId: EBAY_MARKETPLACE_ID,
    format: 'FIXED_PRICE',
    availableQuantity: Math.max(0, Number(quantity || 0)),
    categoryId: String(categoryId),
    merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
    listingDescription: listingDescription || '',
    pricingSummary: {
      price: {
        value: Number(price).toFixed(2),
        currency: 'EUR'
      }
    }
  };

  if (EBAY_PAYMENT_POLICY_ID && EBAY_PAYMENT_POLICY_ID !== 'default') {
    payload.paymentPolicyId = EBAY_PAYMENT_POLICY_ID;
  }
  if (EBAY_RETURN_POLICY_ID && EBAY_RETURN_POLICY_ID !== 'default') {
    payload.returnPolicyId = EBAY_RETURN_POLICY_ID;
  }
  if (EBAY_FULFILLMENT_POLICY_ID && EBAY_FULFILLMENT_POLICY_ID !== 'default') {
    payload.fulfillmentPolicyId = EBAY_FULFILLMENT_POLICY_ID;
  }

  const result = await ebayRequest('post', '/sell/inventory/v1/offer', payload);
  return result.offerId;
}

async function publishOfferById(offerId) {
  return ebayRequest('post', `/sell/inventory/v1/offer/${encodeURIComponent(offerId)}/publish`, {});
}

/* ============================================================================
 * ROUTES
 * ========================================================================== */

app.get('/health', async (req, res) => {
  const state = await loadEbayState();
  const auth = state.ebayAuth;

  res.json({
    ok: true,
    appBaseUrl: APP_BASE_URL,
    ebayEnv: EBAY_ENV,
    ebayConnected: Boolean(auth?.refreshToken),
    ebayAccessTokenExpiresInSec: auth?.accessTokenExpiresAt ? getRemainingSeconds(auth.accessTokenExpiresAt) : null,
    ebayStateFile: EBAY_STATE_FILE,
    merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
    marketplaceId: EBAY_MARKETPLACE_ID,
    now: new Date().toISOString()
  });
});

app.get('/ebay/connect', (req, res) => {
  try {
    cleanupOldCsrfStates();
    const returnTo = String(req.query.returnTo || '/ebay/publisher');
    const authUrl = getEbayAuthUrl(returnTo);
    return res.redirect(authUrl);
  } catch (err) {
    logError('[eBay connect]', err.message);
    return res.status(500).send(`
      <h1>Errore connessione eBay</h1>
      <pre>${escapeHtml(err.message)}</pre>
    `);
  }
});

app.get('/ebay/callback', async (req, res) => {
  try {
    const code = req.query.code;
    const stateToken = req.query.state;

    if (!code) {
      return res.status(400).send('Missing eBay authorization code');
    }

    const csrf = memoryCsrfStates.get(String(stateToken || ''));
    memoryCsrfStates.delete(String(stateToken || ''));

    const tokenPayload = await exchangeCodeForTokens(String(code));
    await saveOAuthTokenPayload(tokenPayload);

    const returnTo = csrf?.returnTo || '/ebay/publisher';
    return res.redirect(`${returnTo}?connected=1`);
  } catch (err) {
    logError('[eBay callback]', err.response?.data || err.message);
    return res.status(500).send(`
      <h1>Errore callback eBay</h1>
      <pre>${escapeHtml(JSON.stringify(err.response?.data || err.message, null, 2))}</pre>
      <p><a href="/ebay/publisher">Torna al publisher</a></p>
    `);
  }
});

app.get('/ebay/status', async (req, res) => {
  try {
    const state = await loadEbayState();
    const auth = state.ebayAuth;

    if (!auth || !auth.refreshToken) {
      return res.json({
        connected: false,
        environment: EBAY_ENV,
        marketplaceId: EBAY_MARKETPLACE_ID,
        merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY
      });
    }

    const tokenInfo = await getValidEbayUserToken();

    return res.json({
      connected: tokenInfo.connected,
      environment: EBAY_ENV,
      marketplaceId: EBAY_MARKETPLACE_ID,
      merchantLocationKey: EBAY_MERCHANT_LOCATION_KEY,
      scope: auth.scope,
      tokenType: auth.tokenType,
      accessTokenExpiresAt: auth.accessTokenExpiresAt,
      accessTokenExpiresInSec: getRemainingSeconds(auth.accessTokenExpiresAt),
      refreshTokenExpiresAt: auth.refreshTokenExpiresAt,
      reason: tokenInfo.reason || null
    });
  } catch (err) {
    logError('[eBay status]', err.message);
    return res.status(500).json({
      connected: false,
      error: err.message
    });
  }
});

app.post('/ebay/disconnect', async (req, res) => {
  try {
    await clearEbayState();
    return res.json({ ok: true, connected: false });
  } catch (err) {
    logError('[eBay disconnect]', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/ebay/product', async (req, res) => {
  try {
    const sku = String(req.query.sku || '').trim();
    if (!sku) {
      return res.status(400).json({ ok: false, error: 'Missing sku' });
    }

    const product = await fetchShopifyProductBySku(sku);
    if (!product) {
      return res.status(404).json({ ok: false, error: 'Product not found in Shopify' });
    }

    return res.json({
      ok: true,
      product
    });
  } catch (err) {
    logError('[eBay product]', err.response?.data || err.message);
    return res.status(500).json({
      ok: false,
      error: err.response?.data || err.message
    });
  }
});

app.post('/ebay/publish', async (req, res) => {
  try {
    const sku = String(req.body.sku || '').trim();
    const categoryId = String(req.body.categoryId || '').trim();

    if (!sku) {
      return res.status(400).json({ ok: false, error: 'Missing sku' });
    }
    if (!categoryId) {
      return res.status(400).json({ ok: false, error: 'Missing categoryId' });
    }

    const product = await fetchShopifyProductBySku(sku);
    if (!product) {
      return res.status(404).json({ ok: false, error: 'Product not found in Shopify' });
    }

    const listingDescription = normalizeHtmlForEbay(
      req.body.listingDescription || product.descriptionHtml || product.descriptionText || ''
    );

    await upsertInventoryItemFromShopifyProduct(product);

    const existingOffers = await getOffersBySku(sku);
    let offerId = existingOffers[0]?.offerId || null;

    if (!offerId) {
      offerId = await createOfferForSku({
        sku: product.sku,
        categoryId,
        price: product.price,
        quantity: product.quantity,
        listingDescription
      });
    }

    const publishResult = await publishOfferById(offerId);

    return res.json({
      ok: true,
      message: 'Prodotto pubblicato su eBay',
      sku: product.sku,
      offerId,
      publishResult
    });
  } catch (err) {
    const ebayError = err.response?.data || err.details || err.message;
    const statusCode = err.statusCode || err.response?.status || 500;

    logError('[eBay publish]', ebayError);

    return res.status(statusCode).json({
      ok: false,
      error: ebayError
    });
  }
});

/* ============================================================================
 * UI: /ebay/publisher
 * ========================================================================== */

app.get('/ebay/publisher', (req, res) => {
  const defaultSku = escapeHtml(String(req.query.sku || '17-GELO-ZHRW'));

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>SyncAmzEby • eBay Publisher</title>
  <style>
    :root{
      --bg:#0b1220;
      --panel:#121a2b;
      --panel-2:#172136;
      --text:#eaf0ff;
      --muted:#9fb0d0;
      --ok:#11c26d;
      --warn:#ffb020;
      --bad:#ff5d5d;
      --line:#26324d;
      --primary:#4c8dff;
      --primary-2:#76a6ff;
      --shadow:0 10px 30px rgba(0,0,0,.28);
      --radius:18px;
    }

    *{box-sizing:border-box}
    body{
      margin:0;
      font-family:Inter,Segoe UI,Arial,sans-serif;
      background:
        radial-gradient(circle at top right, rgba(76,141,255,.18), transparent 26%),
        radial-gradient(circle at top left, rgba(17,194,109,.10), transparent 20%),
        linear-gradient(180deg, #09111d 0%, #0b1220 100%);
      color:var(--text);
      min-height:100vh;
    }

    .wrap{
      max-width:1180px;
      margin:0 auto;
      padding:28px 20px 80px;
    }

    .hero{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:20px;
      background:linear-gradient(135deg, rgba(76,141,255,.18), rgba(17,194,109,.08));
      border:1px solid rgba(255,255,255,.08);
      border-radius:24px;
      padding:24px;
      box-shadow:var(--shadow);
      margin-bottom:22px;
    }

    .hero h1{
      margin:0 0 8px;
      font-size:32px;
      line-height:1.1;
      letter-spacing:.2px;
    }

    .hero p{
      margin:0;
      color:var(--muted);
      font-size:15px;
    }

    .badge{
      display:inline-flex;
      align-items:center;
      gap:8px;
      font-size:13px;
      padding:10px 14px;
      border-radius:999px;
      border:1px solid rgba(255,255,255,.1);
      background:rgba(255,255,255,.04);
      white-space:nowrap;
    }

    .dot{
      width:10px;
      height:10px;
      border-radius:999px;
      display:inline-block;
      background:#888;
      box-shadow:0 0 0 4px rgba(255,255,255,.03);
    }
    .dot.ok{background:var(--ok)}
    .dot.bad{background:var(--bad)}
    .dot.warn{background:var(--warn)}

    .grid{
      display:grid;
      grid-template-columns:1.2fr .8fr;
      gap:22px;
    }

    .card{
      background:linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.01));
      border:1px solid rgba(255,255,255,.08);
      border-radius:var(--radius);
      box-shadow:var(--shadow);
      overflow:hidden;
    }

    .card-head{
      padding:18px 20px;
      border-bottom:1px solid var(--line);
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
    }

    .card-head h2{
      margin:0;
      font-size:18px;
    }

    .card-body{
      padding:20px;
    }

    .row{
      display:grid;
      grid-template-columns:160px 1fr;
      gap:12px;
      align-items:center;
      margin-bottom:14px;
    }

    .row label{
      color:var(--muted);
      font-size:14px;
      font-weight:600;
    }

    input[type="text"], textarea{
      width:100%;
      border:1px solid #2b3b5d;
      background:#0f1727;
      color:var(--text);
      border-radius:12px;
      padding:12px 14px;
      outline:none;
      font-size:14px;
    }

    input[type="text"]:focus, textarea:focus{
      border-color:var(--primary);
      box-shadow:0 0 0 4px rgba(76,141,255,.16);
    }

    textarea{
      min-height:130px;
      resize:vertical;
      font-family:inherit;
    }

    .btns{
      display:flex;
      flex-wrap:wrap;
      gap:12px;
    }

    button{
      border:none;
      border-radius:12px;
      padding:12px 16px;
      font-size:14px;
      font-weight:700;
      cursor:pointer;
      transition:.18s ease;
    }

    .btn-primary{
      background:linear-gradient(180deg, var(--primary), var(--primary-2));
      color:white;
    }

    .btn-primary:hover{transform:translateY(-1px)}

    .btn-secondary{
      background:#1a2740;
      color:#dbe6ff;
      border:1px solid #30415f;
    }

    .btn-danger{
      background:#402028;
      color:#ffd6dc;
      border:1px solid #6a3040;
    }

    .info{
      display:grid;
      grid-template-columns:repeat(2,1fr);
      gap:12px;
      margin-bottom:18px;
    }

    .kpi{
      background:#0f1727;
      border:1px solid #25324d;
      border-radius:14px;
      padding:14px;
    }

    .kpi .k{
      color:var(--muted);
      font-size:12px;
      margin-bottom:6px;
      text-transform:uppercase;
      letter-spacing:.08em;
    }

    .kpi .v{
      font-size:15px;
      font-weight:700;
      word-break:break-word;
    }

    .log{
      background:#08101d;
      border:1px solid #22314c;
      border-radius:14px;
      min-height:250px;
      padding:14px;
      overflow:auto;
      font-family:ui-monospace,SFMono-Regular,Consolas,monospace;
      font-size:13px;
      color:#cfe0ff;
      line-height:1.5;
    }

    .status-line{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      padding:12px 14px;
      border:1px solid #2a3a58;
      background:#0f1727;
      border-radius:14px;
      margin-bottom:16px;
    }

    .muted{color:var(--muted)}
    .small{font-size:13px}

    .product-box{
      display:grid;
      grid-template-columns:100px 1fr;
      gap:14px;
      padding:14px;
      border:1px solid #2a3a58;
      background:#0f1727;
      border-radius:14px;
      margin-top:16px;
    }

    .product-box img{
      width:100px;
      height:100px;
      object-fit:cover;
      border-radius:12px;
      border:1px solid #22314c;
      background:white;
    }

    .hidden{display:none !important}

    .overlay{
      position:fixed;
      inset:0;
      background:rgba(5,10,18,.72);
      backdrop-filter:blur(4px);
      display:none;
      align-items:center;
      justify-content:center;
      z-index:9999;
    }

    .overlay.show{display:flex}

    .overlay-card{
      width:min(92vw,460px);
      background:linear-gradient(180deg,#111a2c,#0d1524);
      border:1px solid rgba(255,255,255,.10);
      border-radius:20px;
      box-shadow:0 30px 80px rgba(0,0,0,.45);
      padding:26px;
      text-align:center;
    }

    .spinner{
      width:64px;
      height:64px;
      border-radius:999px;
      border:5px solid rgba(255,255,255,.15);
      border-top-color:var(--primary);
      margin:0 auto 18px;
      animation:spin 1s linear infinite;
    }

    @keyframes spin{
      to{transform:rotate(360deg)}
    }

    .overlay-title{
      font-size:20px;
      font-weight:800;
      margin-bottom:8px;
    }

    .overlay-sub{
      color:var(--muted);
      font-size:14px;
      line-height:1.5;
    }

    @media (max-width: 980px){
      .grid{grid-template-columns:1fr}
      .info{grid-template-columns:1fr}
      .row{grid-template-columns:1fr}
      .hero{flex-direction:column;align-items:flex-start}
      .product-box{grid-template-columns:1fr}
      .product-box img{width:120px;height:120px}
    }
  </style>
</head>
<body>
  <div class="overlay" id="overlay">
    <div class="overlay-card">
      <div class="spinner"></div>
      <div class="overlay-title" id="overlayTitle">Il sistema sta lavorando</div>
      <div class="overlay-sub" id="overlaySub">Attendi qualche secondo, sto elaborando l'operazione in corso.</div>
    </div>
  </div>

  <div class="wrap">
    <div class="hero">
      <div>
        <h1>SyncAmzEby • eBay Publisher</h1>
        <p>Connessione eBay persistente, caricamento prodotto Shopify e pubblicazione controllata da interfaccia.</p>
      </div>
      <div class="badge" id="topStatusBadge">
        <span class="dot warn" id="topStatusDot"></span>
        <span id="topStatusText">Verifica connessione eBay...</span>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="card-head">
          <h2>Publisher</h2>
          <div class="btns">
            <button class="btn-secondary" id="btnLoadStatus">Aggiorna stato</button>
            <button class="btn-primary" id="btnConnect">Connetti a eBay</button>
            <button class="btn-danger" id="btnDisconnect">Disconnetti</button>
          </div>
        </div>
        <div class="card-body">
          <div class="status-line">
            <div>
              <div style="display:flex;align-items:center;gap:10px;">
                <span class="dot warn" id="connDot"></span>
                <strong id="connText">Stato connessione: verifica in corso...</strong>
              </div>
              <div class="muted small" id="connMeta">Caricamento dati...</div>
            </div>
          </div>

          <div class="row">
            <label for="sku">SKU Shopify</label>
            <input id="sku" type="text" value="${defaultSku}" placeholder="Es. 17-GELO-ZHRW" />
          </div>

          <div class="row">
            <label for="categoryId">Categoria eBay</label>
            <input id="categoryId" type="text" placeholder="Inserisci categoryId eBay" />
          </div>

          <div class="row">
            <label for="listingDescription">Descrizione listing</label>
            <textarea id="listingDescription" placeholder="Descrizione HTML o testo del listing eBay"></textarea>
          </div>

          <div class="btns">
            <button class="btn-secondary" id="btnLoadProduct">Carica prodotto Shopify</button>
            <button class="btn-primary" id="btnPublish">Pubblica su eBay</button>
          </div>

          <div id="productBox" class="product-box hidden">
            <img id="productImg" alt="product" src="" />
            <div>
              <div style="font-size:18px;font-weight:800;margin-bottom:6px;" id="productTitle">—</div>
              <div class="small muted" id="productMeta1">—</div>
              <div class="small muted" id="productMeta2" style="margin-top:6px;">—</div>
              <div class="small" style="margin-top:10px;">
                <a id="productLink" href="#" target="_blank" rel="noreferrer" style="color:#8db2ff;text-decoration:none;">Apri prodotto Shopify</a>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2>Stato sistema</h2>
        </div>
        <div class="card-body">
          <div class="info">
            <div class="kpi">
              <div class="k">Marketplace</div>
              <div class="v" id="kMarketplace">—</div>
            </div>
            <div class="kpi">
              <div class="k">Merchant Location</div>
              <div class="v" id="kLocation">—</div>
            </div>
            <div class="kpi">
              <div class="k">Environment</div>
              <div class="v" id="kEnv">—</div>
            </div>
            <div class="kpi">
              <div class="k">Token scadenza</div>
              <div class="v" id="kExpiry">—</div>
            </div>
          </div>

          <div class="log" id="log"></div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const overlay = document.getElementById('overlay');
    const overlayTitle = document.getElementById('overlayTitle');
    const overlaySub = document.getElementById('overlaySub');
    const logEl = document.getElementById('log');

    function showOverlay(title, sub) {
      overlayTitle.textContent = title || 'Il sistema sta lavorando';
      overlaySub.textContent = sub || 'Attendi qualche secondo, sto elaborando l\\'operazione in corso.';
      overlay.classList.add('show');
    }

    function hideOverlay() {
      overlay.classList.remove('show');
    }

    function logLine(message, data) {
      const ts = new Date().toLocaleTimeString();
      let line = '[' + ts + '] ' + message;
      if (data !== undefined) {
        try {
          line += '\\n' + JSON.stringify(data, null, 2);
        } catch (e) {
          line += '\\n' + String(data);
        }
      }
      logEl.textContent = line + '\\n\\n' + logEl.textContent;
    }

    function setConnectionUi(connected, metaText) {
      const connDot = document.getElementById('connDot');
      const connText = document.getElementById('connText');
      const connMeta = document.getElementById('connMeta');
      const topStatusDot = document.getElementById('topStatusDot');
      const topStatusText = document.getElementById('topStatusText');

      connDot.className = 'dot ' + (connected ? 'ok' : 'bad');
      topStatusDot.className = 'dot ' + (connected ? 'ok' : 'bad');

      connText.textContent = connected
        ? 'Stato connessione: eBay collegato'
        : 'Stato connessione: eBay NON collegato';

      topStatusText.textContent = connected
        ? 'eBay account collegato'
        : 'eBay account non collegato';

      connMeta.textContent = metaText || '';
    }

    async function api(url, options = {}) {
      const response = await fetch(url, {
        headers: {
          'Content-Type': 'application/json'
        },
        ...options
      });

      let payload;
      try {
        payload = await response.json();
      } catch (e) {
        const text = await response.text();
        throw new Error(text || ('HTTP ' + response.status));
      }

      if (!response.ok) {
        const err = new Error(payload.error ? JSON.stringify(payload.error) : ('HTTP ' + response.status));
        err.payload = payload;
        throw err;
      }

      return payload;
    }

    async function refreshStatus() {
      try {
        const status = await api('/ebay/status');
        document.getElementById('kMarketplace').textContent = status.marketplaceId || '—';
        document.getElementById('kLocation').textContent = status.merchantLocationKey || '—';
        document.getElementById('kEnv').textContent = status.environment || '—';
        document.getElementById('kExpiry').textContent = status.accessTokenExpiresInSec != null
          ? (status.accessTokenExpiresInSec + ' sec')
          : '—';

        setConnectionUi(
          Boolean(status.connected),
          status.connected
            ? ('Token valido. Scadenza: ' + (status.accessTokenExpiresAt || 'n/d'))
            : (status.reason || 'Serve collegare eBay')
        );

        logLine('Stato connessione aggiornato', status);
      } catch (err) {
        setConnectionUi(false, 'Errore nel controllo stato');
        logLine('Errore refresh status', err.message);
      }
    }

    async function loadProduct() {
      const sku = document.getElementById('sku').value.trim();
      if (!sku) {
        alert('Inserisci uno SKU');
        return;
      }

      showOverlay('Caricamento prodotto', 'Sto leggendo il prodotto da Shopify e preparando i dati del publisher...');
      try {
        const data = await api('/ebay/product?sku=' + encodeURIComponent(sku));
        const p = data.product;

        document.getElementById('productTitle').textContent = p.title || '—';
        document.getElementById('productMeta1').textContent = 'SKU: ' + (p.sku || '—') + ' • Prezzo: € ' + (p.price ?? '0') + ' • Q.tà: ' + (p.quantity ?? 0);
        document.getElementById('productMeta2').textContent = 'Vendor: ' + (p.vendor || '—') + ' • EAN: ' + (p.ean || '—') + ' • Tipo: ' + (p.productType || '—');

        const img = document.getElementById('productImg');
        img.src = (p.imageUrls && p.imageUrls[0]) ? p.imageUrls[0] : 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><rect width="100" height="100" fill="#fff"/><text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#888" font-size="12">NO IMG</text></svg>');

        const link = document.getElementById('productLink');
        link.href = p.onlineStoreUrl || '#';

        if (!document.getElementById('listingDescription').value.trim()) {
          document.getElementById('listingDescription').value = p.descriptionHtml || p.descriptionText || '';
        }

        document.getElementById('productBox').classList.remove('hidden');
        logLine('Prodotto Shopify caricato', p);
      } catch (err) {
        logLine('Errore caricamento prodotto', err.payload || err.message);
        alert('Errore caricamento prodotto: ' + err.message);
      } finally {
        hideOverlay();
      }
    }

    async function publishProduct() {
      const sku = document.getElementById('sku').value.trim();
      const categoryId = document.getElementById('categoryId').value.trim();
      const listingDescription = document.getElementById('listingDescription').value;

      if (!sku) {
        alert('Inserisci SKU');
        return;
      }
      if (!categoryId) {
        alert('Inserisci categoryId eBay');
        return;
      }

      showOverlay('Pubblicazione eBay in corso', 'Sto verificando la connessione eBay, sincronizzando inventario e creando/pubblicando l\\'offerta...');
      try {
        const result = await api('/ebay/publish', {
          method: 'POST',
          body: JSON.stringify({
            sku,
            categoryId,
            listingDescription
          })
        });

        logLine('Pubblicazione completata', result);
        alert('Pubblicazione completata con successo');
        await refreshStatus();
      } catch (err) {
        logLine('Errore pubblicazione', err.payload || err.message);
        alert('Errore pubblicazione: ' + err.message);
        await refreshStatus();
      } finally {
        hideOverlay();
      }
    }

    async function disconnectEbay() {
      if (!confirm('Vuoi davvero scollegare l\\'account eBay?')) return;

      showOverlay('Disconnessione eBay', 'Sto rimuovendo i token salvati...');
      try {
        const result = await api('/ebay/disconnect', {
          method: 'POST',
          body: JSON.stringify({})
        });
        logLine('Account eBay disconnesso', result);
        await refreshStatus();
      } catch (err) {
        logLine('Errore disconnessione', err.payload || err.message);
        alert('Errore disconnessione: ' + err.message);
      } finally {
        hideOverlay();
      }
    }

    document.getElementById('btnLoadStatus').addEventListener('click', refreshStatus);
    document.getElementById('btnLoadProduct').addEventListener('click', loadProduct);
    document.getElementById('btnPublish').addEventListener('click', publishProduct);
    document.getElementById('btnDisconnect').addEventListener('click', disconnectEbay);
    document.getElementById('btnConnect').addEventListener('click', () => {
      showOverlay('Redirect verso eBay', 'Sto aprendo il flusso OAuth per collegare il tuo account eBay...');
      window.location.href = '/ebay/connect?returnTo=' + encodeURIComponent('/ebay/publisher');
    });

    refreshStatus();
  </script>
</body>
</html>`);
});

/* ============================================================================
 * ROOT
 * ========================================================================== */

app.get('/', (req, res) => {
  res.redirect('/ebay/publisher');
});

/* ============================================================================
 * START
 * ========================================================================== */

(async () => {
  try {
    await ensureStateDir();

    if (!fs.existsSync(EBAY_STATE_FILE)) {
      await saveEbayState({
        ebayAuth: null,
        meta: {
          updatedAt: new Date().toISOString()
        }
      });
    }

    app.listen(PORT, () => {
      log(\`SyncAmzEby listening on port \${PORT}\`);
      log(\`eBay state file: \${EBAY_STATE_FILE}\`);
      log(\`Publisher UI: \${buildAppUrl('/ebay/publisher')}\`);
    });
  } catch (err) {
    logError('Failed to start server:', err);
    process.exit(1);
  }
})();
