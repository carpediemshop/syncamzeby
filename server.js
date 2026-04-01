import express from "express";
import axios from "axios";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

/* =========================
   CONFIG
========================= */

const EBAY_BASE = "https://api.ebay.com";
const EBAY_MARKETPLACES = [
  { id: "EBAY_IT", lang: "it-IT" },
  { id: "EBAY_DE", lang: "de-DE" },
  { id: "EBAY_FR", lang: "fr-FR" },
  { id: "EBAY_ES", lang: "es-ES" },
  { id: "EBAY_GB", lang: "en-GB" }
];

/* =========================
   TOKEN EBAY
========================= */

async function getEbayAccessToken() {
  const res = await axios.post(
    "https://api.ebay.com/identity/v1/oauth2/token",
    new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: process.env.EBAY_REFRESH_TOKEN,
      scope: "https://api.ebay.com/oauth/api_scope"
    }),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization:
          "Basic " +
          Buffer.from(
            process.env.EBAY_CLIENT_ID +
              ":" +
              process.env.EBAY_CLIENT_SECRET
          ).toString("base64")
      }
    }
  );

  return res.data.access_token;
}

/* =========================
   CATEGORY + ASPECTS
========================= */

async function getCategoryTreeId(marketplaceId, token) {
  const res = await axios.get(
    `${EBAY_BASE}/commerce/taxonomy/v1/get_default_category_tree_id?marketplace_id=${marketplaceId}`,
    {
      headers: { Authorization: `Bearer ${token}` }
    }
  );

  return res.data.categoryTreeId;
}

async function getCategorySuggestions(title, marketplaceId, token) {
  const treeId = await getCategoryTreeId(marketplaceId, token);

  const res = await axios.get(
    `${EBAY_BASE}/commerce/taxonomy/v1/category_tree/${treeId}/get_category_suggestions?q=${encodeURIComponent(
      title
    )}`,
    {
      headers: { Authorization: `Bearer ${token}` }
    }
  );

  return res.data.categorySuggestions?.[0]?.category?.categoryId;
}

async function getRequiredAspects(categoryId, marketplaceId, token) {
  const treeId = await getCategoryTreeId(marketplaceId, token);

  const res = await axios.get(
    `${EBAY_BASE}/commerce/taxonomy/v1/category_tree/${treeId}/get_item_aspects_for_category?category_id=${categoryId}`,
    {
      headers: { Authorization: `Bearer ${token}` }
    }
  );

  const aspects = res.data.aspects || [];

  return aspects
    .filter(a => a.aspectConstraint?.aspectRequired)
    .map(a => a.localizedAspectName);
}

/* =========================
   SMART MAPPING SHOPIFY → EBAY
========================= */

function buildAspects(product, variant, requiredAspects) {
  const aspects = {};

  const brand = product.vendor || "Generico";
  const model =
    variant.sku ||
    variant.title ||
    product.title?.split(" ").slice(0, 3).join(" ");

  requiredAspects.forEach(name => {
    const key = name.toLowerCase();

    if (key.includes("brand") || key.includes("marca")) {
      aspects[name] = [brand];
    } else if (key.includes("model") || key.includes("modello")) {
      aspects[name] = [model];
    } else if (key.includes("type") || key.includes("tipo")) {
      aspects[name] = [product.product_type || "Altro"];
    } else if (key.includes("mpn")) {
      aspects[name] = [variant.sku || "N/A"];
    } else if (key.includes("ean") || key.includes("gtin")) {
      aspects[name] = [variant.barcode || "Does not apply"];
    } else {
      aspects[name] = ["Non specificato"];
    }
  });

  return aspects;
}

/* =========================
   CREATE INVENTORY ITEM
========================= */

async function createInventoryItem({
  sku,
  product,
  variant,
  marketplaceId,
  token
}) {
  const categoryId = await getCategorySuggestions(
    product.title,
    marketplaceId,
    token
  );

  const requiredAspects = await getRequiredAspects(
    categoryId,
    marketplaceId,
    token
  );

  const aspects = buildAspects(product, variant, requiredAspects);

  console.log("REQUIRED ASPECTS:", requiredAspects);
  console.log("FINAL ASPECTS:", aspects);

  await axios.put(
    `${EBAY_BASE}/sell/inventory/v1/inventory_item/${sku}`,
    {
      product: {
        title: product.title,
        description: product.body_html || product.title,
        aspects: aspects,
        imageUrls: product.images?.map(i => i.src) || []
      },
      condition: "NEW",
      availability: {
        shipToLocationAvailability: {
          quantity: variant.inventory_quantity || 0
        }
      }
    },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      }
    }
  );

  return { categoryId, aspects };
}

/* =========================
   CREATE OFFER
========================= */

async function createOffer({
  sku,
  price,
  marketplaceId,
  categoryId,
  token
}) {
  const res = await axios.post(
    `${EBAY_BASE}/sell/inventory/v1/offer`,
    {
      sku,
      marketplaceId,
      format: "FIXED_PRICE",
      availableQuantity: 999,
      categoryId,
      pricingSummary: {
        price: {
          value: price,
          currency: "EUR"
        }
      },
      listingPolicies: {
        fulfillmentPolicyId: process.env.EBAY_FULFILLMENT_POLICY_ID,
        paymentPolicyId: process.env.EBAY_PAYMENT_POLICY_ID,
        returnPolicyId: process.env.EBAY_RETURN_POLICY_ID
      }
    },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      }
    }
  );

  return res.data.offerId;
}

async function publishOffer(offerId, token) {
  await axios.post(
    `${EBAY_BASE}/sell/inventory/v1/offer/${offerId}/publish`,
    {},
    {
      headers: { Authorization: `Bearer ${token}` }
    }
  );
}

/* =========================
   MAIN SYNC ROUTE
========================= */

app.post("/ebay/sync", async (req, res) => {
  try {
    const { product, variant } = req.body;

    const token = await getEbayAccessToken();

    for (const market of EBAY_MARKETPLACES) {
      console.log("SYNC MARKET:", market.id);

      const { categoryId } = await createInventoryItem({
        sku: variant.sku,
        product,
        variant,
        marketplaceId: market.id,
        token
      });

      const offerId = await createOffer({
        sku: variant.sku,
        price: variant.price,
        marketplaceId: market.id,
        categoryId,
        token
      });

      await publishOffer(offerId, token);
    }

    res.json({ ok: true });
  } catch (err) {
    console.error("EBAY ERROR:", err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

/* =========================
   HEALTH
========================= */

app.get("/ebay/health", (req, res) => {
  res.json({ status: "ok" });
});

/* =========================
   START
========================= */

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
