import express from "express";
import axios from "axios";
import pkg from "pg";

const { Pool } = pkg;
const app = express();

const inventoryMap = new Map();

// Amazon
const AMAZON_MARKETPLACE_ID = process.env.AMAZON_MARKETPLACE_ID;
const AMAZON_CLIENT_ID = process.env.AMAZON_LWA_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_LWA_CLIENT_SECRET;
const AMAZON_REFRESH_TOKEN = process.env.AMAZON_REFRESH_TOKEN;
const AMAZON_SELLER_ID = process.env.AMAZON_SELLER_ID;

// Shopify
const SHOPIFY_SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN;
const SHOPIFY_CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const SHOPIFY_CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
const SHOPIFY_LOCATION_ID = process.env.SHOPIFY_LOCATION_ID;

// Database
const DATABASE_URL = process.env.DATABASE_URL;
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    })
  : null;

async function initDb() {
  if (!pool) {
    console.log("DATABASE_URL missing");
    return;
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_amazon_orders (
      amazon_order_id TEXT PRIMARY KEY,
      processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  console.log("DB READY");
}

async function isAmazonOrderProcessed(orderId) {
  if (!pool) return false;

  const result = await pool.query(
    `SELECT 1 FROM processed_amazon_orders WHERE amazon_order_id = $1 LIMIT 1`,
    [orderId]
  );

  return result.rowCount > 0;
}

async function markAmazonOrderProcessed(orderId) {
  if (!pool) return;

  await pool.query(
    `
      INSERT INTO processed_amazon_orders (amazon_order_id)
      VALUES ($1)
      ON CONFLICT (amazon_order_id) DO NOTHING
    `,
    [orderId]
  );
}

async function getAmazonAccessToken() {
  const response = await axios.post(
    "https://api.amazon.com/auth/o2/token",
    new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: AMAZON_REFRESH_TOKEN,
      client_id: AMAZON_CLIENT_ID,
      client_secret: AMAZON_CLIENT_SECRET
    }),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      }
    }
  );

  return response.data.access_token;
}

async function getShopifyAccessToken() {
  try {
    const response = await axios.post(
      `https://${SHOPIFY_SHOP_DOMAIN}/admin/oauth/access_token`,
      new URLSearchParams({
        client_id: SHOPIFY_CLIENT_ID,
        client_secret: SHOPIFY_CLIENT_SECRET,
        grant_type: "client_credentials"
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Accept: "application/json"
        }
      }
    );

    return response.data.access_token;
  } catch (error) {
    console.log("SHOPIFY TOKEN ERROR");

    if (error.response) {
      console.log(error.response.data);
      throw new Error(JSON.stringify(error.response.data));
    } else {
      console.log(error.message);
      throw error;
    }
  }
}

async function shopifyGraphQL(query, variables = {}) {
  const token = await getShopifyAccessToken();

  const response = await axios.post(
    `https://${SHOPIFY_SHOP_DOMAIN}/admin/api/2026-01/graphql.json`,
    { query, variables },
    {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token
      }
    }
  );

  if (response.data.errors) {
    throw new Error(JSON.stringify(response.data.errors));
  }

  if (response.data.data?.inventoryAdjustQuantities?.userErrors?.length) {
    throw new Error(
      JSON.stringify(response.data.data.inventoryAdjustQuantities.userErrors)
    );
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
    query: `sku:${sku}`
  });

  const edge = data.inventoryItems.edges[0];
  if (!edge) return null;

  return {
    inventoryItemId: edge.node.id,
    sku: edge.node.sku,
    tracked: edge.node.tracked
  };
}

async function adjustShopifyInventoryBySku({
  sku,
  delta,
  reason = "correction",
  referenceDocumentUri = "gid://syncamzeby/amazon-order-sync/manual-test"
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
          delta
        }
      ]
    }
  };

  const data = await shopifyGraphQL(mutation, variables);

  console.log("SHOPIFY INVENTORY ADJUST RESULT", JSON.stringify(data, null, 2));

  return {
    ok: true,
    sku,
    delta,
    inventoryItemId: found.inventoryItemId,
    data
  };
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
              quantity: quantity
            }
          ]
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
                      value_with_tax: price
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    };

    const url =
      "https://sellingpartnerapi-eu.amazon.com/listings/2021-08-01/items/" +
      AMAZON_SELLER_ID +
      "/" +
      encodeURIComponent(sku) +
      "?marketplaceIds=" +
      encodeURIComponent(AMAZON_MARKETPLACE_ID) +
      "&issueLocale=it_IT";

    const response = await axios.patch(url, body, {
      headers: {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
      }
    });

    console.log("AMAZON UPDATE SUCCESS", response.data);
    return response.data;
  } catch (error) {
    console.log("AMAZON UPDATE ERROR");

    if (error.response) {
      console.log(error.response.data);
      return error.response.data;
    } else {
      console.log(error.message);
      return { error: error.message };
    }
  }
}

async function getRecentAmazonOrders() {
  const token = await getAmazonAccessToken();

  const createdAfter = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();

  const url =
    "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders" +
    "?MarketplaceIds=" +
    encodeURIComponent(AMAZON_MARKETPLACE_ID) +
    "&CreatedAfter=" +
    encodeURIComponent(createdAfter) +
    "&OrderStatuses=Unshipped,PartiallyShipped,Shipped";

  const response = await axios.get(url, {
    headers: {
      "x-amz-access-token": token,
      "Content-Type": "application/json"
    }
  });

  return response.data;
}

async function getAmazonOrderItems(orderId) {
  const token = await getAmazonAccessToken();

  const url =
    "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders/" +
    encodeURIComponent(orderId) +
    "/orderItems";

  const response = await axios.get(url, {
    headers: {
      "x-amz-access-token": token,
      "Content-Type": "application/json"
    }
  });

  return response.data;
}

async function processRecentAmazonOrders() {
  const ordersData = await getRecentAmazonOrders();
  const orders = ordersData?.payload?.Orders || [];

  console.log("AMAZON RECENT ORDERS FOUND", orders.length);

  let processedCount = 0;

  for (const order of orders) {
    const orderId = order.AmazonOrderId;

    if (!orderId) continue;

    const alreadyProcessed = await isAmazonOrderProcessed(orderId);
    if (alreadyProcessed) {
      console.log("AMAZON ORDER ALREADY PROCESSED", orderId);
      continue;
    }

    const itemsData = await getAmazonOrderItems(orderId);
    const items = itemsData?.payload?.OrderItems || [];

    console.log("PROCESS AMAZON ORDER", orderId, "ITEMS", items.length);

    for (const item of items) {
      const sku = item.SellerSKU;
      const qty = Number(item.QuantityOrdered || 0);

      if (!sku || qty <= 0) {
        continue;
      }

      console.log("AMAZON ORDER ITEM", { orderId, sku, qty });

      await adjustShopifyInventoryBySku({
        sku,
        delta: -Math.abs(qty),
        reason: "correction",
        referenceDocumentUri: `gid://syncamzeby/amazon-order/${orderId}`
      });
    }

    await markAmazonOrderProcessed(orderId);
    processedCount += 1;
  }

  return { found: orders.length, processed: processedCount };
}

app.get("/", (req, res) => {
  res.send("SyncAmzEby running");
});

app.get("/setup/amazon-notifications", async (req, res) => {
  try {
    const token = await getAmazonAccessToken();

    const destinationRes = await axios.post(
      "https://sellingpartnerapi-eu.amazon.com/notifications/v1/destinations",
      {
        name: "SyncAmzEby SQS Destination",
        resourceSpecification: {
          sqs: {
            arn: "arn:aws:sqs:us-east-1:730335601952:syncamzeby-order-change"
          }
        }
      },
      {
        headers: {
          "x-amz-access-token": token,
          "Content-Type": "application/json"
        }
      }
    );

    const destinationId = destinationRes.data.payload.destinationId;
    console.log("DESTINATION CREATED:", destinationId);

    const subRes = await axios.post(
      "https://sellingpartnerapi-eu.amazon.com/notifications/v1/subscriptions/ORDER_CHANGE",
      {
        payloadVersion: "1.0",
        destinationId: destinationId
      },
      {
        headers: {
          "x-amz-access-token": token,
          "Content-Type": "application/json"
        }
      }
    );

    console.log("SUBSCRIPTION CREATED", subRes.data);

    res.json({
      ok: true,
      destinationId,
      subscription: subRes.data
    });
  } catch (error) {
    console.log("AMAZON NOTIFICATION SETUP ERROR");

    if (error.response) {
      console.log(error.response.data);
      return res.status(500).json(error.response.data);
    }

    return res.status(500).json({ error: error.message });
  }
});

app.get("/amazon/test-orders", async (req, res) => {
  try {
    const orders = await getRecentAmazonOrders();
    res.json(orders);
  } catch (error) {
    if (error.response) {
      res.status(500).json(error.response.data);
    } else {
      res.status(500).json({ error: error.message });
    }
  }
});

app.get("/amazon/process-orders", async (req, res) => {
  try {
    const result = await processRecentAmazonOrders();
    res.json({ ok: true, ...result });
  } catch (error) {
    if (error.response) {
      res.status(500).json(error.response.data);
    } else {
      res.status(500).json({ error: error.message });
    }
  }
});

app.get("/shopify/test-adjust", async (req, res) => {
  try {
    const sku = req.query.sku;
    const qty = Number(req.query.qty || 1);

    if (!sku) {
      return res.status(400).json({ error: "missing sku" });
    }

    const result = await adjustShopifyInventoryBySku({
      sku,
      delta: -Math.abs(qty),
      reason: "correction",
      referenceDocumentUri: `gid://syncamzeby/manual-adjust/${sku}`
    });

    res.json({ ok: true, result });
  } catch (error) {
    if (error.response) {
      console.log("SHOPIFY TEST ADJUST ERROR RESPONSE", error.response.data);
      return res.status(500).json(error.response.data);
    }

    console.log("SHOPIFY TEST ADJUST ERROR", error.message);
    return res.status(500).json({ error: error.message });
  }
});

app.post("/webhooks/products", express.raw({ type: "*/*" }), (req, res) => {
  const payload = JSON.parse(req.body.toString());

  payload.variants.forEach((variant) => {
    const existing = inventoryMap.get(String(variant.inventory_item_id)) || {};

    inventoryMap.set(String(variant.inventory_item_id), {
      sku: variant.sku,
      price: variant.price,
      quantity: existing.quantity || variant.inventory_quantity
    });

    if (existing.quantity !== undefined) {
      console.log("SYNC TO AMAZON", {
        sku: variant.sku,
        price: variant.price,
        quantity: existing.quantity
      });

      sendPriceQuantityToAmazon({
        sku: variant.sku,
        price: variant.price,
        quantity: existing.quantity
      });
    }
  });

  console.log("=== PRODUCT WEBHOOK OK ===");
  console.log(
    JSON.stringify(
      payload.variants.map((variant) => ({
        sku: variant.sku,
        price: variant.price,
        inventory_item_id: variant.inventory_item_id,
        inventory_quantity: variant.inventory_quantity
      })),
      null,
      2
    )
  );

  res.sendStatus(200);
});

app.post("/webhooks/inventory", express.raw({ type: "*/*" }), async (req, res) => {
  const payload = JSON.parse(req.body.toString());

  console.log("=== INVENTORY WEBHOOK RAW ===");
  console.log(JSON.stringify(payload, null, 2));

  let mapped = inventoryMap.get(String(payload.inventory_item_id));

  if (!mapped) {
    inventoryMap.set(String(payload.inventory_item_id), {
      quantity: payload.available
    });

    console.log("WAITING PRODUCT DATA FOR", payload.inventory_item_id);
    return res.sendStatus(200);
  }

  mapped.quantity = payload.available;

  const sku = mapped.sku;
  const price = mapped.price;
  const quantity = payload.available;

  console.log("SYNC TO AMAZON", { sku, price, quantity });

  await sendPriceQuantityToAmazon({ sku, price, quantity });

  res.sendStatus(200);
});

const PORT = process.env.PORT || 3000;

initDb()
  .then(() => {
    app.listen(PORT, () => {
      console.log("Server running on port " + PORT);
    });
  })
  .catch((error) => {
    console.log("DB INIT ERROR", error.message);
    process.exit(1);
  });
