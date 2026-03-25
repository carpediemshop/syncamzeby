import express from "express";
import axios from "axios";
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";

const app = express();
const PORT = Number(process.env.PORT || 3000);

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

// Polling
const AUTO_POLL_SQS = String(process.env.AUTO_POLL_SQS || "false") === "true";
const SQS_POLL_INTERVAL_MS = Number(process.env.SQS_POLL_INTERVAL_MS || 20000);

const AMAZON_SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com";

const inventoryMap = new Map();
const processedAmazonOrderEvents = new Set();

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

function requireEnv(name, value) {
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`);
  }
}

function validateEnv() {
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

async function amazonGet(path, accessToken, params = {}) {
  const response = await axios.get(`${AMAZON_SP_API_BASE}${path}`, {
    params,
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

async function amazonPost(path, accessToken, body) {
  const response = await axios.post(`${AMAZON_SP_API_BASE}${path}`, body, {
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

async function amazonPatch(path, accessToken, body, params = {}) {
  const response = await axios.patch(`${AMAZON_SP_API_BASE}${path}`, body, {
    params,
    headers: amazonHeaders(accessToken),
  });
  return response.data;
}

// =========================
// SHOPIFY TOKEN (CLIENT CREDENTIALS GRANT)
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

  if (!response.data?.access_token) {
    throw new Error(
      `Missing Shopify access_token in response: ${JSON.stringify(response.data)}`
    );
  }

  return response.data.access_token;
}

// =========================
// SHOPIFY GRAPHQL
// =========================

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

    const path = `/listings/2021-08-01/items/${encodeURIComponent(
      AMAZON_SELLER_ID
    )}/${encodeURIComponent(sku)}`;

    const response = await amazonPatch(path, token, body, {
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
  if (Array.isArray(destinationsResponse?.payload)) return destinationsResponse.payload;
  if (Array.isArray(destinationsResponse?.payload?.destinations)) return destinationsResponse.payload.destinations;
  if (Array.isArray(destinationsResponse?.destinations)) return destinationsResponse.destinations;
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

    if (!hasConflict) throw error;

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
        eventFilterType: "ORDER_CHANGE"
      }
    }
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

    if (!hasConflict) throw error;

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
// ROUTES
// =========================

app.get("/", (req, res) => {
  res.send("SyncAmzEby running");
});

app.get("/health", async (req, res) => {
  try {
    validateEnv();

    const attrs = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: AMAZON_SQS_QUEUE_URL,
        AttributeNames: ["QueueArn"],
      })
    );

    res.json({
      ok: true,
      queueArn: attrs.Attributes?.QueueArn || null,
      autoPoll: AUTO_POLL_SQS,
      pollIntervalMs: SQS_POLL_INTERVAL_MS,
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/amazon/notifications/setup", async (req, res) => {
  try {
    const result = await ensureAmazonOrderChangeSubscription();
    res.json({ ok: true, ...result });
  } catch (error) {
    if (error.response) {
      return res.status(500).json({
        ok: false,
        error: error.response.data,
      });
    }

    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/amazon/notifications/destinations", async (req, res) => {
  try {
    const result = await listAmazonDestinations();
    res.json({ ok: true, result });
  } catch (error) {
    if (error.response) {
      return res.status(500).json({
        ok: false,
        error: error.response.data,
      });
    }

    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.get("/amazon/notifications/subscription", async (req, res) => {
  try {
    const result = await getOrderChangeSubscription();
    res.json({ ok: true, result });
  } catch (error) {
    if (error.response) {
      return res.status(500).json({
        ok: false,
        error: error.response.data,
      });
    }

    return res.status(500).json({
      ok: false,
      error: error.message,
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
    if (error.response) {
      return res.status(500).json({
        ok: false,
        error: error.response.data,
      });
    }

    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

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
    if (error.response) {
      return res.status(500).json({
        ok: false,
        error: error.response.data,
      });
    }

    return res.status(500).json({
      ok: false,
      error: error.message,
    });
  }
});

app.post("/webhooks/products", express.raw({ type: "*/*" }), async (req, res) => {
  try {
    const payload = JSON.parse(req.body.toString());

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
    }

    return res.sendStatus(200);
  } catch (error) {
    console.log("PRODUCT WEBHOOK ERROR", error.message);
    return res.status(500).send("error");
  }
});

app.post("/webhooks/inventory", express.raw({ type: "*/*" }), async (req, res) => {
  try {
    const payload = JSON.parse(req.body.toString());

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

    return res.sendStatus(200);
  } catch (error) {
    console.log("INVENTORY WEBHOOK ERROR", error.message);
    return res.status(500).send("error");
  }
});

async function start() {
  try {
    validateEnv();

    app.listen(PORT, () => {
      console.log(`SyncAmzEby listening on port ${PORT}`);
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

fai tu la modifica con tutto il codice necessario
Dimmi però cosa cambia in render , se serve.
Nutile che giri attorno. Adesso scrivimi il file completo funzionante.
લ

::contentReference[oaicite:3]{index=3}
