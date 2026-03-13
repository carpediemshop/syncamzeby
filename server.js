import express from "express";
import crypto from "crypto";

const app = express();

function verifyShopifyWebhook(req, res, next) {
  try {
    const shopifyHmac = req.get("X-Shopify-Hmac-Sha256") || "";
    const secret = process.env.SHOPIFY_API_SECRET || "";

    const digest = crypto
      .createHmac("sha256", secret)
      .update(req.body, "utf8")
      .digest("base64");

    const valid =
      shopifyHmac.length === digest.length &&
      crypto.timingSafeEqual(Buffer.from(shopifyHmac), Buffer.from(digest));

    if (!valid) {
      return res.status(401).send("Invalid webhook signature");
    }

    next();
  } catch (error) {
    console.error("Webhook verification error:", error);
    return res.status(500).send("Verification error");
  }
}

app.get("/", (req, res) => {
  res.send("SyncAmzEby server running");
});

app.get("/settings", (req, res) => {
  res.send("Settings page placeholder");
});

app.post(
  "/webhooks/inventory",
  express.raw({ type: "application/json" }),
  verifyShopifyWebhook,
  (req, res) => {
    try {
      const payload = JSON.parse(req.body.toString("utf8"));
      console.log("Inventory webhook received:");
      console.log(JSON.stringify(payload, null, 2));

      return res.sendStatus(200);
    } catch (error) {
      console.error("Inventory webhook parse error:", error);
      return res.status(400).send("Invalid JSON");
    }
  }
);

app.post(
  "/webhooks/products",
  express.raw({ type: "application/json" }),
  verifyShopifyWebhook,
  (req, res) => {
    try {
      const payload = JSON.parse(req.body.toString("utf8"));
      console.log("Product webhook received:");
      console.log(JSON.stringify(payload, null, 2));

      return res.sendStatus(200);
    } catch (error) {
      console.error("Product webhook parse error:", error);
      return res.status(400).send("Invalid JSON");
    }
  }
);

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
