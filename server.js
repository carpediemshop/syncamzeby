import express from "express";

const app = express();

// mappa temporanea in memoria:
// inventory_item_id -> { sku, price, product_id, variant_id, product_title }
const inventoryMap = new Map();

app.get("/", (req, res) => {
  res.send("SyncAmzEby server running");
});

app.get("/settings", (req, res) => {
  res.send("Settings page placeholder");
});

function buildAmazonPayload({ sku, price, quantity }) {
  return {
    marketplace: "AMAZON_IT",
    sku: sku ?? null,
    price: price ?? null,
    quantity: quantity ?? null,
  };
}

app.post(
  "/webhooks/products",
  express.raw({ type: "*/*" }),
  (req, res) => {
    try {
      const payload = JSON.parse(req.body.toString("utf8"));
      const variants = Array.isArray(payload.variants) ? payload.variants : [];

      const normalizedVariants = variants.map((variant) => {
        const normalized = {
          product_id: payload.id ?? null,
          product_title: payload.title ?? null,
          variant_id: variant.id ?? null,
          inventory_item_id: variant.inventory_item_id ?? null,
          sku: variant.sku ?? null,
          price: variant.price ?? null,
          inventory_quantity: variant.inventory_quantity ?? null,
          updated_at: variant.updated_at ?? null,
        };

        if (normalized.inventory_item_id) {
          inventoryMap.set(String(normalized.inventory_item_id), {
            sku: normalized.sku,
            price: normalized.price,
            product_id: normalized.product_id,
            variant_id: normalized.variant_id,
            product_title: normalized.product_title,
          });
        }

        return normalized;
      });

      console.log("=== PRODUCT WEBHOOK NORMALIZED VARIANTS ===");
      console.log(JSON.stringify(normalizedVariants, null, 2));

      const amazonPayloads = normalizedVariants.map((item) =>
        buildAmazonPayload({
          sku: item.sku,
          price: item.price,
          quantity: item.inventory_quantity,
        })
      );

      console.log("=== AMAZON READY FROM PRODUCT WEBHOOK ===");
      console.log(JSON.stringify(amazonPayloads, null, 2));

      return res.sendStatus(200);
    } catch (error) {
      console.error("Product webhook parse error:", error);
      return res.status(400).send("Invalid JSON");
    }
  }
);

app.post(
  "/webhooks/inventory",
  express.raw({ type: "*/*" }),
  (req, res) => {
    try {
      const payload = JSON.parse(req.body.toString("utf8"));

      const normalized = {
        topic: "inventory_levels/update",
        inventory_item_id: payload.inventory_item_id ?? null,
        location_id: payload.location_id ?? null,
        available: payload.available ?? null,
        updated_at: payload.updated_at ?? null,
      };

      console.log("=== INVENTORY WEBHOOK NORMALIZED ===");
      console.log(JSON.stringify(normalized, null, 2));

      const mapped = inventoryMap.get(String(normalized.inventory_item_id));

      const amazonPayload = buildAmazonPayload({
        sku: mapped?.sku ?? null,
        price: mapped?.price ?? null,
        quantity: normalized.available,
      });

      console.log("=== AMAZON READY FROM INVENTORY WEBHOOK ===");
      console.log(JSON.stringify(amazonPayload, null, 2));

      return res.sendStatus(200);
    } catch (error) {
      console.error("Inventory webhook parse error:", error);
      return res.status(400).send("Invalid JSON");
    }
  }
);

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
