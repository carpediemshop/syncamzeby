import express from "express";

const app = express();

app.get("/", (req, res) => {
  res.send("SyncAmzEby server running");
});

app.get("/settings", (req, res) => {
  res.send("Settings page placeholder");
});

app.post(
  "/webhooks/inventory",
  express.raw({ type: "*/*" }),
  (req, res) => {
    try {
      const payloadText = req.body.toString("utf8");
      const payload = JSON.parse(payloadText);

      const normalized = {
        topic: "inventory_levels/update",
        inventory_item_id: payload.inventory_item_id ?? null,
        location_id: payload.location_id ?? null,
        available: payload.available ?? null,
        updated_at: payload.updated_at ?? null,
      };

      console.log("=== INVENTORY WEBHOOK RAW ===");
      console.log(JSON.stringify(payload, null, 2));

      console.log("=== INVENTORY WEBHOOK NORMALIZED ===");
      console.log(JSON.stringify(normalized, null, 2));

      return res.sendStatus(200);
    } catch (error) {
      console.error("Inventory webhook parse error:", error);
      return res.status(400).send("Invalid JSON");
    }
  }
);

app.post(
  "/webhooks/products",
  express.raw({ type: "*/*" }),
  (req, res) => {
    try {
      const payloadText = req.body.toString("utf8");
      const payload = JSON.parse(payloadText);

      const variants = Array.isArray(payload.variants) ? payload.variants : [];

      const normalizedVariants = variants.map((variant) => ({
        product_id: payload.id ?? null,
        product_title: payload.title ?? null,
        variant_id: variant.id ?? null,
        inventory_item_id: variant.inventory_item_id ?? null,
        sku: variant.sku ?? null,
        price: variant.price ?? null,
        inventory_quantity: variant.inventory_quantity ?? null,
        updated_at: variant.updated_at ?? null,
      }));

      console.log("=== PRODUCT WEBHOOK RAW ===");
      console.log(JSON.stringify(payload, null, 2));

      console.log("=== PRODUCT WEBHOOK NORMALIZED VARIANTS ===");
      console.log(JSON.stringify(normalizedVariants, null, 2));

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
