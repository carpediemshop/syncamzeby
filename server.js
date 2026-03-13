import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.send("SyncAmzEby server running");
});

app.post("/webhooks/inventory", (req, res) => {
  console.log("Inventory update received");
  console.log(req.body);
  res.sendStatus(200);
});

app.post("/webhooks/products", (req, res) => {
  console.log("Product update received");
  console.log(req.body);
  res.sendStatus(200);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
