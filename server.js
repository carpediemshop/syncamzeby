import express from "express";
import axios from "axios";

const app = express();

const inventoryMap = new Map();

const AMAZON_MARKETPLACE_ID = process.env.AMAZON_MARKETPLACE_ID;
const AMAZON_CLIENT_ID = process.env.AMAZON_LWA_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_LWA_CLIENT_SECRET;
const AMAZON_REFRESH_TOKEN = process.env.AMAZON_REFRESH_TOKEN;

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
      process.env.AMAZON_SELLER_ID +
      "/" +
      sku;

    const response = await axios.patch(url, body, {
      headers: {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
      }
    });

    console.log("AMAZON UPDATE SUCCESS", response.data);

  } catch (error) {
    console.log("AMAZON UPDATE ERROR");

    if (error.response) {
      console.log(error.response.data);
    } else {
      console.log(error.message);
    }
  }
}

app.get("/", (req, res) => {
  res.send("SyncAmzEby running");
});

app.post("/webhooks/products", express.raw({ type: "*/*" }), (req, res) => {
  const payload = JSON.parse(req.body.toString());

  payload.variants.forEach((variant) => {
    inventoryMap.set(String(variant.inventory_item_id), {
      sku: variant.sku,
      price: variant.price
    });
  });

  console.log("PRODUCT WEBHOOK OK");

  res.sendStatus(200);
});

app.post("/webhooks/inventory", express.raw({ type: "*/*" }), async (req, res) => {

  const payload = JSON.parse(req.body.toString());

  const mapped = inventoryMap.get(String(payload.inventory_item_id));

  if (!mapped) {
    console.log("SKU NOT FOUND");
    return res.sendStatus(200);
  }

  const sku = mapped.sku;
  const price = mapped.price;
  const quantity = payload.available;

  console.log("SYNC TO AMAZON", { sku, price, quantity });

  await sendPriceQuantityToAmazon({ sku, price, quantity });

  res.sendStatus(200);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("Server running on port " + PORT);
});
