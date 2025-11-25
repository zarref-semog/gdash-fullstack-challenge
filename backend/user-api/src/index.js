import express from "express";
import { MongoClient, ObjectId } from "mongodb";
import axios from "axios";
import helmet from "helmet";
import cors from "cors";

const API_GATEWAY_URL = "http://localhost:8000";

const DATABASE_URL = "mongodb://user:password@localhost:27017";
const DATABASE_NAME = "dbname";
const PORT = 8081;

const app = express();

app.use(express.json());
app.use(helmet());
app.use(cors({ origin: [API_GATEWAY_URL] }));

async function connectDB(collectionName) {
  const client = new MongoClient(DATABASE_URL);
  await client.connect();
  const db = client.db(DATABASE_NAME);
  const collection = db.collection(collectionName);
  return [client, collection];
}

app.get("/", (req, res) => {
  res.status(200).send("Hello World!");
});

app.get("/api/user", async (req, res) => {
  let client, collection;

  try {
    const size = parseInt(req.query.size) || 10;
    const page = parseInt(req.query.page) || 0;

    [client, collection] = await connectDB("users");

    const users = await collection
      .find()
      .limit(size)
      .skip(page * size)
      .toArray();

    res.status(200).json(users);
  } catch (e) {
    console.error(e.message);
    res.status(500).send("Server Error");
  } finally {
    if (client) await client.close();
  }
});

app.get("/api/user/:id", async (req, res) => {
  let client, collection;

  try {
    const { id } = req.params;

    [client, collection] = await connectDB("users");

    const user = await collection.findOne({ _id: new ObjectId(id) });

    if (!user) return res.status(404).send("User not found");

    res.status(200).json(user);
  } catch (e) {
    console.error(e.message);
    res.status(500).send("Server Error");
  } finally {
    if (client) await client.close();
  }
});

app.post("/api/user", async (req, res) => {
  let client, collection;

  try {
    const user = req.body;

    [client, collection] = await connectDB("users");

    const result = await collection.insertOne(user);

    res.status(201).json(result);
  } catch (e) {
    console.error(e.message);
    res.status(500).send("Server Error");
  } finally {
    if (client) await client.close();
  }
});

app.put("/api/user/:id", async (req, res) => {
  let client, collection;

  try {
    const { id } = req.params;
    const userUpdates = req.body;

    [client, collection] = await connectDB("users");

    const result = await collection.updateOne(
      { _id: new ObjectId(id) },
      { $set: userUpdates }
    );

    res.status(200).json(result);
  } catch (e) {
    console.error(e.message);
    res.status(500).send("Server Error");
  } finally {
    if (client) await client.close();
  }
});

app.delete("/api/user/:id", async (req, res) => {
  let client, collection;

  try {
    const { id } = req.params;

    [client, collection] = await connectDB("users");

    const result = await collection.deleteOne({ _id: new ObjectId(id) });

    res.status(200).json(result);
  } catch (e) {
    console.error(e.message);
    res.status(500).send("Server Error");
  } finally {
    if (client) await client.close();
  }
});

app.listen(PORT, () => {
  console.log("Server is running on port", PORT);
});
