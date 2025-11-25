import express from "express";
import { MongoClient } from "mongodb";
import axios from "axios";
import helmet from "helmet";
import cors from "cors";

const API_GATEWAY_URL = 'localhost:8000';
const WEATHER_API_URL = 'https://api.openweathermap.org/data/2.5/weather';
const API_KEY = 'apikey';
const DATABASE_URL = 'mongodb://user:password@localhost:27017';
const DATABASE_NAME = 'dbname';
const PORT = 8082;

const app = express();

app.use(express.json());
app.use(helmet());
app.use(cors({ origin: [API_GATEWAY_URL] }));

async function getWeatherInfo({ lat, lon, city, state, country }) {
  let url = WEATHER_API_URL;

  if (lat && lon) {
    url += `?lat=${lat}&lon=${lon}`;
  } else if (city && (state || country)) {
    url += `?q=${city}${state ? `,${state}` : ''}${country ? `,${country}` : ''}`;
  } else {
    throw new Error("No required parameters were passed.");
  }

  url += `&appid=${API_KEY}`;

  const { data } = await axios.get(url);
  return data;
}

async function persist(collectionName, document) {
  let client;
  let result = {};

  try {
    client = new MongoClient(DATABASE_URL);
    await client.connect();

    const database = client.db(DATABASE_NAME);
    const col = database.collection(collectionName);

    result = await col.insertOne(document);
  } catch (e) {
    console.error(e.message);
  } finally {
    if (client) await client.close();
  }

  return result;
}

async function notify(subject, subscribers = []) {
  if (!Array.isArray(subscribers) || subscribers.length === 0) return;

  const promises = subscribers.map(sub =>
    axios.post(sub, subject).catch(err => console.error("Notify error:", err.message))
  );

  await Promise.all(promises);
}

app.get("/", (req, res) => {
  res.status(200).send("Hello World!");
});

app.get("/api/weather", async (req, res) => {
  try {
    const data = await getWeatherInfo(req.query);

    await persist("weatherInfo", data);
    res.status(200).json(data);
  } catch (e) {
    res.status(400).send(e.message);
  }
});

app.post("/api/weather", async (req, res) => {
  try {
    const result = await persist("weatherInfo", document);

    await notify(result, [API_GATEWAY_URL]);

    res.status(200).json(result);
  } catch (e) {
    res.status(500).send(e.message);
  }
});

app.listen(PORT, () => {
  console.log("Server is running on port", PORT);
});
