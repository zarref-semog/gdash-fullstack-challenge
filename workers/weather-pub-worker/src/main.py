import requests
import pika
import json
import time

RABBITMQ_URI = ''
WEATHER_API_URL = ''

RETRY_DELAYS = [60, 180, 300]   # 1, 3 and 5 minutes

def retry(operation, *args, **kwargs):
    """
    Retries any operation using the retry delays defined in RETRY_DELAYS.
    Returns the operation result or raises after all failures.
    """
    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        try:
            return operation(*args, **kwargs)

        except Exception as e:
            print(f"[ERROR] Attempt {attempt} failed: {e}")
            print(f"[INFO] Retrying in {delay} seconds...")
            time.sleep(delay)

    print("[ERROR] All retry attempts failed.")
    raise Exception("Operation failed after all retries.")


def create_queues(channel):
    """
    Creates the main queue and the DLQ with proper dead-letter configuration.
    """

    # Dead-letter queue
    channel.queue_declare(queue="weather-data.dlq", durable=True)

    # Main queue → sends failed messages to DLQ
    channel.queue_declare(
        queue="weather-data",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "weather-data.dlq"
        }
    )


def publish_message(queue='', message=None):
    """
    Publishes a message to RabbitMQ. Does not retry internally;
    retry is handled externally by the `retry()` function.
    """
    params = pika.URLParameters(RABBITMQ_URI)
    params.socket_timeout = 5

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    create_queues(channel)

    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2  # persistent
        )
    )

    connection.close()

    print(f"[INFO] Message published to queue '{queue}'")


def _request_locations():
    response = requests.get(f"{WEATHER_API_URL}/locations")
    response.raise_for_status()
    return response.json()


def _request_weather(latitude, longitude):
    response = requests.get(f"{WEATHER_API_URL}?lat={latitude}&lon={longitude}")
    response.raise_for_status()
    return response.json()


def get_locations():
    """Gets list of locations with retry support."""
    try:
        return retry(_request_locations)
    except Exception:
        print("[ERROR] Failed to fetch locations even after retries.")
        return None


def send_to_dlq(payload):
    """
    Forces a message into the DLQ explicitly.
    """

    print("[WARN] Sending message to DLQ...")

    params = pika.URLParameters(RABBITMQ_URI)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue="weather-data.dlq", durable=True)

    channel.basic_publish(
        exchange='',
        routing_key="weather-data.dlq",
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    connection.close()

    print("[WARN] Message stored in DLQ.")


def get_weather_info(latitude, longitude):
    """
    Gets weather info + publishes.
    If any part fails after retries, message goes to DLQ.
    """

    payload = {"latitude": latitude, "longitude": longitude}

    try:
        data = retry(_request_weather, latitude, longitude)
        payload["weather"] = data
    except Exception:
        print("[ERROR] Weather request failed after retries. Sending to DLQ.")
        send_to_dlq(payload)
        return None

    # Try to publish to the main queue
    try:
        retry(publish_message, queue="weather-data", message=data)
    except Exception:
        print("[ERROR] Failed to publish weather data after retries. Sending to DLQ.")
        send_to_dlq(payload)
        return None

    return data


def main():
    print("[INFO] Worker started.")

    locations = get_locations()

    if not locations or not isinstance(locations, list):
        print("[ERROR] Could not fetch locations.")
        return

    print(f"[INFO] {len(locations)} locations received.")

    for loc in locations:
        lat = loc.get("latitude")
        lon = loc.get("longitude")

        if lat is None or lon is None:
            print("[WARN] Invalid location entry, skipping.")
            continue

        print(f"[INFO] Fetching weather for {lat}, {lon}")
        get_weather_info(lat, lon)

    print("[INFO] Worker finished.")


if __name__ == "__main__":
    main()
