# WEATHER PUB WORKER

## DESCRIPTION

Responsible for periodically retrieving the list of locations from the Weather API and fetching updated weather data from the OpenWeatherAPI. After collecting the weather information, it publishes messages to a RabbitMQ queue, enabling asynchronous and event-driven processing by other services.

# TODO

- ✅ Retrieve the list of locations from the Weather API;
- ✅ Fetch weather data from the OpenWeatherAPI for each location;
- ✅ Publish weather events to RabbitMQ;
- ✅ Ensure persistent and reliable communication with message broker;
- ✅ Implement error handling and retry logic for external API requests;