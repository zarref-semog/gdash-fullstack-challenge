# WEATHER SUB WORKER

## DESCRIPTION

Responsible for consuming weather events from the RabbitMQ queue and processing them with an Ollama model to extract insights, patterns, or observations from the weather data. After generating insights, it sends the results to the Weather API for storage or further processing.

## TODO

- Consume weather events from RabbitMQ;
- Process weather data using an Ollama model;
- Generate AI-based insights from the processed data;
- Send insights to the Weather API;
- Ensure reliable message consumption and fault tolerance;
- Implement monitoring and logging for model execution;