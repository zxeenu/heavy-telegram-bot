# Heavy Telegram Bot

## Project Overview

This repository contains the core infrastructure and multiple microservices/apps for our event-driven Telegram ecosystem.

## Infrastructure Services

Infrastructure is managed via Docker Compose in:

- [`infra/docker-compose.yml`](./infra/docker-compose.infra.yml)

This includes:

- RabbitMQ (message broker)
- Redis (cache and ephemeral store)
- MinIO (object storage)
- RedisInsight (Redis UI)

### Starting Infrastructure

Run the following command to start all infra services after setting up the envs:

```bash
docker-compose -f infra/docker-compose.yml up -d
```

## Gateway Service

The gateway service is a Python application that listens to Telegram events using Hydrogram and publishes these events into RabbitMQ.

- Located in the [`gateway/`](./gateway) directory.
- See [`gateway/README.md`](./gateway/README.md) for detailed setup and usage instructions.

### Running the Gateway Service

Before starting the gateway service, ensure your infrastructure services are running.

You can run the gateway service locally using:

```bash
docker-compose -f gateway/docker-compose.yml up -d
```
