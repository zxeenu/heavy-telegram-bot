# Heavy Telegram Bot

TLDR: An intentionally overengineered Telegram userbot ecosystem built for architectural play, event choreography, and modular experimentation.

## Project Overview

This repository contains the core infrastructure and microservices for an event-driven (not distributed yet!) Telegram user-bot ecosystem. The project is intentionally overengineered — an experiment in modular design and event choreography using modern tooling.

⚠️ This project logs in as a user account using MTProto. Be aware that using userbots may violate Telegram’s terms of service. Use responsibly and at your own risk.

## Architecture Decisions

- **Event Choreography over Orchestration** - Services react to events independently
- **Saga Pattern** - Distributed workflow without central coordinator
- **Interest Accumulation** - An approach to handling concurrent requests
- **Rate Limiting** - Limiting usage of services

### Interest Accumulation

When multiple users request the same media file, we track their interest via a shared Redis key (based on content hash or normalized URL). Once the download completes, all interested parties are notified. This prevents duplicate downloads and reduces resource use.

### Rate limiting

Strategy: Fixed Window Rate Limiting with TTL

This strategy limits how many actions a user can perform within a fixed time window. It uses a Redis key per user (e.g. rate:user:<id>) to count requests. The counter resets after a set TTL (e.g. 60 seconds), allowing automatic cleanup. If the request count exceeds the allowed limit during the window, further requests are denied until the TTL expires.

Since we are publishing raw telegram events from Gateway into the message broker we can't blindly increment the usage count with this event. Instead, we are allowing services to decide when to increment the usage count. **This should be done after meaningful events.**

#### Note:

The Gateway does not know what any of the services do, so we cant handle this logic inside it. Taking the event parsing logic from other services and putting it in Gateway will unfortunately become unmaintainable as there are more services - it would also lead to use maintaining multiples places with the same parsing logic.

Normalizing telegram events to application events in the Gateway is a solution. This would make things more robust and structured, but would also make adding new features more involved. This also goes against choreographing events.

In favor on keeping things easy to extend for now, we will be delegating rate limiting to the service level.

## Security Model

- Gateway authenticates all incoming requests.
- Correlation IDs, user metadata, or scoped access tokens are attached to events at this boundary.
- Internal services trust messages from the Gateway (or each other) and do not perform redundant authentication.

This approach simplifies each worker’s responsibility and centralizes control.

## Insights

### Event-Driven Architecture & React Development is Surpassingly Similar

Event-based programming has a remarkably similar feel to React programming. Both paradigms share fundamental patterns that make the mental model transferable:

#### Unidirectional Data Flow

- React: Props flow down, events bubble up through component hierarchy
- Event Systems: Events flow through message queues, responses bubble back through services

#### Event Handling

- React: So many ways to do it. `onClick` to handle user click events. `useEffect` to react to changes in reactive state. Most of these functions don't return anything, they just do things.
- Event systems: subscribers listen to events, and do something. It doesn't make sense to return anything when it's event based, you just pass it to the next step in the flow, or end the process.

#### Global State Management

- React: You want to share lift up reactive state to share across different parts of your application tree, you reach out for `context` or a global state management library, and implement it with a setter and a getter.
- Event Systems: Redis for state management - effectively state that has been lifted to share across different parts. But its more powerful, you can set automatic cleanups with TTL (You do have similar ability to clean up in React via returning a cleanup function in a `useEffect`).

#### Event Propagation

- React: Event bubbling through the DOM tree with `stopPropagation()` control
- RabbitMQ: Exchange types (direct, fanout, topic) for controlling event distribution patterns. You can change whether multiple consumers receives the same event, or whether they disappear after an event has been touched by a subscriber.

#### Composition

- React: You can separate out functionality via `hooks` and `components`. Encapsulated boxes that take props and output behavior or UI. Components can be dumb (they don't do business logic, and just react to props) or they can be smart (they take care of business logic, calling endpoints, handling toast notifications)
- Microservices: Break out functionality into its own service. They can either dumbly call other events to handle logic it cant do, or they can take care of everything.

This mental models transfer suggests that frontend developers already possess much of the conceptual framework needed for distributed systems architecture. The shift from "what happens when a button is clicked?" to "what happens when an event is published?" feels very natural.

Both types of programming have similar reactive event-driven flows.

## Getting Started

1. Start the infrastructure
2. Start the Gateway
3. Start MediaPirate

You should now be able to interact with the bot via Telegram.

## Infrastructure Services

Infrastructure is managed via Docker Compose in:

- [`infra/docker-compose.yml`](./infra/docker-compose.yml)

This includes:

- 📨 **RabbitMQ** - message broker
- 🧠 **Redis** - cache and ephemeral data store
- 💾 **MinIO** - S3-compatible object storage
- 🧭 **RedisInsight** - Redis UI for debugging and introspection

### Starting Infrastructure

Ensure you’ve configured the necessary environment variables, then start the services:

```bash
docker-compose -f infra/docker-compose.yml up -d
```

## Gateway Service

> **📤 Event Publisher**  
> Listens to Telegram events and publishes them into RabbitMQ.

### What it is

The Gateway service is a Python application that listens to Telegram events using Hydrogram and publishes them to RabbitMQ.

- Located in the [`gateway/`](./gateway) directory.
- See [`gateway/README.md`](./gateway/README.md) for detailed setup and usage instructions.

### Key features

- Associating logs with correlation IDs handling using `contextvars`
- Computes time taken for event to be received into Gateway and dispatched out of Gateway
- Speeds up user response by reusing videos and audios already uploaded to Telegram if available via a Redis hashmap. If the uploaded link is expired, we default to fetching the data from object storage via a presigned url.
- Cleans up files saved to disk via an Redis counter. Files are deleted oldest first.
- Adds metadata to the event envelope letting subscribers know if the event comes from a user that has been rate limited

#### Interest Accumulation

To upload a file do Telegram you can use a actual file on disk, or the `file_id` on Telegram's servers. The issue here is that the `file_id` on telegram expires.

When we send videos and audios back to the user, we are first trying to see if we have access to a cached `file_id`, and if it is not available, then we get the file from our object storage via a supplied presigned get request.

When sending the uploading the object to telegram, we also check if it exists in the temp downloads directory, and it doesn't, we download it. So there's 2 checks until this point. Check redis cache for a telegram `file_id`, and if not available get a fresh copy from object storage.

If an upload to telegram is successful, we cache it to use later. These cached `file_id`s expire.

The issue here is that, what if multiple people are requesting for an object that is not on disk (a fresh video source).

Now, functionally, this issue can be resolved by using the `correlation_id` as a filename for the initial download, and then then renaming it to the actual object's name after it has been uploaded to telegram. But it would still do redundant computations: downloading, renaming and deleting files - when 2 files that have different `correlation_id` map to the same actual object name. This will occur, because we need to content based addressing needs maps to the same file when given the same content (content being the presigned url in this case).

So what we have instead opted for the following process:

- Every presigned url will map to an `interest_accumulator_key` to use a a lock
- We will check if the `interest_accumulator_key` lock exists, before doing any processing, and then republish an event after a small delay if it does.
- If the `interest_accumulator_key` does not exist, then we will process it normally.

This process gives us the ability to delay events that are being locked, but when they come back... they will get the `file_id` that was cached. The accumulator pattern relies of the redis cache!

_The delayed requests essentially become cache hits rather than duplicate work - transforming a coordination problem into a caching optimization._

### Task Roadmap

- [x] Listen for video downloads events, and upload from minio into telegram
- [x] Optimize video uploading, by reusing documents already in telegram
- [ ] JSON Schema implementation
- [ ] Add support for dynamically allowing other users to interact with certain functionality
- [x] Implement basic authentication
- [ ] Implement dynamic authorization and only publish events that have to be worked on
- [ ] Implement OpenTelemetry with `contextvars` correlation support
- [ ] Implement Redis TTL-based heartbeat for service health
- [x] Handle race condition via interest accumulation. Decouple download requests from download execution - accumulate interested parties and fan-out results for success and failed for all interested parties.

### Supported Command Words (🚧 PLANNED)

- `.whoami` — tells the user their current status (banned, blessed, or unmolested)
- `.grace <30d?>` — Allow a chat to interact with the bot forever, or with an optional TTL
- `.bless @<username> <30d?>` — Bless user for 30 days, with an optional TTL
- `.smite` — Permanent ban from bot interactions for everyone in chat forever (no TTL)
- `.hammer @<username> <1h?>` — Temporary ban, with an optional TTL

### Running the Gateway Service

Start after infrastructure is running:

```bash
docker-compose -f gateway/docker-compose.yml up -d
```

## MediaPirate Service

> **📥 Event Subscriber & Event Publisher**  
> Consumes raw events from RabbitMQ and processes or delegates them.

### What it is

MediaPirate is a distributed content relay and command system designed to experiment with messaging patterns, service orchestration, and multi-user sync.

- Located in the [`media-pirate/`](./media-pirate) directory.
- See [`media-pirate/README.md`](./media-pirate/README.md) for detailed setup and usage instructions.

### Key features

- Associating logs with correlation IDs handling using `contextvars`
- Injecting `correlation_id` received from Gateway for inter-service context-aware logging
- Content-Based Addressing for object storage
- Fetching from Youtube, Tiktok, and anything else that is supported by `yt-dlp` is handled in an idempotent manner
- Enriches stored documents with meta data for future analytics
- Rate limits usage based on time and user.

#### Content-Based Addressing

This project uses a content-based addressing strategy to deduplicate downloads and maintain consistency across services. Each media object is identified by a unique hash derived from the normalized source URL (with query parameters stripped) and the extraction type (audio or video). This allows media derived from the same URL — but with different formats or intents — to be treated as distinct entities.

The content hash is used to:

- Uniquely identify media across services
- Prevent redundant downloads/uploads
- Store files in MinIO under type-scoped paths: audio/<hash>.mp3 and video/<hash>.mp4

The full source URL and normalized form are preserved as metadata to ensure traceability, support introspection, and allow for future normalization improvements.

### Task Roadmap

- [x] Handle YouTube downloads directly to disk
- [x] Upload downloaded files to MinIO
- [ ] Enforce file size limits for small downloads
- [ ] Implement durable, idempotent jobs for large downloads with retry support
- [ ] JSON Schema implementation (cross-service payload validations)
- [ ] Implement OpenTelemetry with `contextvars` correlation support
- [ ] Implement Redis TTL-based heartbeat for service health
- [ ] Reuse downloaded files when doing audio extraction (if a video exists inside the bucket)

### Supported Command Words

- `.vdl <url>` — Download a file from a given URL
- `.vdl` (as a reply) — Download a file from the replied message
- `.adl <url>` — Download and extract the audio from a given URL
- `.adl` (as a reply) — Download and extract the audio from the replied message

### Running the MediaPirate Service

Ensure infrastructure is running before starting:

```bash
docker-compose -f media-pirate/docker-compose.yml up -d
```

## QuarterMaster Service (🚧 PLANNED)

> **📥 Event Subscriber & Job Processor**  
> Consumes download job events from RabbitMQ and processes them reliably.

QuarterMaster is a Python service responsible for consuming download-related events from RabbitMQ and dispatching durable, long-running download jobs. It is designed to handle large downloads and rate-limited sources with support for:

- Resumable downloads (planned feature)
- Retry and failure handling
- Persistence and idempotency for reliable processing

This service acts as the heavy-duty worker in your media pipeline, offloading complex or resource-intensive tasks from the MediaPirate service.

- Located in the [`quarter-master/`](./quarter-master) directory.
- See [`quarter-master/README.md`](./quarter-master/README.md) for detailed setup and usage instructions.

## Logger Service (🚧 PLANNED)

> **📥 Event Subscriber**  
> Consumes logging events from RabbitMQ and find a place to stash them.

The Logger service is a Go application that listens to log events from RabbitMQ and stores them centrally. It provides visibility into system behavior across services and helps monitor event choreography.

## 🧩 Current Flow

```mermaid
flowchart TD
    Gateway[Telegram Gateway]
    MediaPirate[MediaPirate]
    Commands{Commands}
    Results{Results}

    Gateway -->|events.telegram.raw<br>🔐 authenticated| MediaPirate
    MediaPirate --> Commands
    Commands -->|commands.media.video_download<br/>commands.media.audio_download| MediaPirate
    MediaPirate --> Results
    Results -->|events.dl.video.ready<br/>events.dl.audio.ready| Gateway

    classDef authEdge stroke:#green,stroke-width:3px
```

## 🚧 Planned Flow

```mermaid
flowchart TD
    T1[Telegram Gateway]
    M1["MediaPirate - Event Processor"]

    T1 -->|raw event| E1((events.telegram.raw))
    E1 --> M1

    M1 --> E2((commands.tiktok.download))

    %% Internal branching inside MediaPirate command handler
    E2 -->|small download| DD["Direct Download Handler"]
    E2 -->|large download| DW["QuarterMaster - Queue Processor"]

    %% Retry loop for Download Worker
    DW -->|retry on fail| DW

    %% If direct download fails, push to queue processor
    DD -- fails --> DW

    DD -->|success| E3((commands.telegram.reply))
    DW -->|success| E3

    E3 -->|return blob to user| T1
```
