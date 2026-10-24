# KafkaEventSender

An Android event producer, written in Kotlin, that emits structured telemetry events to a
self-hosted Kafka cluster through the Confluent REST Proxy.

It exists to solve a problem that comes up whenever you build a streaming pipeline: you need a
real, continuous source of mobile events to develop and test against. Synthetic generators
produce unrealistically uniform data, and wiring a native Kafka client into an Android app means
shipping the whole JVM client and exposing brokers to the handset. This app instead posts JSON
over HTTP to the REST Proxy, which keeps the client thin and the brokers unexposed.

It produces two event types — periodic GPS fixes from a foreground service, and manual
button-press events — wrapped in a common envelope carrying device, session and context metadata.

## Architecture

```
┌──────────────────────────────┐
│ Android device               │
│                              │
│  MainActivity (Compose UI)   │  manual events
│         │                    │
│  LocationForegroundService   │  periodic location fixes, every 5 s
│         │                    │
│         └──> KafkaEventSender│  Retrofit + OkHttp + Gson, Dispatchers.IO
└───────────────┬──────────────┘
                │  POST /topics/{topic}
                │  Content-Type: application/vnd.kafka.json.v2+json
                ▼
      ┌─────────────────────┐
      │ Confluent REST Proxy│  :8082
      └──────────┬──────────┘
                 ▼
          ┌─────────────┐
          │ Kafka broker│   topic: events
          └─────────────┘
```

Each event is sent as a single-record produce request, keyed by its `eventId`, so records
distribute evenly across partitions and the response carries back the assigned partition and
offset.

## Event envelope

Every event shares one envelope and differs only in `eventType` and `payload`:

```json
{
  "records": [
    {
      "key": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
      "value": {
        "eventId": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
        "eventType": "location",
        "timestamp": 1745600000000,
        "device": {
          "deviceId": "9774d56d682e549c",
          "manufacturer": "Google",
          "model": "Pixel 7",
          "osVersion": "Android 14"
        },
        "session": { "sessionId": "b2c3d4e5-..." },
        "context": { "network": "wifi", "battery": 82 },
        "payload": {
          "lat": 24.8607,
          "lon": 67.0011,
          "accuracy": 12.5,
          "speed": 0.0
        }
      }
    }
  ]
}
```

| Field | Source |
| --- | --- |
| `eventId` | Random UUID per event; also used as the record key |
| `timestamp` | `System.currentTimeMillis()` at construction |
| `device.deviceId` | `Settings.Secure.ANDROID_ID` |
| `session.sessionId` | Random UUID, generated once per Activity or Service instance |
| `context.network` | `wifi`, `cellular` or `unknown`, from `ConnectivityManager` |
| `context.battery` | Battery percentage from `BatteryManager` |

### Event types

| `eventType` | Payload | Emitted by |
| --- | --- | --- |
| `location` | `{ lat, lon, accuracy, speed }` | Foreground service every 5 s, or the one-shot button |
| `button_pressed` | `{}` | "Send Button Event" |

## Requirements

- Android SDK with a device or emulator on API 26+ (`minSdk 26`, `targetSdk 36`)
- JDK 11
- A reachable Kafka broker and Confluent REST Proxy

## Setup

**1. Start Kafka and the REST Proxy.** Any deployment works as long as the proxy is reachable from
the device. A minimal local stack:

```yaml
# docker-compose.yml
services:
  kafka:
    image: apache/kafka:3.9.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  rest-proxy:
    image: confluentinc/cp-kafka-rest:7.7.0
    depends_on: [kafka]
    ports: ["8082:8082"]
    environment:
      KAFKA_REST_HOST_NAME: rest-proxy
      KAFKA_REST_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_REST_LISTENERS: http://0.0.0.0:8082
```

**2. Configure the app.** Copy the example file and fill it in:

```bash
cp local.properties.example local.properties
```

```properties
sdk.dir=/path/to/your/Android/Sdk
REST_PROXY_URL=http://192.168.1.10:8082/
TOPIC=events
```

`local.properties` is gitignored and must not be committed. Both values are injected at compile
time as `BuildConfig` fields, falling back to `http://localhost:8082/` and `events` if absent.

Use the host's LAN IP, not `localhost` — on a device or emulator `localhost` resolves to the
handset itself. The trailing slash on `REST_PROXY_URL` is required by Retrofit's `baseUrl`.

**3. Build, install and launch.**

```bash
./gradlew installDebug
```

The `installDebug` task is wired to launch the app over `adb` afterwards, so no manual start is
needed.

**4. Verify events are landing.**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic events --from-beginning
```

The app's status line also reports the partition and offset returned by the proxy for each
successful send, which is the quickest confirmation that the whole path works.

## Permissions

| Permission | Why |
| --- | --- |
| `INTERNET` | POST to the REST Proxy |
| `ACCESS_NETWORK_STATE` | Populate `context.network` |
| `ACCESS_FINE_LOCATION`, `ACCESS_COARSE_LOCATION` | Location fixes |
| `ACCESS_BACKGROUND_LOCATION` | Keep emitting while the app is backgrounded |
| `FOREGROUND_SERVICE`, `FOREGROUND_SERVICE_LOCATION` | Required for a location-typed foreground service on modern Android |

Background location is requested as a second, separate prompt after fine location is granted,
which is what the platform requires. The foreground service posts a low-importance ongoing
notification while tracking.

## Design notes

**REST Proxy instead of a native Kafka client.** The JVM Kafka client is large, assumes a
long-lived connection to every broker, and would require exposing the broker listeners to
untrusted mobile networks. The REST Proxy keeps a single HTTP endpoint at the edge and lets the
app stay a thin producer. The cost is one HTTP round trip per event and no client-side batching.

**`eventId` as the record key.** Keying by a per-event UUID spreads records uniformly across
partitions. This deliberately trades away per-device ordering — see below.

**Compile-time configuration.** The proxy URL and topic come from `local.properties` through
`BuildConfig` rather than being hardcoded, so the repository holds no environment-specific values
and no endpoint has to be redacted before publishing.

**Foreground service for continuous capture.** Android aggressively suspends background work, so
a plain coroutine or `WorkManager` job cannot sustain a 5-second cadence. A location-typed
foreground service is the only reliable mechanism, at the cost of a permanent notification.

## Known limitations

These are real, and worth stating rather than discovering:

- **At-most-once delivery.** Send failures are swallowed by `runCatching` in the service, so any
  event produced while offline or during a proxy outage is lost silently.
- **No batching.** Every event is its own HTTP request and its own produce call.
- **Connection reuse is disabled.** The OkHttp connection pool is configured with zero idle
  connections, so each send pays a fresh TCP handshake.
- **Cleartext HTTP.** `usesCleartextTraffic="true"` is set and the proxy is addressed over plain
  HTTP, which is acceptable on a lab network and nowhere else.
- **`ANDROID_ID` as device identity.** It resets on factory reset and is scoped per signing key,
  so it is not a stable long-term identifier.
- **Divergent session IDs.** `MainActivity` and `LocationForegroundService` each generate their
  own `sessionId`, so events from the UI and from background tracking appear as separate sessions
  on the same device.
- **Tests are template stubs.** `ExampleUnitTest` and `ExampleInstrumentedTest` are the generated
  defaults and assert nothing meaningful.

## What I'd do differently

**I'd make delivery durable before anything else.** Right now a dropped request is a lost event,
which makes the stream unusable as a test source for exactly the scenarios that matter — poor
connectivity and handoffs. I'd write events to a local Room queue first and drain it with
`WorkManager` under a network constraint, so the app becomes at-least-once and the consumer side
deduplicates on `eventId`, which is already a natural idempotency key.

**I'd key records by `deviceId` instead of `eventId`.** Uniform partition spread was the wrong
goal: the useful guarantee for telemetry is that one device's events arrive in order, and keying
by a per-event UUID throws that away for a property nobody needed.

**I'd batch, and re-enable connection reuse.** The REST Proxy accepts many records per request,
and the current one-event-per-request design combined with a disabled connection pool means a full
handshake per GPS fix — avoidable battery and latency cost for no benefit.

**I'd hoist the envelope construction out of both callers.** `buildBaseEvent`, `getDeviceInfo` and
`getContextInfo` are duplicated verbatim in `MainActivity` and `LocationForegroundService`, which
is how the two components ended up disagreeing about session identity. One collaborator owning the
envelope and the session would have made that bug impossible instead of merely unlikely.

## Stack

Kotlin 2.2.10 · Jetpack Compose (BOM 2026.02.01, Material 3) · Coroutines 1.7.3 ·
Retrofit 2.9.0 + Gson · OkHttp · Play Services Location 21.3.0 · AGP 9.1.0 · Gradle version
catalog
