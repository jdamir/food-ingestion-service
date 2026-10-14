# Food Receipt Ingestion & Processor Service (Go)

High-performance, transactional ingestion service exposing an isolated **gRPC boundary** linked to foundational authentication layers. Implements asynchronous partner B2B webhook consumption via **Franz-go (Kafka)**, atomic analytical mutations inside **PostgreSQL (pgxpool)**, and secure blob extraction backed by dedicated **MinIO S3** clusters.

## Tech Stack
* **Language:** Go (1.27+)
* **API Protocol:** gRPC (via Google gRPC-Go Core with Reflection)
* **Streaming Protocol:** Apache Kafka (via Franz-go High-Performance Driver)
* **Storage Interface:** MinIO S3 SDK (V7 Object Streaming Middleware)
* **Database Driver:** PostgreSQL pgx/v5 (High-Concurrency Connection Pooling)

## Architectural Flow
1. **gRPC Ingestion Gate & Interception:** Exposes a strict transport layer on port `:8083` protected by a unified `StreamAuthInterceptor`. The gateway routes context requests to a decoupled remote `auth-service` via dedicated channel clients to validate tokens prior to parsing payload schemas.
2. **Transactional State Storage:** Offloads extracted receipt entities into transactional PostgreSQL nodes utilizing low-latency `pgxpool` instances, managing complex query routines under clean application scopes.
3. **Blob Processing Context:** Commits multi-chunk image assets into active S3 storage boundaries (`receipt-minio-s3:9000`) under strict administrative constraints, emitting decoupled streaming events into isolation pipelines (`receipt-images-topic`).
4. **Asynchronous B2B Backpressure:** Spawns specialized background consumer routines (`StoreCallbackWorker`). The async thread automatically absorbs transactional callback events emitted via target partner webhooks without causing memory blocks or thread starvation within active client connections.

## Interface Specifications (`ReceiptProcessorService`)

The service acts as a strict gRPC ingestion gateway, exposing client-side streaming and unary RPCs:

### B2C Media Streaming Endpoints
*   **`UploadReceipt(stream UploadReceiptRequest) returns (UploadReceiptResponse)`**
    *   *Mechanism:* Client-side chunked binary stream (`oneof` data packet framework).
    *   *Requirements:* The initial payload **must** wrap a `Metadata` packet containing the target `file_extension` (`jpg`, `png`), followed by sequence allocation blocks (`bytes chunk`).
    *   *Response Guarantee:* Immediate non-blocking decoupled dispatch state (`"processing"`) with an atomic metadata target assignment identifier (`receipt_id`).
*   **`UploadAudio(stream UploadAudioRequest) returns (UploadAudioResponse)`**
    *   *Mechanism:* Client-side audio matrix buffer streaming (`wav`, `mp3`).
    *   *Response Guarantee:* Retains a strict downstream transaction mapping context (`audio_id`), preserving session pipeline decoupling.

### B2B Direct Ledger Integration
*   **`RegisterStoreReceipt(StoreReceiptRequest) returns (StoreReceiptResponse)`**
    *   *Mechanism:* High-performance unary execution lane bypassing deep-learning pipelines completely.
    *   *Payload Matrix:* Accepts token state hashes, multi-tiered arrays containing `StoreProduct` metrics (item price, fractional portions, granular weights), and real-time absolute balances (`total_price`).
    *   *Response Guarantee:* Returns immediate ingestion validation codes (`"accepted"`) along with a persistent auditing anchor link (`correlation_id`) for distributed trace profiling within downstream `purchase-events` logs.

## Topology & Network Boundaries

The service operates across two distinct network layers to maintain strict isolation of data persistence while allowing validated external orchestration.

| Component | Target Network | Port Mapping | Service Exposure Scope |
| :--- | :--- | :--- | :--- |
| **receipt-processor (Go)** | `default` & `auth-shared-net` | `:8083` | Ingestion Gateway Protocol Gateway |
| **receipt-db (PostgreSQL)** | `default` (Isolated) | `:5432` (`:5456` host) | Sealed Internal Ledger Storage |
| **minio (MinIO S3)** | `default` & `auth-shared-net` | `:9000`, `:9001` | Object Storage Node Architecture |
| **auth-service** | `auth-shared-network` | `:50051` | Remote Authentication Verification |
| **tracker-kafka** | `auth-shared-network` | `:9092` | Global Orchestration Event Stream |

### Network Segmentation Strategy
*   **`default` (Isolated Private Network):** Seals communication lines between the core Go application, the private PostgreSQL ledger, and MinIO storage, completely eliminating direct external attack surfaces.
*   **`auth-shared-network` (External Shared Infrastructure Node):** Bridges the ingestion gateway to core security layers (`auth-service`) and unified enterprise message pipelines (`tracker-kafka`).

## Environment Specifications
*   `DB_DSN`: Internal routing pointer targeted at local storage pools (`postgres://receipt_user:receipt_password@receipt-service-db:5432/receipt_processor_db?sslmode=disable`).
*   `KAFKA_BROKERS`: Infrastructure routing handle mapped inside shared boundaries (`tracker-kafka:9092`).
*   `AUTH_SERVICE_ADDR`: Target routing coordinates for remote validation engines (`auth-service:50051`).
*   `MINIO_ENDPOINT`: Internal storage connection layer node map (`receipt-minio-s3:9000`).

## Deployment & Cache Invalidation

Ensure the external communication architecture exists within your environment workspace before triggering runtime execution tasks:

```bash
docker network create auth-shared-network || true
```

To invalidate layers, rebuild runtime execution matrices inside an ephemeral container, and instantiate active service bindings:

```bash
docker compose build --no-cache && docker compose up --force-recreate
```
