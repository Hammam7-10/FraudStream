# Real Time Financial Fraud Detection System — Big Data Streaming

A practical Big Data streaming project that simulates bank transactions, processes them in real time, and identifies potentially fraudulent activity using rule-based checks and an existing XGBoost model.

The system publishes transaction events to **Apache Kafka**, processes them with **Apache Spark Structured Streaming**, stores raw events as **Parquet files in Hadoop HDFS**, and records transactions and fraud alerts in a shared **SQLite** database for immediate display in an Arabic dashboard.

The original trained model is retained in `pipeline.joblib`. For lightweight inference inside the Docker environment, the model is also exported as `pipeline.xgb.json`, allowing the application to use the XGBoost tree model without packaging the full training pipeline and its heavier machine-learning dependencies.

> **Project scope:** This is an educational, demonstration-oriented streaming system. It is not a production-ready banking fraud detection platform.

## Key Features

- **Real-time event ingestion:** Publishes simulated bank transactions to an Apache Kafka topic.
- **Stream processing:** Uses Spark Structured Streaming to consume Kafka messages in micro-batches.
- **Rule-based detection:** Flags transactions that meet configured suspicious-activity rules.
- **Machine-learning inference:** Uses an existing XGBoost model to score transactions.
- **Raw-data persistence:** Stores incoming events as Parquet files in Hadoop HDFS.
- **Streaming recovery support:** Stores Spark streaming checkpoints in HDFS.
- **Dashboard and alerts:** Saves transaction records and fraud alerts in a shared SQLite database and displays them in an Arabic web dashboard.
- **Scenario-based testing:** Provides predefined data scenarios for normal activity and different types of suspicious behavior.

## Detection Logic

The system uses three detection paths:

1. **High transaction amount** — flags transactions with an amount greater than or equal to `10,000` by default.
2. **High transaction frequency** — flags activity when the same customer makes at least `4` transactions within a `10-minute` period.
3. **XGBoost prediction** — evaluates transactions using the existing model trained on credit-card transaction data.

The model expects the following input features:

- `V1` through `V28`
- `Amount`

### Model Input and Preprocessing

The original model is available as `pipeline.joblib`, while `pipeline.xgb.json` contains the exported XGBoost tree model used for lightweight inference by the application.

The model is **not a preprocessing pipeline**. The demonstration data generator therefore supplies the required numeric features in the expected order. For real transaction data, the `V1`–`V28` features must be produced using the same preprocessing and feature-generation procedure used during model training. Supplying unrelated or differently transformed values would make the predictions unreliable.

## System Architecture

```text
Transaction Generator / Dashboard
              │
              │ JSON events
              ▼
       Apache Kafka
   (bank-transactions)
              │
              ▼
 Spark Structured Streaming
       + XGBoost inference
              │
       ┌──────┴─────────┐
       │                │
       ▼                ▼
 Hadoop HDFS        Shared SQLite
       │             Database
       │          (transactions,
       │           fraud_alerts)
       │                │
       ▼                ▼
 Raw Parquet       FastAPI Dashboard
 + Checkpoints      and Alerts
```

### Data Flow

1. The dashboard or transaction producer generates transaction events in JSON format.
2. The producer publishes the events to the Kafka topic `bank-transactions`.
3. Spark Structured Streaming consumes the Kafka messages in micro-batches.
4. The streaming job applies the configured detection rules and runs XGBoost inference.
5. Raw events and streaming checkpoints are written to Hadoop HDFS.
6. Transaction records and generated fraud alerts are written to the shared SQLite database.
7. The FastAPI dashboard reads the stored results and displays transactions and alerts.

## Technology Stack

| Component | Purpose |
|---|---|
| Apache Kafka | Transaction event ingestion and buffering |
| Apache Spark Structured Streaming | Micro-batch stream processing and detection |
| XGBoost | Machine-learning inference |
| Hadoop HDFS | Raw Parquet storage and streaming checkpoints |
| SQLite | Shared storage for dashboard transactions and fraud alerts |
| FastAPI | Backend API and dashboard delivery |
| Docker Compose | Multi-container environment orchestration |
| Kafka UI | Kafka topic and broker inspection |
| Hadoop NameNode Web UI | HDFS status and file-system inspection |
| PostgreSQL | Supporting service used for Hive initialization in the course image |

## Docker Services

The Docker Compose environment includes the following services and supporting containers:

- `zookeeper`
- `kafka`
- `kafka-init`
- `kafka-ui`
- `itvdelab` — Hadoop HDFS/YARN environment
- `spark-stream` — Spark streaming application
- `dashboard` — FastAPI application and web dashboard
- A supporting PostgreSQL service for Hive initialization
- A short-lived initialization service for shared-storage permissions

The exact service configuration is defined in `docker-compose.yml`.

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### 1. Start the Environment

From the project directory, run:

```bash
docker compose up --build -d
```

Check the container status:

```bash
docker compose ps
```

### 2. Open the Interfaces

Once the services are running, open the following local addresses:

| Interface | URL |
|---|---|
| Project Dashboard | [http://localhost:8000](http://localhost:8000/) |
| Kafka UI | [http://localhost:8080](http://localhost:8080/) |
| Hadoop NameNode UI | [http://localhost:9870](http://localhost:9870/) |

These `localhost` addresses are intended for access from the machine running Docker. Access from another device requires appropriate network configuration.

### 3. Start a Simulation

1. Open the project dashboard.
2. Select a scenario from the dropdown menu.
3. Click **Start System**.
4. Observe incoming transactions and generated alerts in the dashboard.
5. Click **Stop** to stop transaction generation while keeping the displayed results available.

The default simulation sends approximately **5 transactions per second**. Spark consumes the events in micro-batches, writes raw data to HDFS, and progressively records transactions and alerts in SQLite.

## Sample Data Scenarios

The dashboard provides predefined scenarios for testing different detection paths:

| Preset | Sample count shown in the dashboard | Purpose |
|---|---:|---|
| `mixed` | 121 | A mixture of normal and suspicious transactions |
| `normal_only` | 90 | Normal transactions without deliberately injected suspicious patterns |
| `high_value` | 81 | Transactions designed to test the high-amount rule |
| `velocity` | 103 | Repeated transactions designed to test the transaction-frequency rule |
| `model` | 84 | Transactions intended to exercise the XGBoost inference path |

You can also publish a preset directly from the terminal:

```bash
docker compose run --rm dashboard python -m src.stream_producer --preset mixed --interval 0.1
```

The `--interval` option controls the delay between generated events for this producer command.

## Monitoring and Verification

### Follow Spark Logs

```bash
docker compose logs -f spark-stream
```

### Inspect Files in HDFS

```bash
docker compose exec namenode hdfs dfs -ls -R /fraud
```

The HDFS directory `/fraud` contains the project's stored streaming data and related output, according to the configured application paths.

## Data Storage

The project uses separate storage components for different purposes:

- **Hadoop HDFS:** Stores raw transaction events in Parquet format and Spark Structured Streaming checkpoints.
- **SQLite:** Stores the transaction records and fraud alerts required by the dashboard.
- **XGBoost model files:** `pipeline.joblib` retains the original serialized model, and `pipeline.xgb.json` provides the exported tree model used by the lightweight inference implementation.

A legacy batch-processing implementation, `src/fraud_spark_job.py`, remains in the repository for comparison and reference. It uses the older MongoDB-based path and is separate from the primary streaming workflow described in this README.

## Project Structure

```text
.
├── src/
│   ├── stream_producer.py       # Kafka transaction producer
│   ├── fraud_streaming_job.py   # Spark streaming job, rules, and ML inference
│   ├── xgb_predictor.py         # Lightweight inference using exported XGBoost trees
│   ├── storage.py               # Shared SQLite storage for transactions and alerts
│   ├── api.py                   # FastAPI endpoints and dashboard
│   └── fraud_spark_job.py       # Legacy batch-processing implementation (MongoDB)
├── pipeline.joblib              # Original serialized model
├── pipeline.xgb.json             # Exported XGBoost tree model for lightweight inference
├── docker-compose.yml            # Multi-service Docker environment
└── docs/
    └── PROJECT.md                # Additional project documentation
```

For more implementation details, see [`docs/PROJECT.md`](https://github.com/alimaresh/bank-fraud-detection/blob/main/docs/PROJECT.md).

## Stopping the Project

To stop the containers while preserving Docker-managed data volumes, run:

```bash
docker compose down
```

To remove the associated Docker volumes as well, run:

```bash
docker compose down -v
```

> **Warning:** `docker compose down -v` removes the Compose-managed volumes. Any persistent data stored in those volumes, including HDFS data where configured, may be deleted. Use it only when you intend to reset the environment.

## Limitations and Academic Notes

- The transaction stream is a simulation and should not be interpreted as a connection to a real banking system.
- The XGBoost model was trained on credit-card transaction data and expects the `V1`–`V28` and `Amount` features. These features do not automatically correspond to ordinary banking transaction fields.
- Reliable use with real data requires reproducing the model's original preprocessing and feature-generation steps.
- The configured rules and model predictions are demonstration mechanisms; their effectiveness must be evaluated on representative, appropriately labelled data before any operational use.
- A real banking deployment would require additional controls, including authenticated and encrypted communication, access control, secure secrets management, data-governance policies, monitoring, and defined data-retention procedures.

---

**Project type:** Educational Big Data Streaming project  
**Primary workflow:** Kafka → Spark Structured Streaming → HDFS / SQLite → FastAPI Dashboard