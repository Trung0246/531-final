# Enron HDFS Visualization Service

A Spring Boot 3.x microservice that reads Enron-style email data from HDFS, parses it into a structured model, and exposes visualization-ready REST endpoints.

## Current MVP scope

- Register an HDFS dataset path
- Optionally import a local directory into HDFS
- Parse Enron maildir-style email files
- Produce chart-friendly analytics:
  - email volume by month
  - hourly activity distribution
  - top senders
  - top recipients
  - top mailbox owners
  - communication graph edges
  - top subject keywords
- Serve a minimal dashboard at `/`

## Why this shape?

This project is intentionally **visualization-first**:

- HDFS stores the raw dataset
- Spring Boot handles ingestion, parsing, aggregation, and API delivery
- The frontend consumes JSON that is already shaped for dashboards

## Project structure

```text
src/main/java/com/example/enronviz
├── config
├── controller
├── dto
├── model
├── service
└── util
```

## Main endpoints

### Dataset management

- `POST /api/datasets/register`
- `POST /api/datasets/import-local`
- `GET /api/datasets`
- `GET /api/datasets/{datasetId}`
- `GET /api/datasets/{datasetId}/files?limit=50&recursive=true`

### Analytics

- `GET /api/datasets/{datasetId}/analytics`
- `GET /api/datasets/{datasetId}/analytics/overview`
- `GET /api/datasets/{datasetId}/analytics/volume-by-month`
- `GET /api/datasets/{datasetId}/analytics/hourly-distribution`
- `GET /api/datasets/{datasetId}/analytics/top-senders`
- `GET /api/datasets/{datasetId}/analytics/top-recipients`
- `GET /api/datasets/{datasetId}/analytics/top-mailbox-owners`
- `GET /api/datasets/{datasetId}/analytics/subject-keywords`
- `GET /api/datasets/{datasetId}/analytics/communication-graph`

## Example configuration

Update `src/main/resources/application.yml` or use environment variables:

```yaml
app:
  hdfs:
    uri: hdfs://localhost:9000
    user: hadoop
  analytics:
    default-max-files: 5000
    cache-ttl: PT10M
    max-files-hard-limit: 20000
```

If you already have `core-site.xml` and `hdfs-site.xml` available on the runtime classpath, Hadoop's `Configuration` will pick them up as well.

## Example workflow

### 1) Register an existing HDFS dataset

```bash
curl -X POST http://localhost:8080/api/datasets/register \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "enron-maildir",
        "description": "Enron email corpus in HDFS",
        "datasetType": "ENRON_EMAIL",
        "hdfsPath": "/datasets/enron/maildir"
      }'
```

### 2) Request analytics snapshot

```bash
curl "http://localhost:8080/api/datasets/<DATASET_ID>/analytics?maxFiles=5000&refresh=true"
```

### 3) Open the dashboard

Visit:

```text
http://localhost:8080/
```

## Local import example

```bash
curl -X POST http://localhost:8080/api/datasets/import-local \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "enron-maildir",
        "description": "Imported from local filesystem",
        "datasetType": "ENRON_EMAIL",
        "localDirectory": "/data/enron/maildir",
        "targetHdfsPath": "/datasets/enron/maildir"
      }'
```

## Notes

- The dataset registry is currently in-memory.
- Analytics are computed on demand and cached for a configurable TTL.
- The parser is intentionally lightweight and tuned for Enron-like raw mail files.
- This is an MVP for exploration and visualization, not a production evidence platform.

## Run locally

This repository includes a `pom.xml`, but the Maven wrapper is not bundled in this environment.

```bash
mvn spring-boot:run
```

## Next good improvements

- persist dataset registrations in PostgreSQL
- push parsed metadata into OpenSearch or PostgreSQL for faster repeated queries
- add async scan jobs instead of request-time scanning for very large corpora
- add CSV analytics alongside email analytics
- add authentication and per-dataset access control
