# HDFS Dataset Visualization Service

A Spring Boot 3.x microservice that reads mail archive data from HDFS, parses it into a structured model, and exposes visualization-ready REST endpoints.

## Current MVP scope

- Register an HDFS dataset path
- Optionally import a local directory into HDFS
- Parse maildir-style email files
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
src/main/java/com/example/datasetviz
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
        "name": "mail-archive",
        "description": "Mail archive in HDFS",
        "datasetType": "EMAIL_ARCHIVE",
        "hdfsPath": "/datasets/mail-archive"
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
        "name": "mail-archive",
        "description": "Imported from local filesystem",
        "datasetType": "EMAIL_ARCHIVE",
        "localDirectory": "/data/mail-archive",
        "targetHdfsPath": "/datasets/mail-archive"
      }'
```

## Notes

- The dataset registry is currently in-memory.
- Analytics are computed on demand and cached for a configurable TTL.
- The parser is intentionally lightweight and tuned for raw mail files.
- This is an MVP for exploration and visualization, not a production evidence platform.

## Build the jar

To keep Maven downloads inside this project directory, use a project-local Maven cache and Jansi temp directory:

```bash
mkdir -p .m2/repository .tmp/jansi
JAVA_HOME="$(mise where java@17.0.2)" \
MAVEN_OPTS="-Djansi.tmpdir=$PWD/.tmp/jansi" \
mvn -Dmaven.repo.local="$PWD/.m2/repository" clean package
```

The packaged server jar is written to:

```text
target/hdfs-dataset-visualization-service-0.0.1-SNAPSHOT.jar
```

## Start the server

Start the server with the packaged jar:

```bash
APP_HDFS_URI=hdfs://localhost:9000 \
APP_HDFS_USER=hadoop \
java -jar target/hdfs-dataset-visualization-service-0.0.1-SNAPSHOT.jar
```

The server listens on `http://localhost:8080/` by default.

Useful runtime environment variables:

- `APP_HDFS_URI` defaults to `hdfs://localhost:9000`
- `APP_HDFS_USER` defaults to empty
- `APP_ANALYTICS_DEFAULT_MAX_FILES` defaults to `5000`
- `APP_ANALYTICS_MAX_FILES_HARD_LIMIT` defaults to `20000`
- `APP_ANALYTICS_CACHE_TTL` defaults to `PT10M`

## Next good improvements

- persist dataset registrations in PostgreSQL
- push parsed metadata into OpenSearch or PostgreSQL for faster repeated queries
- add async scan jobs instead of request-time scanning for very large corpora
- add CSV analytics alongside email analytics
- add authentication and per-dataset access control
