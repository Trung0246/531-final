# HDFS Dataset Visualization Service

A Spring Boot 3.x microservice that reads HDFS-backed datasets, parses them into structured analytics, and exposes visualization-ready REST endpoints.

## Current MVP scope

- Register an HDFS dataset path
- Optionally import a local directory into HDFS
- Parse supported dataset types:
  - maildir-style email archives via `EMAIL_ARCHIVE`
  - tabular CSV reports via `CSV_TEXT`
- Produce chart-friendly analytics for different aspects of the data:
  - email volume by month
  - hourly activity distribution
  - top senders
  - top recipients
  - top mailbox owners
  - communication graph edges
  - top subject keywords
  - rows by observation date for CSV datasets
  - detected metric totals such as `Confirmed`, `Deaths`, and `Recovered`
  - top locations by metric for CSV datasets
  - multi-metric trend lines for CSV datasets
- Serve a minimal dashboard at `/`

## Why this shape?

This project is intentionally **visualization-first**:

- HDFS stores the raw dataset
- Spring Boot handles ingestion, parsing, aggregation, and API delivery
- The frontend consumes dataset-type-specific JSON that is already shaped for dashboards

## Project structure

```text
src/main/java/com/datasetviz
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

`GET /api/datasets/{datasetId}/analytics` is the generic dashboard endpoint and supports both `EMAIL_ARCHIVE` and `CSV_TEXT` datasets.
The detailed email-specific endpoints above remain focused on `EMAIL_ARCHIVE` datasets.

## Example configuration

When running the packaged jar, the application also reads external configuration from the same directory as the jar. Place an `application.yml` beside the jar to override packaged defaults.
You can still update `src/main/resources/application.yml` or use environment variables:

```yaml
server:
  address: 0.0.0.0
  port: 8080

app:
  hdfs:
    uri: file:///
    user: hadoop
    # HDFS path prefix used only for imported files.
    hdfs-path:
    # Local filesystem root allowed for localDirectory.
    local-path:
    embedded:
      enabled: false
      base-dir:
      data-nodes: 1
      name-node-port: 0
      format: true
  analytics:
    default-max-files: 5000
    cache-ttl: PT10M
    max-files-hard-limit: 20000
```

Then start it with `java -jar hdfs-dataset-visualization-service-0.0.1-SNAPSHOT.jar`.

The dashboard is served by the same Spring Boot app, so the frontend URL is controlled by `server.address` and `server.port`.

- `0.0.0.0` listens on all IPv4 interfaces
- `"::"` listens on the IPv6 unspecified address
- `127.0.0.1` or a specific LAN IP binds to one interface only

Example external config beside the jar:

```yaml
server:
  address: 0.0.0.0
  port: 9090
```

IPv6 example:

```yaml
server:
  address: "::"
  port: 8080
```

`app.hdfs.uri` controls which filesystem backend the app talks to.

- `file:///` means single-machine local filesystem mode and does not require Hadoop daemons
- `hdfs://host:port` means external HDFS mode and requires a reachable NameNode
- `app.hdfs.embedded.enabled=true` starts an in-process mini HDFS cluster for development using the same app process

`app.hdfs.uri` and `app.hdfs.hdfs-path` are filesystem destination settings, not local import-source settings.

- `app.hdfs.uri` should point to the reachable HDFS NameNode, for example `hdfs://192.168.1.143:9000`
- `app.hdfs.hdfs-path` is an optional base HDFS directory used only when importing local files into HDFS
- `app.hdfs.local-path` is an optional allowed local root for `localDirectory`; imports outside that root are rejected
- If you do not want a global import prefix, leave `app.hdfs.hdfs-path` empty
- If you want chroot-like import restrictions, set `app.hdfs.local-path` to the only local root that imports may read from
- Local machine paths belong in the `localDirectory` field of the `/api/datasets/import-local` request, not in `app.hdfs.hdfs-path`

Examples:

```yaml
app:
  hdfs:
    uri: file:///
    hdfs-path: /mnt/main/trung/Other/Project/_edu_/CSPC531/final/data/datasets
    local-path: /mnt/main/trung/Other/Project/_edu_/CSPC531/final/data
```

Local single-machine mode above needs no external HDFS service.

```yaml
app:
  hdfs:
    uri: hdfs://192.168.1.143:9000
    hdfs-path: /datasets/uploads
    local-path: /mnt/main/trung/Other/Project/_edu_/CSPC531/final/data
```

Remote HDFS mode above needs a running NameNode at `192.168.1.143:9000`.

```yaml
app:
  hdfs:
    embedded:
      enabled: true
      base-dir: .tmp/datasetviz-hdfs
      data-nodes: 1
      name-node-port: 52000
```

Embedded mode is development-oriented and starts a mini HDFS cluster inside the application process.

`hdfs-path: /datasets/uploads` means imported files are written under that path in the selected filesystem backend.

`local-path: /mnt/main/trung/Other/Project/_edu_/CSPC531/final/data` means the API will only allow `localDirectory` values under that folder on the server machine.

Wrong:

```yaml
app:
  hdfs:
    hdfs-path: /mnt/main/trung/Other/Project/_edu_/CSPC531/final/data
```

That points `hdfs-path` at a local disk path, which is not what this setting means.


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

### 2b) Register a CSV report dataset

This works for CSV datasets such as the Kaggle coronavirus report when the files contain date, location, and numeric metric columns.

```bash
curl -X POST http://localhost:8080/api/datasets/register \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "covid-report",
        "description": "CSV public health report in HDFS",
        "datasetType": "CSV_TEXT",
        "hdfsPath": "/datasets/covid-report"
      }'
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
- The email parser is intentionally lightweight and tuned for raw mail files.
- CSV analytics currently auto-detect date, location, and numeric metric columns and work well for report-style datasets such as coronavirus case reports.
- This is an MVP for exploration and visualization, not a production evidence platform.

## Build the jar

To keep Maven downloads inside this project directory, use a project-local Maven cache and Jansi temp directory:

```bash
mkdir -p .m2/repository .tmp/jansi
JAVA_HOME="$(mise where java@17.0.2)" \
MAVEN_OPTS="-Djansi.tmpdir=$PWD/.tmp/jansi" \
mvn -Dmaven.repo.local="$PWD/.m2/repository" clean package
```

```bash
env JAVA_HOME="$(mise where java@17.0.2)" \
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
SERVER_ADDRESS=0.0.0.0 \
SERVER_PORT=8080 \
APP_HDFS_URI=hdfs://localhost:9000 \
APP_HDFS_USER=hadoop \
java -jar target/hdfs-dataset-visualization-service-0.0.1-SNAPSHOT.jar
```

If you run it on the same machine, open `http://localhost:8080/` by default.
If you change the port, use that port instead. If you bind to `0.0.0.0` or `::`, open the app with the machine's reachable hostname or IP.

## Windows Hadoop warning

If you run the jar on Windows, Hadoop may log warnings about `HADOOP_HOME` or `hadoop.home.dir` not being set. That warning comes from the Hadoop client runtime, not from the dashboard code itself.

- On Linux and macOS, you usually do not need to set these values.
- On Windows, the warning is commonly caused by missing Hadoop Windows helpers such as `winutils.exe`.
- If the app can still connect to HDFS and your requests work, the warning is often non-fatal.

To remove the warning on Windows, install matching Hadoop client binaries and point both `HADOOP_HOME` and `hadoop.home.dir` to that folder.

PowerShell example:

```powershell
$env:HADOOP_HOME = 'C:\hadoop\hadoop-3.5.0'
java -Dhadoop.home.dir=$env:HADOOP_HOME -jar target/hdfs-dataset-visualization-service-0.0.1-SNAPSHOT.jar
```

The directory should contain `bin/winutils.exe`.

## Running the client

There is no separate frontend build or client dev server in this project.

- The UI files live in `src/main/resources/static/`
- Spring Boot serves them automatically
- After starting the backend, open `http://localhost:8080/`

If you want to serve the static files separately, you would need your own static web server plus an API proxy or client-side API base URL changes, because `app.js` currently calls relative `/api/...` endpoints.

Useful runtime environment variables:

- `SERVER_ADDRESS` defaults to `0.0.0.0`
- `SERVER_PORT` defaults to `8080`
- `APP_HDFS_URI` defaults to `hdfs://localhost:9000`
- `APP_HDFS_USER` defaults to empty
- `APP_HDFS_HDFS_PATH` defaults to empty
- `APP_HDFS_LOCAL_PATH` defaults to empty
- `APP_ANALYTICS_DEFAULT_MAX_FILES` defaults to `5000`
- `APP_ANALYTICS_MAX_FILES_HARD_LIMIT` defaults to `20000`
- `APP_ANALYTICS_CACHE_TTL` defaults to `PT10M`

## Next good improvements

- persist dataset registrations in PostgreSQL
- push parsed metadata into OpenSearch or PostgreSQL for faster repeated queries
- add async scan jobs instead of request-time scanning for very large corpora
- add CSV analytics alongside email analytics
- add authentication and per-dataset access control
