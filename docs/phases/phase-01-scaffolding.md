# Phase 01 — repository scaffolding

## Concepts and implementation

The Python package uses `src/` layout so importing `fraud_engine` requires an editable install instead of accidentally importing files from the working directory. `pyproject.toml` declares runtime and test dependencies, Python 3.12+, Ruff, pytest, and four command-line entry points. `Settings` in `config.py` reads environment variables and an optional `.env`; `.env.example` contains only local demo defaults. `db.py` owns the SQLAlchemy engine and ORM models. `migrations/versions/0001_initial.py` creates unique business/event identifiers and a separate decision table. The unique constraints are a prerequisite for replay safety, not a full processing protocol.

FastAPI exposes a small `/system/health` shell. The Compose file uses one PostgreSQL, one Redis, and one single-node Kafka KRaft broker. Data volumes are relative to this D: project directory. Redis uses `noeviction` so memory pressure becomes an explicit error instead of silent feature loss. Kafka uses three partitions and local one-replica internal topics. `fraud-init-topics` creates the two required topics through Kafka's Admin API after broker startup. CI runs Ruff and pytest on Python 3.12. No streaming business logic belongs in this checkpoint.

Important configuration:

```yaml
kafka:
  image: apache/kafka:3.9.1
  environment:
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_NUM_PARTITIONS: 3
    KAFKA_HEAP_OPTS: -Xms256m -Xmx512m
```

## Verification and errors faced

`pytest` exercised the FastAPI shell and configuration; a real Uvicorn process returned `{"status":"ok","phase":"scaffold"}` on `/system/health`. `alembic upgrade head --sql` generated PostgreSQL DDL; the live `alembic upgrade head` and `alembic current` confirmed revision `0001_initial` on PostgreSQL. Live `ping_database()` and Redis `PING` succeeded. Kafka KRaft started, and the Admin API created `transactions.raw` and `fraud.dead-letter` with three partitions each. The first package install failed because the sandbox forbade outbound sockets; repeating the install with approved network access placed dependencies under `D:\Realtime_Fraud_Detection\.venv`. Docker Desktop initially had no daemon and later required elevated API access. Starting it briefly reduced free C: space; stopping it restored space. A `localhost` database setting intermittently stalled connections on this Windows host; the defaults now use `127.0.0.1` and a five-second DB connect timeout. A first probe run failed with `UNKNOWN_TOPIC_OR_PART` because topic creation had not yet run; explicit Admin API initialization fixed it. Measured container memory during verification was about 378 MiB Kafka, 48 MiB PostgreSQL, and 9 MiB Redis; these are snapshots, not capacity promises.

## Deferred work

The schema is a starting point. Checkpoint 8 must implement durable claims, idempotent writes, and concurrent duplicate handling. The API shell does not yet expose transactions or reviews. Docker's WSL `docker_data.vhdx` remained on C: and grew during the image pulls; after stopping the stack, measured C: free space was about 268 MB. The project bind mounts are on D:, but they do not relocate Docker's image store. The supported remedy is Docker Desktop **Settings → Resources → Advanced → Disk image location**, moved to D: before more container work. The disk was not moved automatically because it contains Docker Desktop data outside this repository.
