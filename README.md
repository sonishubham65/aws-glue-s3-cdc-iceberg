# AWS Glue CDC to Apache Iceberg using Glue Bookmarks

This repository demonstrates a Proof of Concept (POC) for building an incremental CDC (Change Data Capture) pipeline using **AWS Glue**, **Apache Iceberg**, and **Glue Job Bookmarks**.

The Glue job continuously reads new CDC files from Amazon S3, validates the data, keeps the latest state for each product, and performs an atomic `MERGE` into an Iceberg table.

---

## Architecture

```text
                 +----------------------+
                 |   Source Database    |
                 +----------------------+
                           |
                           | CDC
                           |
                 AWS DMS / CDC Producer
                           |
                           v
                 +----------------------+
                 |      Amazon S3       |
                 |   batch1.json        |
                 |   batch2.json        |
                 |   batch3.json        |
                 +----------------------+
                           |
                   Glue Job Bookmark
             (Reads only new files)
                           |
                           v
                 +----------------------+
                 |      AWS Glue        |
                 |----------------------|
                 | Read CDC Files       |
                 | Validate Schema      |
                 | Validate Operations  |
                 | Convert Timestamp    |
                 | Keep Latest Record   |
                 | Iceberg MERGE        |
                 +----------------------+
                           |
                           |
                           v
              +---------------------------+
              | Apache Iceberg Table      |
              | Latest Product Snapshot   |
              +---------------------------+
```

---

## Features

- Incremental file processing using **Glue Job Bookmarks**
- Supports CDC operations:
  - Insert (`I`)
  - Update (`U`)
  - Delete (`D`)
- Atomic Apache Iceberg `MERGE`
- Schema validation
- CDC operation validation
- Automatic timestamp conversion
- Latest product state using `cdc_timestamp`
- CloudWatch logging
- ACID transactions using Iceberg

---

## Project Structure

```text
.
├── glue_job.py
├── sample-data/
│   ├── batch1.json
│   ├── batch2.json
│   └── batch3.json
└── README.md
```

---

## Sample CDC File

Input files use **Newline Delimited JSON (NDJSON)**.

```json
{"product_id":"P100","sku":"IPHONE16-BLK","price":999,"stock":15,"updated_at":"2026-07-01T10:15:00Z","cdc_timestamp":"2026-07-01T10:15:01Z","op":"I"}
{"product_id":"P101","sku":"S24-ULTRA","price":1100,"stock":30,"updated_at":"2026-07-01T10:00:00Z","cdc_timestamp":"2026-07-01T10:00:02Z","op":"I"}
{"product_id":"P102","sku":"PIXEL10","price":800,"stock":50,"updated_at":"2026-07-01T10:15:02Z","cdc_timestamp":"2026-07-01T10:15:03Z","op":"I"}
```

---

## Supported CDC Operations

| Operation | Description |
|----------|-------------|
| I | Insert Product |
| U | Update Product |
| D | Delete Product |

---

## Processing Flow

For every Glue execution:

1. Read only new files using Glue Job Bookmark.
2. Validate schema.
3. Validate CDC operation (`I`, `U`, `D`).
4. Convert timestamps.
5. Keep only the latest record for each product using `cdc_timestamp`.
6. Perform an atomic Iceberg `MERGE`.
7. Commit the Glue Bookmark only after a successful transaction.

---

## Deduplication Strategy

Multiple CDC events for the same product may exist in a single batch.

Example:

```text
10:00:01  P100  UPDATE
10:00:02  P100  UPDATE
10:00:03  P100  UPDATE
```

Only the latest event is retained:

```text
10:00:03  P100  UPDATE
```

This POC maintains the **latest product snapshot** rather than the complete event history.

---

## Glue Bookmark

The Glue job uses:

```python
transformation_ctx="cdc_source"
```

Glue automatically tracks processed S3 files.

Example:

```text
Run #1

batch1.json
batch2.json

↓

Processed


Run #2

batch1.json
batch2.json
batch3.json

↓

Only batch3.json is processed
```

If the job fails before completion, the bookmark is **not** committed and the files are retried during the next execution.

---

## Transaction Safety

Apache Iceberg provides ACID transactions.

```text
          CDC Batch
              |
              |
      Iceberg MERGE
              |
      +-------+--------+
      |                |
   Success          Failure
      |                |
Commit Bookmark   Bookmark NOT committed
```

If the merge fails:

- No partial updates
- No partial inserts
- No partial deletes
- Glue Bookmark is not committed
- Files are automatically retried

---

## Iceberg MERGE

```sql
MERGE INTO product t
USING cdc_stage s
ON t.product_id = s.product_id

WHEN MATCHED AND s.op='D'
THEN DELETE

WHEN MATCHED AND s.op='U'
THEN UPDATE

WHEN NOT MATCHED AND s.op='I'
THEN INSERT
```

All changes are committed as a **single Iceberg transaction**.

---

## Assumptions

This POC assumes:

- Input files are **NDJSON** (compatible with AWS DMS S3 output).
- CDC files arrive in chronological order.
- `cdc_timestamp` determines the latest state.
- Only the latest product state is required.
- Intermediate CDC events are intentionally ignored.

---

## Technology Stack

- AWS Glue 5.0
- Apache Spark
- Apache Iceberg
- AWS Glue Data Catalog
- Amazon S3
- AWS CloudWatch

---

## Future Enhancements

- Schema evolution
- Multi-table CDC processing
- Iceberg snapshot retention
- EventBridge scheduling
- Downstream synchronization (Fredhopper, OpenSearch, etc.)
- Automated partition optimization

---

## Why Apache Iceberg?

Apache Iceberg provides:

- ACID Transactions
- Atomic MERGE
- Time Travel
- Snapshot Isolation
- Rollback Support
- Schema Evolution
- Hidden Partitioning

These capabilities make Iceberg an excellent choice for building reliable incremental CDC pipelines on Amazon S3.

---

## License

MIT
