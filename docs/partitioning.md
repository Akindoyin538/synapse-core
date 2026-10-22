# Database Partitioning Implementation

## Overview

The `transactions` table has been converted to a partitioned table using PostgreSQL's native range partitioning by `created_at` timestamp. This enables efficient handling of millions of records by:

- Improving query performance on time-based queries
- Faster VACUUM and maintenance operations
- Easy archival of old data by detaching partitions
- Automatic partition management

## Architecture

### Partitioning Strategy
- **Type**: Range partitioning
- **Key**: `created_at` (TIMESTAMPTZ)
- **Interval**: Monthly partitions
- **Naming**: `transactions_y{YYYY}m{MM}` (e.g., `transactions_y2025m02`)

### Key Changes
1. Primary key changed to composite: `(id, created_at)`
2. Automatic partition creation for upcoming months
3. Retention policy to detach partitions older than 12 months
4. Background cron job runs every 24 hours

## Database Functions

### `create_monthly_partition()`
Creates a partition for 2 months ahead if it doesn't exist.

```sql
SELECT create_monthly_partition();
```

### `detach_old_partitions(retention_months INTEGER)`
Detaches partitions older than the specified retention period (default: 12 months).

```sql
SELECT detach_old_partitions(12);
```

### `maintain_partitions()`
Combines both operations - creates new partitions and detaches old ones.

```sql
SELECT maintain_partitions();
```

## Partition Manager

The `PartitionManager` runs as a background task in the application:

```rust
use synapse_core::db::partition::PartitionManager;

// Runs maintenance every 24 hours
let manager = PartitionManager::new(pool.clone(), 24);
manager.start();
```

### Manual Operations

```rust
// Create partition manually
manager.create_partition().await?;

// Detach old partitions with custom retention
manager.detach_old_partitions(6).await?; // Keep 6 months
```

## Migration Process

The migration (`20250217000000_partition_transactions.sql`) performs:

1. Renames `transactions` → `transactions_old`
2. Creates new partitioned `transactions` table
3. Creates initial 3 monthly partitions
4. Migrates existing data
5. Sets up maintenance functions

### Rollback (if needed)

```sql
-- Restore old table
DROP TABLE IF EXISTS transactions;
ALTER TABLE transactions_old RENAME TO transactions;
```

## Performance Considerations

### Query Optimization
Queries filtering by `created_at` will automatically use partition pruning:

```sql
-- Only scans relevant partition(s)
SELECT * FROM transactions 
WHERE created_at >= '2025-02-01' 
  AND created_at < '2025-03-01';
```

### Indexes
Each partition inherits indexes from the parent table:
- `idx_transactions_status`
- `idx_transactions_stellar_account`
- `idx_transactions_created_at`

## Monitoring

### Check Existing Partitions

```sql
SELECT 
    c.relname AS partition_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM pg_class c
JOIN pg_inherits i ON c.oid = i.inhrelid
JOIN pg_class p ON i.inhparent = p.oid
WHERE p.relname = 'transactions'
ORDER BY c.relname;
```

### Check Partition Bounds

```sql
SELECT 
    c.relname AS partition_name,
    pg_get_expr(c.relpartbound, c.oid) AS partition_bound
FROM pg_class c
JOIN pg_inherits i ON c.oid = i.inhrelid
JOIN pg_class p ON i.inhparent = p.oid
WHERE p.relname = 'transactions';
```

## Archival Strategy

Detached partitions remain as regular tables and can be:

1. **Archived to cold storage**:
```sql
-- Export to file
COPY transactions_y2024m01 TO '/archive/transactions_2024_01.csv' CSV HEADER;

-- Drop after backup
DROP TABLE transactions_y2024m01;
```

2. **Moved to archive schema**:
```sql
CREATE SCHEMA IF NOT EXISTS archive;
ALTER TABLE transactions_y2024m01 SET SCHEMA archive;
```

3. **Compressed**:
```sql
-- Using pg_squeeze or similar tools
```

## Testing

Run tests with partitioned table:

```bash
DATABASE_URL=postgres://synapse:synapse@localhost:5432/synapse_test cargo test
```

## Requirements

- PostgreSQL 14+ (native declarative partitioning)
- No external dependencies (pg_partman not required)

## Read-Model Projection (Swap Orders, Quotes, Fills)

The swap read model is a CQRS-style projection that consumes swap lifecycle events
from the outbox/event consumer path and maintains denormalized views for tenant
queries. Write-side tables remain normalized; the read model is a stable,
rebuildable surface for listing, filtering, and aggregation.

### Table Layout

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `swap_order_summary` | One row per swap order, denormalized with latest quote/fill state | `order_id` (PK), `tenant_id`, `status`, `base_asset`, `quote_asset`, `created_at`, `updated_at`, `filled_amount`, `projection_version` |
| `swap_fill_summary` | One row per fill, linked to its order | `fill_id` (PK), `order_id`, `tenant_id`, `asset`, `amount`, `filled_at`, `projection_version` |
| `swap_tenant_aggregates` | Per-tenant rollups (counts, volumes by status/asset pair) | `(tenant_id, bucket_start, status, asset_pair)` (PK), `order_count`, `fill_count`, `volume`, `projection_version` |
| `swap_projection_checkpoint` | Consumer checkpoint and projection version | `projection_name` (PK), `last_event_id`, `last_event_at`, `projection_version`, `updated_at` |

All read-model tables carry `tenant_id` so every query can apply tenant isolation
filters at the index level.

### Indexes Matching Query Patterns

Documented query patterns and their supporting indexes:

- List orders by tenant + status + time window:
  `idx_swap_order_summary_tenant_status_created (tenant_id, status, created_at DESC)`
- Filter orders by asset pair:
  `idx_swap_order_summary_tenant_pair (tenant_id, base_asset, quote_asset, created_at DESC)`
- Look up fills for an order:
  `idx_swap_fill_summary_order (order_id, filled_at)`
- List fills by tenant + time window:
  `idx_swap_fill_summary_tenant_filled (tenant_id, filled_at DESC)`
- Aggregate lookups by tenant + bucket:
  `idx_swap_tenant_aggregates_tenant_bucket (tenant_id, bucket_start DESC)`

These indexes are designed for keyset (cursor) pagination on `created_at`/`filled_at`
so that list queries avoid deep OFFSET scans.

### Projection Versioning

Each read-model row stores a `projection_version`. The projector also records the
current version in `swap_projection_checkpoint`. When the projection schema or
handler logic changes, bump the version constant; the projector refuses to apply
events at a mismatched version and requires an explicit rebuild. This prevents
silently mixing rows produced by different projection logic.

### Checkpoint and Lag Characteristics

- The projector processes events in order per aggregate (keyed by `order_id`) and
  idempotently: duplicate events are detected via `last_event_id` and per-row
  version/sequence checks, and out-of-order events are buffered or ignored once a
  newer sequence has been applied.
- The checkpoint (`last_event_id`, `last_event_at`) is advanced in the same
  transaction as the read-model writes, so catch-up after downtime resumes exactly
  where it left off.
- Lag is the difference between the newest outbox event timestamp and
  `last_event_at`. Under normal load the projector is expected to trail the write
  side by well under a second; during high throughput or after downtime, lag grows
  with backlog size and is drained by batched catch-up.
- Partial fills arriving after an order is marked complete are applied to
  `swap_fill_summary` and reconciled into `swap_order_summary.filled_amount`
  without regressing terminal status.

### Rebuild / Backfill Procedure

The rebuild command (under `cli/` or `xtask/`) reconstructs the read model from
event history:

1. Truncate read-model tables (or write to shadow tables for zero-downtime swaps).
2. Reset `swap_projection_checkpoint` for the projection.
3. Replay events from the beginning of history in aggregate order, in batches.
4. Verify row counts and aggregate totals against the write-side truth.
5. Atomically swap shadow tables into place (if used) and record the new
   `projection_version`.

For large histories, replay in time-bounded batches and rely on the same
time-based access patterns described above; if event volume grows substantially,
consider applying the same monthly range-partitioning strategy used for
`transactions` to the read-model tables (partition by `created_at`/`filled_at`)
so rebuilds and retention can detach old ranges.

### Retention

Read-model tables follow the same retention posture as the write side: rows older
than the retention window can be detached/archived using the partition management
functions documented above once the tables are partitioned.

## Future Enhancements

- [ ] Automatic compression of old partitions
- [ ] Metrics/alerts for partition health
- [ ] Dynamic retention policy based on storage
- [ ] Sub-partitioning by status or account (if needed)
- [ ] Partition read-model tables by time once volume warrants it
