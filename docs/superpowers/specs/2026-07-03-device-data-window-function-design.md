# Device Latest-Data Query: Window Function Rewrite (P3 task 022)

## Goal

Rewrite `DeviceDataRepository.findLatestByDeviceKey` to use a `ROW_NUMBER()` window function
instead of a per-row correlated subquery, so query latency stays low as `aiot_device_data`
grows, and the execution plan uses the existing index efficiently.

## Baseline measurement (real dev DB, ~86k rows in aiot_device_data)

Current correlated-subquery version, `EXPLAIN ANALYZE` against device key `DK_INJ_001`
(20,324 candidate rows for that device):

```
Sort  (cost=14733.36..14733.61 rows=100 width=84) (actual time=145.791..145.792 rows=3 loops=1)
  ->  Bitmap Heap Scan on aiot_device_data d ...
        Filter: (report_time = (SubPlan 2))
        Rows Removed by Filter: 20321
        SubPlan 2
          ->  Index Only Scan using idx_device_data_key_identifier_time on aiot_device_data d2
                (actual time=0.006..0.006 rows=1 loops=20324)
Execution Time: 145.994 ms
```

The subplan runs once per outer row (20,324 loops) — this is the correlated-subquery cost
that grows with row count per device.

Candidate window-function version, same device key:

```
Subquery Scan on ranked (actual time=21.771..23.257 rows=3 loops=1)
  ->  WindowAgg (actual time=21.770..23.253 rows=3 loops=1)
        Run Condition: (row_number() OVER (?) <= 1)
        ->  Sort (actual time=21.760..22.348 rows=20324 loops=1)
              ->  Bitmap Heap Scan on aiot_device_data d ...
                    ->  Bitmap Index Scan on idx_device_data_key_identifier_time
                          Index Cond: ((device_key)::text = 'DK_INJ_001'::text)
Execution Time: 23.531 ms
```

Single pass over the device's rows (no per-row subplan), ~6x faster (23.5ms vs 146ms), and
still uses `idx_device_data_key_identifier_time` for the initial scan.

## Query rewrite

`DeviceDataRepository.findLatestByDeviceKey` changes from a JPQL correlated-subquery `@Query`
to a native SQL `@Query(nativeQuery = true)`:

```sql
SELECT ranked.id, ranked.device_id, ranked.device_key, ranked.identifier, ranked.value,
       ranked.value_num, ranked.quality, ranked.report_time, ranked.creator, ranked.create_time,
       ranked.updater, ranked.update_time, ranked.deleted, ranked.tenant_id
FROM (
    SELECT d.*,
           ROW_NUMBER() OVER (PARTITION BY d.identifier ORDER BY d.report_time DESC) AS rn
    FROM aiot_device_data d
    WHERE d.device_key = :deviceKey AND d.deleted = 0
) ranked
WHERE ranked.rn = 1
ORDER BY ranked.identifier
```

Native SQL rather than JPQL: HQL/JPQL window-function support is Hibernate-version-dependent,
and this is already a Postgres-specific optimization (matching the existing partial index),
so native SQL avoids relying on undocumented HQL behavior and guarantees the measured plan.
The outer `SELECT` lists entity columns explicitly (not `SELECT *`) so Hibernate's implicit
native-to-entity mapping for `List<DeviceData>` has no ambiguity introduced by the extra `rn`
column from the inner query.

## Non-goals

- No change to the method signature (`List<DeviceData> findLatestByDeviceKey(String deviceKey)`)
  or its callers (`DeviceMcpToolService.queryDeviceStatus`, the `/api/device/{deviceKey}/latest`
  REST endpoint) — same input, same output shape.
- No index changes — `idx_device_data_key_identifier_time` already exists and is used as-is.
- No change to any other `DeviceDataRepository` method.

## Testing

New `DeviceDataRepositoryTest` (`@SpringBootTest`), the first repository-level integration
test in this codebase, following the same reliance on a live local Postgres that
`SparkAgentEngineApplicationTests` already has (per `CLAUDE.md`, Postgres is expected running
at `localhost:5432`, db `spark_ai`):

- Inserts several `DeviceData` rows for a throwaway device key (e.g. `DK_TEST_WINDOW_FN`)
  across two identifiers, each with multiple timestamps.
- Asserts `findLatestByDeviceKey` returns exactly one row per identifier, and that each
  returned row has the newest `reportTime` for its identifier.
- Cleans up the inserted rows explicitly in an `@AfterEach` (not relying on transactional
  rollback, since this project has no configured test-transaction-rollback convention today).

## Acceptance criteria

At the current data volume (~86k rows), the rewritten query's `EXPLAIN ANALYZE` shows a single
index-backed scan with a `WindowAgg` (no correlated per-row subplan), and measured execution
time is meaningfully lower than the correlated-subquery baseline (~146ms → ~24ms measured for
device key `DK_INJ_001`).
