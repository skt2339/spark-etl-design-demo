# spark-etl-design-demo
Scalable PySpark-based ingestion framework implementing idempotent processing, de-duplication, data validation, and workflow orchestration patterns for production-ready data pipelines.


# Tookitaki – Data Engineer Technical Assessment

## Overview
This repo contains solutions (code/pseudo-code) for:
1) De-duplication + idempotent ingestion
2) Large dataset ingestion (60GB CSV/day)
3) Validations (hard vs soft)
4) Great Expectations suite + pipeline integration
5) Airflow DAG design
6) Spark optimization + SQL queries

## Scenario 1: Latest record per party_key
- Deduplicate using Window + row_number partitioned by party_key.
- Ordering: source_updated_at desc, ingested_at desc (tie-breaker).
- Idempotency: deterministic selection + overwrite curated partition (or MERGE by party_key in production).
- Soft deletes: keep tombstones (is_deleted=true) for audit; create active view excluding deleted.

## Scenario 2: 60GB ingestion
- Use explicit schema (no inference), parse types after read.
- Partition by run_date and repartition to control output file sizes.
- Safe restart: write to staging path and commit to final path with marker/metadata.

## Scenario 3: Validations
- Hard validations block ingestion (e.g., party_key non-null, source_updated_at valid).
- Soft validations raise alerts (e.g., name null% threshold, invalid country count).

## Scenario 4: Great Expectations
- Suite includes party_key non-null/unique, country in set, valid timestamp, dob < today, name null% threshold.
- Integrated as a checkpoint before curated write.
- On failure: stop pipeline and/or quarantine data + alert.

## Scenario 5: Airflow DAG
- detect_files >> validate_data >> spark_ingestion >> reconcile_metrics >> notify
- Idempotency via ingestion_metadata (processed files) and partition overwrite / MERGE.

## Scenario 6: Optimization + SQL
- Guidance for shuffles, skew, and small files.
- SQL for duplicates, reconciliation counts, rejected records.
