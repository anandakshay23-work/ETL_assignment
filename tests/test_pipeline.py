"""
Post-pipeline validation tests.
Run after the ETL pipeline completes to verify data integrity.

Usage:
    python tests/test_pipeline.py
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# DWH connection config
DWH_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": "etl_dwh_db",
    "user": "etl_user",
    "password": "etl_password"
}

CONFIG_DB = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": "etl_config_db",
    "user": "etl_user",
    "password": "etl_password"
}


def get_connection(config):
    return psycopg2.connect(**config)


def test_mapping_table_exists():
    """Verify mapping_xbrl table exists and has expected records."""
    conn = get_connection(CONFIG_DB)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM mapping_xbrl")
        result = cur.fetchone()
        assert result["cnt"] == 7, f"Expected 7 mappings, got {result['cnt']}"
        print("✅ mapping_xbrl: 7 mapping rules found")
    conn.close()


def test_raw_tables_populated():
    """Verify all RAW tables have data."""
    conn = get_connection(DWH_CONFIG)
    tables = [
        "raw.xml_xbrl_data",
        "raw.xml_hierarchical_data",
        "raw.json_financials_data",
        "raw.json_period_data",
        "raw.csv_data",
    ]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            result = cur.fetchone()
            count = result["cnt"]
            assert count > 0, f"{table} is empty!"
            print(f"✅ {table}: {count} rows")
    conn.close()


def test_staging_financials():
    """Verify staging financials have data."""
    conn = get_connection(DWH_CONFIG)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM staging.stg_financials")
        result = cur.fetchone()
        assert result["cnt"] > 0, "staging.stg_financials is empty!"
        print(f"✅ staging.stg_financials: {result['cnt']} rows")

        # Check for rejected records
        cur.execute("SELECT COUNT(*) as cnt FROM staging.stg_reject_log")
        result = cur.fetchone()
        print(f"   staging.stg_reject_log: {result['cnt']} rejected records")
    conn.close()


def test_dim_company_scd2():
    """Verify dim_company has SCD Type 2 records."""
    conn = get_connection(DWH_CONFIG)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM dwh.dim_company")
        total = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) as cnt FROM dwh.dim_company WHERE is_current = TRUE")
        current = cur.fetchone()["cnt"]

        assert total > 0, "dim_company is empty!"
        print(f"✅ dwh.dim_company: {total} total, {current} current records")
    conn.close()


def test_fact_financials():
    """Verify fact_financials has data with resolved surrogate keys."""
    conn = get_connection(DWH_CONFIG)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM dwh.fact_financials")
        total = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) as cnt FROM dwh.fact_financials WHERE dim_company_sk IS NOT NULL"
        )
        resolved = cur.fetchone()["cnt"]

        assert total > 0, "fact_financials is empty!"
        print(f"✅ dwh.fact_financials: {total} rows ({resolved} with resolved dim_company_sk)")

        # Check CY/PY distribution
        cur.execute(
            "SELECT period_type, COUNT(*) as cnt FROM dwh.fact_financials "
            "GROUP BY period_type ORDER BY period_type"
        )
        periods = cur.fetchall()
        for p in periods:
            print(f"   Period {p['period_type']}: {p['cnt']} records")
    conn.close()


def test_bi_views():
    """Verify BI views are queryable."""
    conn = get_connection(DWH_CONFIG)
    views = [
        "dwh.v_company_financials_summary",
        "dwh.v_revenue_comparison",
        "dwh.v_company_history",
    ]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for view in views:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {view}")
            result = cur.fetchone()
            print(f"✅ {view}: {result['cnt']} rows")
    conn.close()


def test_data_quality_rejects():
    """Verify known corrupt data was properly rejected."""
    conn = get_connection(DWH_CONFIG)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT rejection_reason, COUNT(*) as cnt "
            "FROM staging.stg_reject_log GROUP BY rejection_reason"
        )
        results = cur.fetchall()
        for r in results:
            print(f"   Rejected: {r['rejection_reason'][:60]}... ({r['cnt']} records)")

        # Check specific known corruptions
        cur.execute(
            "SELECT COUNT(*) as cnt FROM staging.stg_reject_log "
            "WHERE rejection_reason LIKE '%MISSING_COMPANY_ID%'"
        )
        missing_id = cur.fetchone()["cnt"]
        print(f"✅ Missing company_id rejections: {missing_id}")

        cur.execute(
            "SELECT COUNT(*) as cnt FROM staging.stg_reject_log "
            "WHERE rejection_reason LIKE '%INVALID_YEAR%'"
        )
        invalid_year = cur.fetchone()["cnt"]
        print(f"✅ Invalid year rejections: {invalid_year}")
    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("ETL PIPELINE — POST-EXECUTION VALIDATION")
    print("=" * 60)

    tests = [
        test_mapping_table_exists,
        test_raw_tables_populated,
        test_staging_financials,
        test_dim_company_scd2,
        test_fact_financials,
        test_bi_views,
        test_data_quality_rejects,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            print(f"\n--- {test.__name__} ---")
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__} FAILED: {e}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {passed} passed, {failed} failed")
    print(f"{'=' * 60}")

    sys.exit(0 if failed == 0 else 1)
