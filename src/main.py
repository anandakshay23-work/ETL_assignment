"""
ETL Pipeline Orchestrator — Main entry point.

Coordinates the full pipeline flow:
    EXTRACT → RAW LOAD → QUALITY CHECK → TRANSFORM → STAGING LOAD → DWH LOAD

Usage:
    spark-submit src/main.py
"""

import os
import sys
import uuid
from datetime import datetime

from pyspark.sql import functions as F

# Ensure the src directory is in the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logger import setup_logger
from src.utils.spark_session import create_spark_session
from src.utils.db_connector import DBConnector
from src.config.config_manager import ConfigManager
from src.extractors.xml_extractor import XMLExtractor
from src.extractors.json_extractor import JSONExtractor
from src.extractors.csv_extractor import CSVExtractor
from src.transformers.mapping_engine import MappingEngine
from src.transformers.data_quality import DataQualityEngine
from src.loaders.raw_loader import RawLoader
from src.loaders.staging_loader import StagingLoader
from src.loaders.dwh_loader import DWHLoader


def run_pipeline():
    """
    Execute the full ETL pipeline.
    
    Flow:
    1. Initialize (Spark, Config, DB connections)
    2. Extract all sources
    3. Load to RAW (as-is)
    4. Quality check + reject handling
    5. Apply dynamic mapping transformations
    6. Load to STAGING (cleaned + unified)
    7. Load to DWH (SCD2 dimensions + facts)
    8. Report summary
    """

    # ─────────────────────────────────────────────────────────
    # PHASE 0: INITIALIZATION
    # ─────────────────────────────────────────────────────────
    log_dir = os.environ.get("LOG_DIR", "/app/logs")
    logger = setup_logger("etl_pipeline", log_dir)
    logger.info("=" * 70)
    logger.info("ETL PIPELINE STARTED")
    logger.info("=" * 70)

    batch_id = f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logger.info(f"Batch ID: {batch_id}")

    start_time = datetime.now()

    # Initialize Spark
    spark = create_spark_session("ETL_Financial_Pipeline")
    logger.info("SparkSession initialized")

    # Load config
    config_manager = ConfigManager(logger=logger)
    config_manager.load_config()

    # Fetch dynamic mappings from config DB
    mapping_dict = config_manager.get_mapping_dict()
    currency_rates = config_manager.currency_rates

    # Initialize DB connectors
    dwh_connector = DBConnector(config_manager.dwh_db_config, logger)

    # Initialize components
    mapping_engine = MappingEngine(mapping_dict, currency_rates, logger)
    dq_engine = DataQualityEngine(
        config_manager.pipeline_config.get("reject_path", "/app/data/reject"),
        logger
    )
    raw_loader = RawLoader(dwh_connector, logger)
    staging_loader = StagingLoader(dwh_connector, logger)
    dwh_loader = DWHLoader(dwh_connector, spark, logger)

    # Track pipeline metadata
    config_db = config_manager.get_config_db()
    try:
        config_db.execute_command(
            "INSERT INTO pipeline_metadata (run_id, start_time, status, source_files) "
            "VALUES (%s, %s, 'RUNNING', %s)",
            (batch_id, start_time, str([s["name"] for s in config_manager.sources]))
        )
    except Exception as e:
        logger.warning(f"Could not write pipeline metadata: {e}")

    total_records = 0
    total_rejected = 0

    # Collect all staging data for unified DWH load
    all_staging_financials = []
    all_staging_companies = []

    try:
        # ─────────────────────────────────────────────────────────
        # PHASE 1: EXTRACT + RAW LOAD + QUALITY + TRANSFORM + STAGE
        # Process each source file independently
        # ─────────────────────────────────────────────────────────

        for source_config in config_manager.sources:
            source_name = source_config["name"]
            source_type = source_config["type"]

            logger.info("─" * 60)
            logger.info(f"PROCESSING SOURCE: {source_name} (type={source_type})")
            logger.info("─" * 60)

            try:
                # ── EXTRACT ──
                if source_type == "xml":
                    extractor = XMLExtractor(spark, source_config, logger)
                    extracted_df = extractor.extract()

                    # Load to appropriate RAW table
                    if source_name == "xbrl_xml":
                        raw_loader.load(extracted_df, "raw.xml_xbrl_data", batch_id)
                    else:
                        raw_loader.load(extracted_df, "raw.xml_hierarchical_data", batch_id)

                elif source_type == "json":
                    extractor = JSONExtractor(spark, source_config, logger)
                    extracted_df = extractor.extract()

                    if source_name == "financials_json":
                        raw_loader.load(extracted_df, "raw.json_financials_data", batch_id)
                    else:
                        raw_loader.load(extracted_df, "raw.json_period_data", batch_id)

                elif source_type == "csv":
                    extractor = CSVExtractor(spark, source_config, logger)
                    extracted_df, metadata_df = extractor.extract()

                    raw_loader.load(extracted_df, "raw.csv_data", batch_id)
                    raw_loader.load_csv_metadata(metadata_df, batch_id)

                else:
                    logger.error(f"Unknown source type: {source_type}")
                    continue

                # ── DATA QUALITY ──
                if source_type == "csv":
                    valid_df, rejected_df = dq_engine.validate(extracted_df, source_name)
                    valid_df = dq_engine.remove_duplicates(
                        valid_df, ["company_id", "year"], source_name
                    )
                else:
                    valid_df, rejected_df = dq_engine.validate(extracted_df, source_name)

                # Log rejected records to staging
                staging_loader.load_reject_log(rejected_df, source_name, batch_id)

                rejected_count = rejected_df.count()
                total_rejected += rejected_count

                # ── TRANSFORM (Apply dynamic mappings) ──
                transformed_df = _apply_source_transformations(
                    valid_df, source_name, mapping_engine, logger
                )

                # ── LOAD TO STAGING ──
                stg_count = staging_loader.load_financials(transformed_df, batch_id)
                total_records += stg_count

                # Track company data for dimension loading
                if "company_name" in transformed_df.columns:
                    staging_loader.load_companies(transformed_df, batch_id)
                    all_staging_companies.append(
                        transformed_df.select("company_id", "company_name")
                        .filter(F.col("company_id").isNotNull())
                        .dropDuplicates(["company_id"])
                    )

                all_staging_financials.append(transformed_df)

                logger.info(f"✅ {source_name} complete: {stg_count} staged, {rejected_count} rejected")

            except Exception as e:
                logger.error(f"❌ Failed to process {source_name}: {e}", exc_info=True)
                continue

        # ─────────────────────────────────────────────────────────
        # PHASE 2: DWH LOAD (Dimensions + Facts)
        # ─────────────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("PHASE 2: LOADING TO DATA WAREHOUSE")
        logger.info("=" * 60)

        # Load dim_company (SCD Type 2)
        if all_staging_companies:
            # Union all company DataFrames
            companies_union = all_staging_companies[0]
            for df in all_staging_companies[1:]:
                companies_union = companies_union.unionByName(df, allowMissingColumns=True)
            companies_union = companies_union.dropDuplicates(["company_id"])

            scd_stats = dwh_loader.load_dimension_company(companies_union, batch_id)
        else:
            logger.warning("No company data to load to dim_company")

        # Load fact_financials
        if all_staging_financials:
            # Union all financial DataFrames
            financials_union = all_staging_financials[0]
            for df in all_staging_financials[1:]:
                financials_union = financials_union.unionByName(df, allowMissingColumns=True)

            fact_count = dwh_loader.load_fact_financials(financials_union, batch_id)
            logger.info(f"Fact financials loaded: {fact_count} rows")
        else:
            logger.warning("No financial data to load to fact_financials")

        # ─────────────────────────────────────────────────────────
        # PHASE 3: SUMMARY & CLEANUP
        # ─────────────────────────────────────────────────────────
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        dq_engine.log_quality_summary()

        logger.info("=" * 70)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  Batch ID:         {batch_id}")
        logger.info(f"  Duration:         {duration:.1f} seconds")
        logger.info(f"  Records staged:   {total_records}")
        logger.info(f"  Records rejected: {total_rejected}")
        logger.info(f"  Sources processed: {len(config_manager.sources)}")
        logger.info("=" * 70)

        # Update pipeline metadata
        try:
            config_db.execute_command(
                "UPDATE pipeline_metadata SET end_time=%s, status='SUCCESS', "
                "records_processed=%s, records_rejected=%s WHERE run_id=%s",
                (end_time, total_records, total_rejected, batch_id)
            )
        except Exception as e:
            logger.warning(f"Could not update pipeline metadata: {e}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)

        # Update pipeline metadata with failure
        try:
            config_db.execute_command(
                "UPDATE pipeline_metadata SET end_time=%s, status='FAILED', "
                "error_message=%s WHERE run_id=%s",
                (datetime.now(), str(e), batch_id)
            )
        except Exception:
            pass

        raise

    finally:
        # Cleanup
        config_manager.close()
        dwh_connector.close()
        spark.stop()
        logger.info("Resources cleaned up. Pipeline shutdown complete.")


def _apply_source_transformations(df, source_name, mapping_engine, logger):
    """
    Apply source-specific column mappings before the generic mapping engine.
    
    Each source has different column names that need to be mapped
    to the standard names expected by the mapping engine.
    """

    if source_name == "xbrl_xml":
        # XBRL: company_id, period, revenue, profit
        # Map period → period_type
        result = df
        if "period" in df.columns:
            result = result.withColumnRenamed("period", "period_type")
        # Apply currency conversion (no currency column → treat as USD)
        result = result.withColumn("revenue_usd", F.col("revenue").cast("double"))
        result = result.withColumn("revenue", F.col("revenue").cast("double"))
        result = result.withColumn("profit", F.col("profit").cast("double"))

    elif source_name == "hierarchical_xml":
        # Hierarchical: company_id, company_name, revenue, year
        result = df
        result = mapping_engine.apply_transformations(
            result,
            source_columns_map={"Name": "company_name", "Revenue": "revenue", "Year": "year", "ID": "company_id"}
        )
        # Apply company name uppercase via mapping
        if "company_name" in result.columns:
            result = result.withColumn("company_name", F.upper(F.col("company_name")))
        result = result.withColumn("revenue_usd", F.col("revenue").cast("double"))
        result = result.withColumn("revenue", F.col("revenue").cast("double"))

    elif source_name == "financials_json":
        # Nested JSON: company_id, year, revenue, currency
        result = df
        # Apply currency conversion
        result = mapping_engine._transform_currency_convert(result, "revenue", "revenue_usd")
        result = result.withColumn("revenue", F.col("revenue").cast("double"))

    elif source_name == "period_json":
        # Multi-period: company_id, period, revenue
        result = df
        if "period" in df.columns:
            result = result.withColumnRenamed("period", "period_type")
        result = result.withColumn("revenue_usd", F.col("revenue").cast("double"))
        result = result.withColumn("revenue", F.col("revenue").cast("double"))

    elif source_name == "complex_csv":
        # CSV: company_id, profit, year, remarks, table_index
        result = df
        # Apply split_comma to profit
        result = mapping_engine._transform_split_comma(result, "profit", "profit")
        # Drop table_index and remarks for staging
        if "table_index" in result.columns:
            result = result.drop("table_index")
        if "remarks" in result.columns:
            result = result.drop("remarks")

    else:
        result = df
        logger.warning(f"No source-specific transformation for: {source_name}")

    return result


if __name__ == "__main__":
    run_pipeline()
