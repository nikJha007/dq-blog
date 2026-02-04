"""
Deequ Analyzer for Streaming ETL Framework.

Provides stateful data quality metrics using AWS Deequ library.
Supports completeness, uniqueness, compliance, and size analyzers
with state persistence to S3 and metrics storage in Delta Lake.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# Set SPARK_VERSION before importing pydeequ (REQUIRED)
os.environ['SPARK_VERSION'] = '3.3'

from pydeequ.analyzers import (
    AnalysisRunner,
    Completeness,
    Compliance,
    Size,
    Uniqueness,
)
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DeequAnalyzer")


# Schema for metrics DataFrame
METRICS_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("batch_id", LongType(), False),
    StructField("metric_name", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold", DoubleType(), True),
    StructField("passed", BooleanType(), False),
    StructField("timestamp", TimestampType(), False),
])


class DeequAnalyzer:
    """
    Stateful Deequ analyzer for streaming batches.
    
    Provides enterprise-grade data quality analysis with:
    - Stateful aggregation across batches
    - S3-based state persistence
    - Delta Lake metrics storage
    - Threshold-based alerting
    """
    
    def __init__(
        self,
        spark: SparkSession,
        state_path: str,
        metrics_path: str
    ):
        """
        Initialize Deequ analyzer.
        
        Args:
            spark: SparkSession instance
            state_path: S3 path for Deequ state persistence
            metrics_path: S3 path for metrics Delta table
        """
        self.spark = spark
        self.state_path = state_path.rstrip("/") + "/"
        self.metrics_path = metrics_path.rstrip("/") + "/"
        
        # Initialize FileSystemMetricsRepository for state persistence
        try:
            self.repository = FileSystemMetricsRepository(spark, self.state_path)
            logger.info("Initialized Deequ repository at: %s", self.state_path)
        except Exception as exc:
            logger.error("Failed to initialize Deequ repository: %s", exc)
            self.repository = None
    
    def analyze_batch(
        self,
        df: DataFrame,
        table_name: str,
        batch_id: int,
        checks: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Run Deequ analyzers on a batch and return metrics DataFrame.
        
        Args:
            df: Input DataFrame to analyze
            table_name: Name of the table being analyzed
            batch_id: Current batch identifier
            checks: List of check configurations
        
        Returns:
            DataFrame with metrics schema
        """
        if df.isEmpty():
            logger.info("[%s] Empty DataFrame, skipping Deequ analysis", table_name)
            return self._create_empty_metrics_df()
        
        if not checks:
            logger.info("[%s] No Deequ checks configured", table_name)
            return self._create_empty_metrics_df()
        
        logger.info("[%s] ========== DEEQU ANALYSIS START ==========", table_name)
        logger.info("[%s] Running %d Deequ checks on batch %d",
                    table_name, len(checks), batch_id)
        
        try:
            analysis_runner = self._build_analyzers(df, checks, table_name)
            
            if analysis_runner is None:
                logger.warning("[%s] No valid analyzers built", table_name)
                return self._create_empty_metrics_df()
            
            result_key = ResultKey(
                self.spark,
                ResultKey.current_milli_time(),
                {"table": table_name, "batch": str(batch_id)}
            )
            
            if self.repository:
                analysis_result = (
                    analysis_runner
                    .useRepository(self.repository)
                    .saveOrAppendResult(result_key)
                    .run()
                )
                logger.info("[%s] Analysis completed with state persistence", table_name)
            else:
                analysis_result = analysis_runner.run()
                logger.warning("[%s] Analysis completed WITHOUT state persistence", table_name)
            
            metrics_df = self._result_to_dataframe(
                analysis_result, table_name, batch_id, checks
            )
            
            self.check_thresholds(metrics_df, checks)
            
            logger.info("[%s] ========== DEEQU ANALYSIS END ==========", table_name)
            
            return metrics_df
            
        except Exception as exc:
            logger.error("[%s] Deequ analysis failed: %s", table_name, exc)
            return self._create_empty_metrics_df()
    
    def _build_analyzers(
        self,
        df: DataFrame,
        checks: List[Dict[str, Any]],
        table_name: str
    ) -> Optional[AnalysisRunner]:
        """Build Deequ analyzers from configuration."""
        analysis_runner = AnalysisRunner(self.spark).onData(df)
        valid_analyzers = 0
        
        for check in checks:
            metric = check.get("metric", "").lower()
            column = check.get("column")
            
            try:
                if metric == "completeness":
                    if not column or column not in df.columns:
                        continue
                    analysis_runner = analysis_runner.addAnalyzer(Completeness(column))
                    valid_analyzers += 1
                    
                elif metric == "uniqueness":
                    if not column or column not in df.columns:
                        continue
                    analysis_runner = analysis_runner.addAnalyzer(Uniqueness([column]))
                    valid_analyzers += 1
                    
                elif metric == "compliance":
                    if not column or column not in df.columns:
                        continue
                    pattern = check.get("pattern", ".*")
                    constraint_name = f"{column}_compliance"
                    expression = f"{column} RLIKE '{pattern}'"
                    if "expression" in check:
                        expression = check["expression"]
                        constraint_name = check.get("name", f"{column}_custom")
                    analysis_runner = analysis_runner.addAnalyzer(
                        Compliance(constraint_name, expression)
                    )
                    valid_analyzers += 1
                    
                elif metric == "size":
                    analysis_runner = analysis_runner.addAnalyzer(Size())
                    valid_analyzers += 1
                    
            except Exception as exc:
                logger.error("[%s] Failed to add analyzer for %s: %s",
                             table_name, metric, exc)
        
        return analysis_runner if valid_analyzers > 0 else None
    
    def _result_to_dataframe(
        self,
        result,
        table_name: str,
        batch_id: int,
        checks: List[Dict[str, Any]]
    ) -> DataFrame:
        """Convert Deequ AnalysisResult to metrics DataFrame."""
        metrics_rows = []
        current_time = datetime.now()
        
        try:
            # Get the metrics DataFrame directly from Deequ result
            # This is more reliable than iterating over metricMap()
            metrics_df = AnalysisRunner.successMetricsAsDataFrame(
                self.spark, result
            )
            
            if metrics_df and not metrics_df.isEmpty():
                for row in metrics_df.collect():
                    try:
                        # Extract fields from Deequ's metrics DataFrame
                        # Standard columns: entity, instance, name, value
                        metric_name = row.name if hasattr(row, 'name') else str(row[2])
                        column_name = row.instance if hasattr(row, 'instance') else str(row[1])
                        metric_value = float(row.value) if hasattr(row, 'value') else float(row[3])
                        
                        # Clean up column name (Deequ uses "*" for table-level metrics)
                        if column_name == "*":
                            column_name = None
                        
                        threshold = self._get_threshold_for_metric(checks, metric_name, column_name)
                        
                        passed = True
                        if threshold is not None and metric_value is not None:
                            passed = metric_value >= threshold
                        
                        metrics_rows.append({
                            "table_name": table_name,
                            "batch_id": int(batch_id),
                            "metric_name": metric_name,
                            "column_name": column_name,
                            "metric_value": metric_value,
                            "threshold": threshold,
                            "passed": passed,
                            "timestamp": current_time,
                        })
                        
                        logger.info(
                            "[%s] Metric: %s.%s = %.4f (threshold: %s, passed: %s)",
                            table_name, metric_name, column_name or "N/A",
                            metric_value if metric_value else 0.0, threshold, passed
                        )
                        
                    except Exception as exc:
                        logger.warning("[%s] Failed to process metric row: %s", table_name, exc)
            else:
                logger.info("[%s] No metrics returned from Deequ analysis", table_name)
                    
        except Exception as exc:
            logger.warning("[%s] Could not extract metrics via DataFrame: %s", table_name, exc)
            # Fallback: try to get basic info from result
            try:
                # Just log that analysis completed without detailed metrics
                logger.info("[%s] Deequ analysis completed (metrics extraction skipped)", table_name)
            except Exception:
                pass
        
        if not metrics_rows:
            return self._create_empty_metrics_df()
        
        return self.spark.createDataFrame(metrics_rows, METRICS_SCHEMA)
    
    def _get_metric_name(self, analyzer) -> str:
        """Extract metric name from analyzer."""
        analyzer_str = str(analyzer)
        if "Completeness" in analyzer_str:
            return "Completeness"
        elif "Uniqueness" in analyzer_str:
            return "Uniqueness"
        elif "Compliance" in analyzer_str:
            return "Compliance"
        elif "Size" in analyzer_str:
            return "Size"
        return type(analyzer).__name__
    
    def _get_column_name(self, analyzer) -> Optional[str]:
        """Extract column name from analyzer."""
        try:
            if hasattr(analyzer, 'column'):
                return str(analyzer.column)
            if hasattr(analyzer, 'instance'):
                instance = analyzer.instance
                if instance and str(instance) != "*":
                    return str(instance)
            if hasattr(analyzer, 'columns'):
                cols = analyzer.columns
                if cols:
                    if hasattr(cols, 'head'):
                        return str(cols.head())
                    elif isinstance(cols, (list, tuple)) and len(cols) > 0:
                        return str(cols[0])
        except Exception:
            pass
        return None
    
    def _get_threshold_for_metric(
        self,
        checks: List[Dict[str, Any]],
        metric_name: str,
        column_name: Optional[str]
    ) -> Optional[float]:
        """Find threshold for a metric from checks configuration."""
        for check in checks:
            check_metric = check.get("metric", "").lower()
            check_column = check.get("column")
            
            if check_metric.lower() == metric_name.lower():
                if column_name and check_column:
                    if check_column == column_name:
                        return check.get("threshold")
                elif not column_name or not check_column:
                    return check.get("threshold")
        return None
    
    def check_thresholds(
        self,
        metrics_df: DataFrame,
        checks: List[Dict[str, Any]]
    ) -> None:
        """Log warnings/errors for threshold breaches."""
        if metrics_df.isEmpty():
            return
        
        try:
            for row in metrics_df.collect():
                if not row.passed:
                    severity = self._get_severity_for_metric(
                        checks, row.metric_name, row.column_name
                    )
                    column_info = f".{row.column_name}" if row.column_name else ""
                    metric_value = row.metric_value if row.metric_value else 0.0
                    threshold = row.threshold if row.threshold else "N/A"
                    
                    message = (
                        f"DQ THRESHOLD BREACH: {row.table_name}{column_info} "
                        f"{row.metric_name}={metric_value:.4f} < {threshold}"
                    )
                    
                    if severity == "error":
                        logger.error(message)
                    else:
                        logger.warning(message)
                        
        except Exception as exc:
            logger.error("Failed to check thresholds: %s", exc)
    
    def _get_severity_for_metric(
        self,
        checks: List[Dict[str, Any]],
        metric_name: str,
        column_name: Optional[str]
    ) -> str:
        """Find severity level for a metric from checks configuration."""
        for check in checks:
            check_metric = check.get("metric", "").lower()
            check_column = check.get("column")
            
            if check_metric.lower() == metric_name.lower():
                if column_name and check_column:
                    if check_column == column_name:
                        return check.get("severity", "warning")
                elif not column_name or not check_column:
                    return check.get("severity", "warning")
        return "warning"
    
    def write_metrics(self, metrics_df: DataFrame) -> None:
        """Write metrics to Delta Lake table."""
        if metrics_df.isEmpty():
            logger.info("No metrics to write")
            return
        
        try:
            metrics_with_ts = metrics_df.withColumn("_ingested_at", current_timestamp())
            
            metrics_with_ts.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("table_name") \
                .save(self.metrics_path)
            
            count = metrics_df.count()
            logger.info("Wrote %d metrics to Delta table: %s", count, self.metrics_path)
            
        except Exception as exc:
            logger.error("Failed to write metrics to Delta: %s", exc)
            try:
                fallback_path = self.metrics_path.replace("/delta/", "/parquet/")
                metrics_df.withColumn("_ingested_at", current_timestamp()) \
                    .write.mode("append").parquet(fallback_path)
                logger.warning("Wrote metrics to fallback Parquet: %s", fallback_path)
            except Exception as fallback_exc:
                logger.error("Fallback write also failed: %s", fallback_exc)
    
    def _create_empty_metrics_df(self) -> DataFrame:
        """Create an empty DataFrame with metrics schema."""
        return self.spark.createDataFrame([], METRICS_SCHEMA)


def create_deequ_analyzer(
    spark: SparkSession,
    state_path: str,
    metrics_path: str
) -> DeequAnalyzer:
    """Factory function to create a DeequAnalyzer instance."""
    return DeequAnalyzer(spark, state_path, metrics_path)
