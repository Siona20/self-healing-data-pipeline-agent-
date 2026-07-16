"""
Data Pipeline Monitoring Module.

This module provides the PipelineMonitor class to track pipeline execution time,
rows processed, errors, and to generate metrics and a quality score placeholder.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

# Configure module-level logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PipelineMonitor:
    """
    Monitors pipeline execution, tracks row counts, and calculates performance metrics.
    """

    def __init__(self, pipeline_name: str = "default_pipeline") -> None:
        """
        Initialize the PipelineMonitor.

        Args:
            pipeline_name (str): The name of the pipeline being monitored.
        """
        self.pipeline_name = pipeline_name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.start_timestamp: Optional[str] = None
        self.end_timestamp: Optional[str] = None
        self.rows_processed: int = 0
        self.rows_failed: int = 0

    def start_pipeline(self) -> None:
        """Mark the start of the pipeline execution."""
        self.start_time = time.time()
        self.start_timestamp = datetime.utcnow().isoformat() + "Z"
        logger.info(f"Pipeline '{self.pipeline_name}' started at {self.start_timestamp}")

    def end_pipeline(self) -> None:
        """Mark the end of the pipeline execution."""
        if self.start_time is None:
            logger.warning("Pipeline ended without being started.")
            
        self.end_time = time.time()
        self.end_timestamp = datetime.utcnow().isoformat() + "Z"
        logger.info(f"Pipeline '{self.pipeline_name}' ended at {self.end_timestamp}")

    def record_rows_processed(self, count: int) -> None:
        """
        Record successfully processed rows.

        Args:
            count (int): Number of rows successfully processed.
        """
        if count < 0:
            raise ValueError("Row count cannot be negative.")
        self.rows_processed += count
        logger.debug(f"Recorded {count} rows processed. Total: {self.rows_processed}")

    def record_rows_failed(self, count: int) -> None:
        """
        Record rows that failed processing.

        Args:
            count (int): Number of failed rows.
        """
        if count < 0:
            raise ValueError("Failed row count cannot be negative.")
        self.rows_failed += count
        logger.debug(f"Recorded {count} rows failed. Total: {self.rows_failed}")

    def calculate_execution_time(self) -> float:
        """
        Calculate the total execution time of the pipeline in seconds.

        Returns:
            float: Execution time in seconds.
        """
        if self.start_time is None:
            return 0.0
        end = self.end_time if self.end_time is not None else time.time()
        return end - self.start_time

    def generate_metrics(
        self,
        validation_report: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Calculate and return pipeline metrics as a dictionary.

        When a *validation_report* dict (from ``ValidationReport.to_dict()``) is
        supplied, the quality score is derived from validation results:
        ``(passed_checks / total_checks) * 100``.  Otherwise the score falls
        back to the row-level success rate for backward compatibility.

        Args:
            validation_report (Optional[Dict[str, Any]]): The validation report
                dictionary produced by ``validate_dataset()``.

        Returns:
            Dict[str, Any]: A dictionary containing execution times, row counts,
                            success rate, and quality score.
        """
        execution_time = self.calculate_execution_time()

        total_rows = self.rows_processed + self.rows_failed
        success_rate = (self.rows_processed / total_rows) if total_rows > 0 else 1.0

        # Derive quality score from validation results when available
        if validation_report is not None:
            total_checks = validation_report.get("total_checks", 0)
            passed_checks = validation_report.get("passed_checks", 0)
            quality_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 100.0
        else:
            quality_score = min(100.0, max(0.0, success_rate * 100))

        metrics = {
            "pipeline_name": self.pipeline_name,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "execution_time_seconds": round(execution_time, 4),
            "rows_processed": self.rows_processed,
            "rows_failed": self.rows_failed,
            "total_rows": total_rows,
            "success_rate": round(success_rate, 4),
            "quality_score": round(quality_score, 2),
        }

        logger.info(f"Generated metrics for pipeline '{self.pipeline_name}'")
        return metrics


if __name__ == "__main__":
    import json

    # Example Usage
    logger.info("Running monitoring example...")
    
    monitor = PipelineMonitor(pipeline_name="customer_data_pipeline")
    
    # 1. Start the pipeline
    monitor.start_pipeline()
    
    # Simulate processing time
    time.sleep(1.5)
    
    # 2. Record rows
    monitor.record_rows_processed(8500)
    monitor.record_rows_failed(150)
    
    # Simulate some more processing
    time.sleep(0.5)
    
    # Processed another batch
    monitor.record_rows_processed(1350)
    
    # 3. End the pipeline
    monitor.end_pipeline()
    
    # 4. Generate metrics
    final_metrics = monitor.generate_metrics()
    
    print("\nPipeline Monitoring Report:")
    print(json.dumps(final_metrics, indent=4))
