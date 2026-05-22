import logging
import time

logger = logging.getLogger("stream-monitor")


class BatchMetrics:
    """Tracks per-batch metrics for observability."""

    def __init__(self):
        self.total_batches = 0
        self.total_records = 0
        self.total_duplicates = 0
        self.total_dlq = 0
        self.start_time = time.time()

    def record_batch(self, batch_id: int, written: int, duplicates: int, dlq: int):
        self.total_batches += 1
        self.total_records += written
        self.total_duplicates += duplicates
        self.total_dlq += dlq

        elapsed = time.time() - self.start_time
        throughput = self.total_records / elapsed if elapsed > 0 else 0

        logger.info(
            f"[Batch {batch_id}] "
            f"written={written} | dup={duplicates} | dlq={dlq} | "
            f"total_records={self.total_records} | "
            f"avg_throughput={throughput:.1f} rec/s | "
            f"dup_rate={self.total_duplicates/max(1,self.total_records):.2%} | "
            f"dlq_rate={self.total_dlq/max(1,self.total_records):.2%}"
        )


# Global singleton used by bronze_writer
METRICS = BatchMetrics()
