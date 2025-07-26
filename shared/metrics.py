from prometheus_client import Counter, Histogram

# Define metrics in a central place to avoid re-registration across processes.
# Python's module import system ensures this code runs only once.

MESSAGES_PROCESSED = Counter(
    "pipeline_messages_total",
    "Total messages processed by the pipeline",
    ["service", "status"]
)

PROCESSING_TIME = Histogram(
    "pipeline_processing_duration_seconds",
    "Time spent processing a message",
    ["service"]
)
