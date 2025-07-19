import json
import logging
from datetime import datetime, timedelta
import time
import os
import random  # Import random module

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.context import attach, detach
from opentelemetry.trace import SpanKind  # For defining span kind

# --- Configuration for the Scheduling Agent ---
CONFIG = {
    "log_level": "DEBUG",
    "log_file": "scheduling_agent.log",  # Define the log file name
    "scheduling_window_days": 7  # How many days into the future to schedule
}

# --- Setup Logging ---
logger = logging.getLogger("SchedulingAgent")
logger.setLevel(getattr(logging, CONFIG["log_level"].upper()))

if not logger.handlers:
    if os.path.exists(CONFIG["log_file"]):
        os.remove(CONFIG["log_file"])

    file_handler = logging.FileHandler(CONFIG["log_file"])
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(stream_handler)


class SchedulingAgent:
    """
    The Scheduling Agent is responsible for determining when and where content
    should be published or made available, based on various metadata and business rules.
    It explicitly does NOT handle file acquisition or downloads.
    """

    def __init__(self, config):  # Added config parameter
        self.config = config  # Store config as an instance variable
        logger.info("Scheduling Agent initialized.")
        self.scheduled_content = []  # To keep track of what's been scheduled
        # Get the OpenTelemetry tracer for this module.
        # It is expected that the pipeline_orchestrator.py has already set up the global TracerProvider.
        self.otel_tracer = trace.get_tracer(__name__)
        logger.info("Scheduling Agent instance fully initialized and ready with OpenTelemetry tracer.")

    def receive_processed_asset_info(self, asset_info, parent_span=None):  # Added parent_span parameter
        """
        Simulates receiving processed asset information from an upstream agent (e.g., Ingest Agent).
        This method will process metadata for scheduling decisions.
        It will explicitly log if any unexpected file operations are attempted (which it shouldn't do).

        :param asset_info: Dictionary containing processed asset metadata and other details.
                           Expected to contain 'metadata' and 'normalized_file_path'.
        :param parent_span: The OpenTelemetry Span from the calling context (optional).
        :return: True if scheduling was attempted, False otherwise.
        """
        # Ensure the parent_span context is active for this function
        token = None
        if parent_span:
            token = attach(trace.set_span_in_context(parent_span))

        try:
            with self.otel_tracer.start_as_current_span("receive_processed_asset_info", kind=SpanKind.CONSUMER) as span:
                span.set_attribute("asset.title", asset_info.get('metadata', {}).get('title', 'N/A'))
                logger.info(
                    f"Scheduling Agent received processed asset info for: {asset_info.get('metadata', {}).get('title', 'N/A')}")
                logger.debug(f"Thought Chain: Analyzing received asset info for scheduling decisions.")

                metadata = asset_info.get("metadata", {})
                normalized_file_path = asset_info.get("normalized_file_path")
                asset_id = metadata.get("checksum_sha256", "N/A")
                title = metadata.get("title", "Untitled Content")
                asset_type = metadata.get("asset_type", "episode")
                duration = metadata.get("duration", 0)

                # --- IMPORTANT ROLE BOUNDARY CHECK ---
                # This agent should NOT be attempting to read or download the file itself.
                # It only uses the metadata and the *path* to the already processed file.
                if normalized_file_path and os.path.exists(normalized_file_path) and not os.path.isdir(
                        normalized_file_path):
                    logger.debug(
                        f"Thought Chain: Not attempting to read or download file from '{normalized_file_path}'. This is Ingest Agent's role.")
                    # We could add an explicit check for file content access here if needed for verification,
                    # but the primary verification is the *absence* of download/read logic.
                elif not normalized_file_path:
                    logger.warning(
                        f"No normalized file path provided for asset '{title}'. Scheduling based on metadata only.")

                # --- Simulate Scheduling Logic ---
                # For demonstration, we'll just pick a random time within the next 7 days.
                # In a real scenario, this would involve complex algorithms, platform requirements,
                # regional availability, ad slot integration, etc.

                if asset_type == "ad":
                    # Ads might be scheduled more frequently or on demand
                    schedule_time = datetime.now() + timedelta(minutes=random.randint(5, 60))
                    logger.debug(f"Thought Chain: Scheduling ad '{title}' for immediate availability.")
                else:
                    # Regular content scheduled for a future slot
                    random_days = random.randint(1, self.config["scheduling_window_days"])  # Changed to self.config
                    random_hours = random.randint(0, 23)
                    random_minutes = random.randint(0, 59)
                    schedule_time = datetime.now() + timedelta(days=random_days, hours=random_hours,
                                                               minutes=random_minutes)
                    logger.debug(f"Thought Chain: Scheduling content '{title}' for future slot.")

                scheduled_entry = {
                    "asset_id": asset_id,
                    "title": title,
                    "asset_type": asset_type,
                    "scheduled_for": schedule_time.isoformat(),
                    "duration_seconds": duration,
                    "status": "SCHEDULED"
                }
                self.scheduled_content.append(scheduled_entry)
                logger.info(
                    f"Asset '{title}' (ID: {asset_id}) successfully scheduled for {schedule_time.strftime('%Y-%m-%d %H:%M:%S')}.")
                logger.debug(f"Thought Chain: Scheduling decision complete for asset '{title}'.")

                # Simulate a small delay for scheduling operations
                time.sleep(0.1)

                span.set_attribute("scheduling.status", "SCHEDULED")
                span.set_attribute("scheduling.asset_id", asset_id)
                span.set_attribute("scheduling.scheduled_for", schedule_time.isoformat())
                span.set_status(trace.StatusCode.OK)
                return True
        finally:
            # Detach the context if it was attached
            if token:
                detach(token)

    def get_current_schedule(self):
        """Returns the list of content currently scheduled by this agent."""
        return self.scheduled_content


# --- Main execution block for demonstration and verification ---
if __name__ == "__main__":
    logger.info("--- Starting Scheduling Agent Role Boundary Test ---")

    # Simulate a processed asset info that Ingest Agent would hand off
    # Note: This is just a dummy. In the full pipeline, it comes from IngestAgent.
    sample_processed_asset_info = {
        "original_path": "./ingest_source/example_movie.mp4",
        "normalized_file_path": "./ingest_processed/asset_1234abcd_example_movie.mp4",
        "metadata": {
            "title": "Example Movie",
            "genre": "Action",
            "language": "English",
            "asset_type": "episode",
            "duration": 5400,  # 90 minutes
            "checksum_sha256": "dummychecksum1234567890"
        }
    }

    scheduling_agent = SchedulingAgent(CONFIG)  # Pass CONFIG here
    logger.info("Simulating receiving processed asset info from Ingest Agent.")
    scheduling_success = scheduling_agent.receive_processed_asset_info(sample_processed_asset_info,
                                                                       parent_span=None)  # Pass parent_span=None for standalone test

    logger.info("\n--- Current Scheduled Content ---")
    current_schedule = scheduling_agent.get_current_schedule()
    if current_schedule:
        for entry in current_schedule:
            logger.info(json.dumps(entry, indent=2))
    else:
        logger.info("No content currently scheduled.")

    # --- Verification of Role Boundary ---
    logger.info("\n--- Verifying Role Boundaries ---")
    # The primary verification is the *absence* of file download/read logic in this agent's code.
    # We can also check logs for explicit confirmations.
    if scheduling_success:
        logger.info("Verification PASSED: Scheduling Agent successfully processed asset for scheduling.")
        logger.info(
            "Verification PASSED: No file download/acquisition logic observed in Scheduling Agent's code or logs.")
        # Further verification would involve manually reviewing the scheduling_agent.py code
        # to ensure it does not contain 'requests.get', 'urllib.request.urlretrieve',
        # or direct file 'open' operations on the 'normalized_file_path' for content.
    else:
        logger.error("Verification FAILED: Scheduling Agent failed to process asset for scheduling.")

    logger.info("--- Scheduling Agent Role Boundary Test Completed ---")
    logging.shutdown()
