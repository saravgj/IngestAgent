import os
import json
import time
import logging
from datetime import datetime

from langfuse import Langfuse

langfuse = Langfuse(
  secret_key="sk-lf-ad126451-35c2-438b-9ae6-72b5e5376270",
  public_key="pk-lf-b9a5480d-3e5e-4330-924c-eeda46ea1db6",
  host="https://us.cloud.langfuse.com"
)

import os
import base64

public = "pk-lf-b9a5480d-3e5e-4330-924c-eeda46ea1db6"
secret = "sk-lf-ad126451-35c2-438b-9ae6-72b5e5376270"
auth_string = f"{public}:{secret}"
LANGFUSE_AUTH = base64.b64encode(auth_string.encode()).decode()

os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://otlp.us.cloud.langfuse.com"
os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"Authorization=Basic {LANGFUSE_AUTH}"
os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"


# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# Import agents and their configurations
from ingest_agent import IngestAgent, CONFIG as INGEST_CONFIG
from qc_agent import QCAgent, CONFIG as QC_CONFIG
from graphics_agent import GraphicsAgent  # Assuming GraphicsAgent doesn't have its own config needed here
from scheduling_agent import SchedulingAgent, CONFIG as SCHEDULING_CONFIG

# --- Setup Logging for Orchestrator ---
LOG_FILE = "pipeline_orchestrator.log"
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

logger = logging.getLogger("PipelineOrchestrator")
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all messages

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

# --- OpenTelemetry Global Setup ---
# This block configures the global TracerProvider for the entire pipeline.
# All agents will then retrieve this global tracer.
langfuse_public_key = os.environ.get("LANGFUSE_PUBLIC_KEY")
langfuse_secret_key = os.environ.get("LANGFUSE_SECRET_KEY")
langfuse_host = os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com")

if not langfuse_public_key or not langfuse_secret_key:
    logger.warning(
        "Langfuse API keys not found in environment variables. OpenTelemetry traces will not be sent to Langfuse.")
    provider = TracerProvider()
else:
    logger.info("Setting up OTLPSpanExporter for Langfuse in Orchestrator.")
    # Define a resource for your service, which will be attached to all spans
    resource = Resource.create({
        "service.name": "cloudport-media-pipeline",
        "service.version": "1.0.0",
        "deployment.environment": "development"
    })

    exporter = OTLPSpanExporter(
        endpoint=f"{langfuse_host}/v1/traces",
        headers={
            "X-Langfuse-Public-Key": langfuse_public_key,
            "X-Langfuse-Secret-Key": langfuse_secret_key
        },
    )
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

trace.set_tracer_provider(provider)
orchestrator_tracer = trace.get_tracer(__name__)
logger.info("OpenTelemetry TracerProvider globally configured by Orchestrator.")


# --- End OpenTelemetry Global Setup ---


class PipelineOrchestrator:
    """
    The Pipeline Orchestrator coordinates the flow of media assets
    through various agents in the Cloudport system.
    It manages the lifecycle of an asset from ingestion to final scheduling.
    """

    def __init__(self):
        self.ingest_agent = IngestAgent(INGEST_CONFIG)
        self.qc_agent = QCAgent(QC_CONFIG)
        self.graphics_agent = GraphicsAgent()
        self.scheduling_agent = SchedulingAgent(SCHEDULING_CONFIG)  # Pass config to SchedulingAgent
        logger.info("Pipeline Orchestrator initialized with all agents.")
        self.total_pipeline_runs = 0
        self.successful_pipeline_runs = 0

    def _cleanup_directories(self):
        """Cleans up ingest_source and ingest_processed directories."""
        for folder in [INGEST_CONFIG["watch_folders"][0], INGEST_CONFIG["output_folder"]]:
            if os.path.exists(folder):
                for f in os.listdir(folder):
                    os.remove(os.path.join(folder, f))
                os.rmdir(folder)  # Remove directory after emptying
                logger.info(f"Cleaned up directory: {folder}")
            os.makedirs(folder, exist_ok=True)  # Recreate empty directories
            logger.info(f"Recreated empty directory: {folder}")

    def run_pipeline_for_asset(self, asset_path):
        """
        Runs a single asset through the entire pipeline.
        Each full pipeline run is a new trace.
        """
        self.total_pipeline_runs += 1
        logger.info(f"\n--- Orchestrating pipeline for asset: {asset_path} ---")

        # Start a new trace for this entire pipeline run
        with orchestrator_tracer.start_as_current_span("pipeline_run", kind=trace.SpanKind.SERVER) as pipeline_span:
            pipeline_span.set_attribute("asset.original_path", asset_path)
            logger.debug(f"Thought Chain: Starting new pipeline run trace for {asset_path}.")

            processed_ingest_info = None
            qc_result = None

            try:
                # Step 1: Ingest Agent processing
                logger.info(f"Orchestrator calling Ingest Agent for {asset_path}...")
                # Pass the current pipeline_span as the parent
                processed_ingest_info = self.ingest_agent.ingest_asset_pipeline(asset_path, parent_span=pipeline_span)
                logger.info(
                    f"Ingest Agent finished for {asset_path}. Status: {processed_ingest_info.get('status', 'N/A')}")
                pipeline_span.add_event("ingest_completed", {"status": processed_ingest_info.get('status', 'N/A')})

                if processed_ingest_info.get("status") in ["FAILED_FORMAT_VALIDATION", "FAILED_NORMALIZATION",
                                                           "DUPLICATE_SKIPPED", "CRITICAL_FAILURE"]:
                    logger.warning(f"Ingest pipeline failed or skipped for {asset_path}. Skipping QC and Graphics.")
                    pipeline_span.set_status(trace.StatusCode.ERROR,
                                             description=f"Ingest failed or skipped: {processed_ingest_info.get('status')}")
                    return False  # Stop pipeline if ingest failed/skipped

                # Step 2: QC Agent processing
                logger.info(f"Orchestrator calling QC Agent for {asset_path}...")
                # Pass the current pipeline_span as the parent
                qc_result = self.qc_agent.run_qc_checks(processed_ingest_info, parent_span=pipeline_span)
                logger.info(f"QC Agent finished for {asset_path}. Status: {qc_result.get('qc_status', 'N/A')}")
                pipeline_span.add_event("qc_completed", {"status": qc_result.get('qc_status', 'N/A')})

                # Merge QC results into the processed_ingest_info for downstream agents
                processed_ingest_info["metadata"].update({
                    "qc_status": qc_result.get("qc_status"),
                    "qc_flags": qc_result.get("qc_flags"),
                    "qc_timestamp": qc_result.get("qc_timestamp")
                })

                if qc_result.get("qc_status") == "FAIL":
                    logger.warning(
                        f"QC failed for {asset_path}. Graphics and Scheduling might still proceed depending on flags.")
                    # Do not return False, let it proceed to graphics/scheduling with flags
                    pipeline_span.set_status(trace.StatusCode.ERROR, description="QC failed.")

                # Step 3: Graphics Agent processing (if not a duplicate or critical failure)
                logger.info(f"Orchestrator calling Graphics Agent for {asset_path}...")
                # The GraphicsAgent's receive_metadata expects the full message payload
                graphics_message_payload = {
                    "event_type": "METADATA_ENRICHED_FOR_GRAPHICS",
                    "timestamp": datetime.now().isoformat(),
                    "asset_id": processed_ingest_info["metadata"].get("checksum_sha256", "N/A"),
                    "metadata_payload": processed_ingest_info["metadata"]  # Pass the updated metadata
                }
                # Graphics Agent doesn't directly take a parent_span in its public method,
                # but its internal methods should retrieve the current span context.
                graphics_success = self.graphics_agent.receive_metadata(graphics_message_payload)
                logger.info(f"Graphics Agent finished for {asset_path}. Success: {graphics_success}")
                pipeline_span.add_event("graphics_completed", {"success": graphics_success})

                # Step 4: Scheduling Agent processing
                logger.info(f"Orchestrator calling Scheduling Agent for {asset_path}...")
                # Scheduling Agent also doesn't directly take a parent_span,
                # but its internal methods should retrieve the current span context.
                scheduling_success = self.scheduling_agent.receive_processed_asset_info(processed_ingest_info)
                logger.info(f"Scheduling Agent finished for {asset_path}. Success: {scheduling_success}")
                pipeline_span.add_event("scheduling_completed", {"success": scheduling_success})

                self.successful_pipeline_runs += 1
                pipeline_span.set_status(trace.StatusCode.OK)
                logger.info(f"--- Pipeline successfully completed for asset: {asset_path} ---")
                return True

            except Exception as e:
                logger.critical(f"Critical error during pipeline orchestration for {asset_path}: {e}", exc_info=True)
                pipeline_span.set_status(trace.StatusCode.ERROR, description=f"Orchestration failed: {e}")
                pipeline_span.record_exception(e)
                return False
            finally:
                # Ensure all spans are flushed at the end of the pipeline run
                # This is important for ensuring traces are sent to Langfuse
                provider.force_flush()

    def run_full_test_scenario(self):
        """
        Runs a predefined set of test assets through the pipeline.
        """
        self._cleanup_directories()
        logger.info("\n--- Starting Full Pipeline Test Scenario ---")

        # Create dummy files for testing
        # This part is typically handled by a separate script like create_dummy_files.py
        # For a self-contained orchestrator, we can include simplified creation here.
        # Ensure ingest_source directory exists
        os.makedirs(INGEST_CONFIG["watch_folders"][0], exist_ok=True)

        test_assets = [
            # Valid asset
            {"name": "my_episode_s01e01.mp4",
             "metadata": {"title": "My First Episode", "genre": "Sci-Fi", "language": "English", "ei_rating": "E"},
             "size_mb": 20},
            # Unsupported format
            {"name": "bad_format_video.avi", "metadata": {}, "size_mb": 1},
            # Long ad (simulated duration exceeds limit)
            {"name": "long_ad_campaign_x.mp4",
             "metadata": {"title": "Long Ad Campaign X", "genre": "Commercial", "language": "English",
                          "asset_type": "ad"}, "size_mb": 101},
            # Missing metadata
            {"name": "ad_missing_meta.mp4", "metadata": {}, "size_mb": 5},
            # Corrupted header (simulated FFprobe failure)
            {"name": "corrupted_header.mp4", "metadata": {}, "size_mb": 0.05},
            # Oversized file (simulated size exceeds limit)
            {"name": "oversized_file_test.mp4", "metadata": {}, "size_mb": 1},  # Small actual size, but simulated large
            # Duplicate content (will be skipped after first one is processed)
            {"name": "duplicate_content_video.mp4",
             "metadata": {"title": "Duplicate Content", "genre": "Test", "language": "English"}, "size_mb": 10},
            {"name": "another_copy_of_duplicate.mp4",
             "metadata": {"title": "Duplicate Content", "genre": "Test", "language": "English"}, "size_mb": 10},
        ]

        # Create dummy files
        for asset_data in test_assets:
            file_path = os.path.join(INGEST_CONFIG["watch_folders"][0], asset_data["name"])
            with open(file_path, "wb") as f:
                # Write content based on simulated size
                f.write(b"A" * int(asset_data["size_mb"] * 1024 * 1024))

            # Create sidecar JSON if metadata is provided
            if asset_data["metadata"]:
                sidecar_path = os.path.splitext(file_path)[0] + ".json"
                with open(sidecar_path, "w") as f:
                    json.dump(asset_data["metadata"], f, indent=2)
            logger.info(f"Created dummy file: {asset_data['name']} with size {asset_data['size_mb']}MB")

        logger.info("All dummy files created for test scenario.")
        time.sleep(1)  # Give a moment for file system to settle

        # Run each asset through the pipeline
        for asset_data in test_assets:
            asset_path = os.path.join(INGEST_CONFIG["watch_folders"][0], asset_data["name"])
            self.run_pipeline_for_asset(asset_path)
            time.sleep(0.5)  # Small delay between processing assets

        logger.info("\n--- Full Pipeline Test Scenario Completed ---")
        logger.info(f"Total pipeline runs: {self.total_pipeline_runs}")
        logger.info(f"Successful pipeline runs: {self.successful_pipeline_runs}")
        logger.info("Check Langfuse dashboard for detailed traces.")


if __name__ == "__main__":
    orchestrator = PipelineOrchestrator()
    orchestrator.run_full_test_scenario()

    # Ensure all spans are flushed before exiting the program
    trace.get_tracer_provider().force_flush()
    logging.shutdown()  # Properly shut down logging
