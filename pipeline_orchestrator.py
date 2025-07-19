import os
import asyncio
import logging
from datetime import datetime
import time

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.propagate import set_global_textmap
# CORRECTED IMPORT PATH: TraceContextTextMapPropagator is in opentelemetry.sdk.trace.propagation
#from opentelemetry.sdk.trace.propagation import TraceContextTextMapPropagator
#from opentelemetry.propagators.tracecontext import TraceContextTextMapPropagator
# OLD (no longer valid in your installed version)
# from opentelemetry.propagators.tracecontext import TraceContextTextMapPropagator

# ✅ NEW (for OpenTelemetry 1.35.0)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

exporter = OTLPSpanExporter(
    endpoint="http://localhost:4318/v1/traces",
    timeout=5,
)


# --- Agent Imports ---
from ingest_agent import IngestAgent, CONFIG as INGEST_CONFIG
from qc_agent import QCAgent, CONFIG as QC_CONFIG
from graphics_agent import GraphicsAgent, CONFIG as GRAPHICS_CONFIG
from scheduling_agent import SchedulingAgent, CONFIG as SCHEDULING_CONFIG

# --- Configuration for the Orchestrator ---
ORCHESTRATOR_CONFIG = {
    "log_level": "INFO",
    "log_file": "pipeline_orchestrator.log",
    "otel_endpoint": "http://localhost:4318/v1/traces",  # Default OTLP HTTP endpoint
    "service_name": "cloudport-pipeline-orchestrator",
    "environment": "development",
    "langfuse_enabled": False  # Set to True to enable Langfuse integration
}

# --- Set your Gemini API Key here ---
# IMPORTANT: Replace "YOUR_GEMINI_API_KEY_HERE" with your actual Gemini API Key.
# It is highly recommended to set this as an environment variable for production.
os.environ["GOOGLE_API_KEY"] = "YOUR_GEMINI_API_KEY_HERE"
# --- End Gemini API Key setup ---


# --- Setup Logging ---
logger = logging.getLogger("PipelineOrchestrator")
logger.setLevel(getattr(logging, ORCHESTRATOR_CONFIG["log_level"].upper()))

if not logger.handlers:
    if os.path.exists(ORCHESTRATOR_CONFIG["log_file"]):
        os.remove(ORCHESTRATOR_CONFIG["log_file"])

    file_handler = logging.FileHandler(ORCHESTRATOR_CONFIG["log_file"])
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(stream_handler)


# --- OpenTelemetry Setup ---
def setup_opentelemetry():
    """Sets up the global OpenTelemetry TracerProvider and exporter."""
    resource = Resource.create({
        "service.name": ORCHESTRATOR_CONFIG["service_name"],
        "environment": ORCHESTRATOR_CONFIG["environment"],
    })

    provider = TracerProvider(resource=resource)

    # Configure OTLP HTTP exporter
    exporter = OTLPSpanExporter(endpoint=ORCHESTRATOR_CONFIG["otel_endpoint"])
    span_processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(span_processor)

    # Set the global TracerProvider
    trace.set_tracer_provider(provider)

    # Set the global propagator for context propagation
    set_global_textmap(TraceContextTextMapPropagator())

    logger.info(
        f"OpenTelemetry configured with service.name='{ORCHESTRATOR_CONFIG['service_name']}' and OTLP endpoint='{ORCHESTRATOR_CONFIG['otel_endpoint']}'.")


class PipelineOrchestrator:
    """
    The central orchestrator for the Cloudport media ingestion pipeline.
    It coordinates the activities of different agents (Ingest, QC, Graphics, Scheduling).
    This class demonstrates complex workflow management and inter-agent communication.
    """

    def __init__(self):
        self.ingest_agent = IngestAgent(INGEST_CONFIG)
        self.qc_agent = QCAgent(QC_CONFIG)
        self.graphics_agent = GraphicsAgent()
        self.scheduling_agent = SchedulingAgent(SCHEDULING_CONFIG)
        self.otel_tracer = trace.get_tracer(__name__)
        logger.info("Pipeline Orchestrator initialized with all agents.")

    async def run_pipeline_for_asset(self, asset_path):
        """
        Orchestrates the end-to-end pipeline for a single media asset.
        This is the main workflow that ties all agents together.
        """
        with self.otel_tracer.start_as_current_span(f"pipeline_run_for_{os.path.basename(asset_path)}") as parent_span:
            parent_span.set_attribute("asset.original_path", asset_path)
            logger.info(f"\n--- Orchestrating pipeline for asset: {asset_path} ---")
            logger.debug(f"Thought Chain: Starting orchestration for {asset_path}.")

            # --- Step 1: Ingest Asset ---
            logger.info("Orchestrator: Initiating Ingest Agent.")
            logger.debug("Thought Chain: Calling Ingest Agent's pipeline.")
            processed_ingest_info = await self.ingest_agent.ingest_asset_pipeline(asset_path, parent_span=parent_span)

            if processed_ingest_info["status"] in ["FAILED_FORMAT_VALIDATION", "FAILED_NORMALIZATION",
                                                   "CRITICAL_FAILURE", "DUPLICATE_SKIPPED"]:
                logger.error(
                    f"Orchestrator: Ingest Agent failed or skipped asset {asset_path} with status: {processed_ingest_info['status']}. Aborting pipeline.")
                parent_span.set_status(trace.StatusCode.ERROR,
                                       description=f"Ingest failed or skipped: {processed_ingest_info['status']}")
                return

            logger.info("Orchestrator: Ingest Agent completed successfully.")
            logger.info("Ragas Evaluation Completed: %s",
                        self.ingest_agent.kpi_tracker.get("ragas_score_avg", "N/A"))

            parent_span.add_event("ingest_completed", {"status": processed_ingest_info["status"]})

            # --- Step 2: Quality Control Checks ---
            logger.info("Orchestrator: Initiating QC Agent.")
            logger.debug("Thought Chain: Calling QC Agent to perform checks.")
            qc_results = await asyncio.to_thread(self.qc_agent.run_qc_checks, processed_ingest_info,
                                                 parent_span=parent_span)

            if qc_results.get("qc_status") == "FAIL":
                logger.error(
                    f"Orchestrator: QC Agent failed for asset {asset_path}. Flags: {qc_results.get('qc_flags')}. Aborting pipeline.")
                parent_span.set_status(trace.StatusCode.ERROR, description=f"QC failed: {qc_results.get('qc_flags')}")
                return

            logger.info("Orchestrator: QC Agent completed successfully (or with review flags).")
            parent_span.add_event("qc_completed", {"status": qc_results.get("qc_status")})

            # --- Step 3: Graphics Generation (Inter-Agent Communication) ---
            logger.info("Orchestrator: Initiating Graphics Agent via message passing.")
            logger.debug("Thought Chain: Preparing message for Graphics Agent.")
            graphics_message = await asyncio.to_thread(self.qc_agent.send_to_graphics_agent,
                                                       qc_results.get("metadata", {}), parent_span=parent_span)

            logger.debug("Thought Chain: Graphics Agent message prepared. Simulating reception.")
            graphics_processing_success = await asyncio.to_thread(self.graphics_agent.receive_metadata,
                                                                  graphics_message, parent_span=parent_span)

            if not graphics_processing_success:
                logger.error(f"Orchestrator: Graphics Agent failed to process asset {asset_path}. Aborting pipeline.")
                parent_span.set_status(trace.StatusCode.ERROR, description="Graphics processing failed.")
                return

            logger.info("Orchestrator: Graphics Agent completed successfully.")
            parent_span.add_event("graphics_completed", {"success": graphics_processing_success})

            # --- Step 4: Scheduling Decision ---
            logger.info("Orchestrator: Initiating Scheduling Agent.")
            logger.debug("Thought Chain: Calling Scheduling Agent to make decisions.")
            scheduling_success = await asyncio.to_thread(self.scheduling_agent.receive_processed_asset_info, qc_results,
                                                         parent_span=parent_span)

            if not scheduling_success:
                logger.error(f"Orchestrator: Scheduling Agent failed for asset {asset_path}. Aborting pipeline.")
                parent_span.set_status(trace.StatusCode.ERROR, description="Scheduling failed.")
                return

            logger.info("Orchestrator: Scheduling Agent completed successfully.")
            parent_span.add_event("scheduling_completed", {"success": scheduling_success})

            logger.info(f"--- Pipeline for asset {asset_path} completed successfully ---")
            parent_span.set_status(trace.StatusCode.OK)


# --- Main execution ---
async def main():
    setup_opentelemetry()
    orchestrator = PipelineOrchestrator()

    # Clean up previous runs' output and logs
    for folder in ["./ingest_source", "./ingest_processed", "./graphics_output"]:
        os.makedirs(folder, exist_ok=True)
        for f in os.listdir(folder):
            os.remove(os.path.join(folder, f))

    for log_file in [ORCHESTRATOR_CONFIG["log_file"], INGEST_CONFIG["log_file"],
                     QC_CONFIG["log_file"], GRAPHICS_CONFIG["log_file"],
                     SCHEDULING_CONFIG["log_file"]]:
        if os.path.exists(log_file):
            os.remove(log_file)

    logger.info("Cleaned up previous run's files and logs.")

    logger.info("Creating dummy ingest files...")

    import json
    INGEST_SOURCE_DIR = "./ingest_source"
    os.makedirs(INGEST_SOURCE_DIR, exist_ok=True)

    # File 1: Valid Media File with Sidecar Metadata
    file_name_1 = "my_episode_s01e01.mp4"
    sidecar_name_1 = "my_episode_s01e01.json"
    simulated_size_mb_1 = 20
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_1), "wb") as f:
        f.write(b"This is a dummy video file content for a valid episode." * (1024 * 1024 * simulated_size_mb_1 // 50))
    metadata_1 = {"title": "My First Episode", "genre": "Sci-Fi", "language": "English", "director": "Jane Doe",
                  "episode_number": 1, "season_number": 1, "ei_rating": "E"}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_1), "w") as f:
        json.dump(metadata_1, f, indent=2)

    # File 2: Media File with Unsupported Format
    file_name_2 = "bad_format_video.avi"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_2), "w") as f:
        f.write("This is a dummy AVI file, which is likely not allowed by policy.")

    # File 3: Media File Violating Policy Rules (e.g., Simulated Duration Limit)
    file_name_3 = "long_ad_campaign_x.mp4"
    simulated_size_mb = 101
    with open(os.path.os.path.join(INGEST_SOURCE_DIR, file_name_3), "wb") as f:
        f.write(b"A" * (1024 * 1024 * simulated_size_mb))
    sidecar_name_3 = "long_ad_campaign_x.json"
    metadata_3 = {"title": "Long Ad Campaign X", "genre": "Commercial", "language": "English", "asset_type": "ad"}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_3), "w") as f:
        json.dump(metadata_3, f, indent=2)

    # File 4: Media File with Missing or Incomplete Metadata
    file_name_4 = "ad_missing_meta.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_4), "w") as f:
        f.write("This is a dummy ad file with intentionally missing metadata.")

    # File 5: Media File Simulating Blank Frames
    file_name_5 = "video_with_blank_frames.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_5), "w") as f:
        f.write("This is a dummy video file content. Imagine it has blank frames within.")

    # File 6: Media File with Minimal/Ambiguous Content (No Sidecar)
    file_name_6 = "mystery_video.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_6), "w") as f:
        f.write("This is a dummy video file with minimal content, no sidecar.")

    # File 7: Misleading Content File
    file_name_7 = "happy_adventure.mp4"
    sidecar_name_7 = "happy_adventure.json"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_7), "w") as f:
        f.write("This is a dummy video file content for a misleading test.")
    metadata_7 = {"title": "A Tragic Drama", "genre": "Drama", "language": "English",
                  "description": "A story of loss, despair, and unavoidable fate. Not a happy ending."}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_7), "w") as f:
        json.dump(metadata_7, f, indent=2)

    # File 8: Nonsensical/Garbage Content File
    file_name_8 = "random_gibberish.mp4"
    sidecar_name_8 = "random_gibberish.json"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_8), "w") as f:
        f.write("asdfjkl;qweruiopzxcvbnm,./1234567890!@#$%^&*()")
    metadata_8 = {"title": "asdfjkl;qweruiopzxcvbnm", "genre": "Nonsense", "language": "Gibberish",
                  "description": "This content is completely random and has no discernible meaning or structure. It's just a string of characters."}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_8), "w") as f:
        json.dump(metadata_8, f, indent=2)

    # File 9: Highly Abstract/Conceptual Content File
    file_name_9 = "concept_of_freedom.mp4"
    sidecar_name_9 = "concept_of_freedom.json"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_9), "w") as f:
        f.write("This is a dummy video file representing an abstract concept.")
    metadata_9 = {"title": "The Concept of Freedom", "genre": "Philosophical Documentary", "language": "English",
                  "description": "An exploration of the multifaceted and evolving understanding of liberty and autonomy across different cultures and historical periods."}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_9), "w") as f:
        json.dump(metadata_9, f, indent=2)

    # File 10: Highly Repetitive Content File
    file_name_10 = "repeating_pattern_video.mp4"
    sidecar_name_10 = "repeating_pattern_video.json"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_10), "w") as f:
        f.write("Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat.")
    metadata_10 = {"title": "A Study in Repetition", "genre": "Experimental", "language": "English",
                   "description": "This video consists solely of a repeating visual and auditory pattern designed to explore the effects of monotony."}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_10), "w") as f:
        json.dump(metadata_10, f, indent=2)

    # File 11: Highly Specific/Niche Content File
    file_name_11 = "quantum_chromodynamics_lecture.mp4"
    sidecar_name_11 = "quantum_chromodynamics_lecture.json"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_11), "w") as f:
        f.write("This is a dummy video file for a highly specific physics lecture.")
    metadata_11 = {"title": "Introduction to Quantum Chromodynamics", "genre": "Educational", "language": "English",
                   "description": "A graduate-level lecture covering the strong nuclear force, quarks, gluons, and asymptotic freedom within the Standard Model of particle physics.",
                   "lecturer": "Dr. Alice Smith", "course_code": "PHY701"}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_11), "w") as f:
        json.dump(metadata_11, f, indent=2)

    # File 12: Contradictory Numerical/Factual Data File
    file_name_12 = "historical_anomaly.mp4"
    sidecar_name_12 = "historical_anomaly.json"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_12), "w") as f:
        f.write("This is a dummy video file about a historical anomaly.")
    metadata_12 = {"title": "The Great Fire of London (1966)", "genre": "Historical Documentary", "language": "English",
                   "description": "A documentary detailing the devastating fire that swept through London in the year 1966, causing widespread destruction.",
                   "historical_period": "20th Century"}
    with open(os.path.join(INGEST_SOURCE_DIR, sidecar_name_12), "w") as f:
        json.dump(metadata_12, f, indent=2)

    # File 13: Corrupted Header Simulation File
    file_name_13 = "corrupted_header.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_13), "wb") as f:
        f.write(os.urandom(50))

    # File 14: Duplicate File Uploads (Same Content, Different Names)
    duplicate_content = b"This is the unique content for the duplicate test files."
    file_name_14_orig = "original_content_video.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_14_orig), "wb") as f:
        f.write(duplicate_content)
    file_name_14_dup1 = "duplicate_content_version_1.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_14_dup1), "wb") as f:
        f.write(duplicate_content)
    file_name_14_dup2 = "another_copy_of_content.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_14_dup2), "wb") as f:
        f.write(duplicate_content)

    # File 15: Oversized File Handling
    file_name_15 = "oversized_file_test.mp4"
    with open(os.path.join(INGEST_SOURCE_DIR, file_name_15), "wb") as f:
        f.write(b"This file is small, but will be simulated as 50GB.")

    logger.info("Dummy ingest files created.")

    media_files_to_process = [
        os.path.join(INGEST_SOURCE_DIR, f) for f in os.listdir(INGEST_SOURCE_DIR)
        if f.endswith(('.mp4', '.mov', '.mxf', '.avi'))
    ]
    media_files_to_process.sort()

    for asset_path in media_files_to_process:
        await orchestrator.run_pipeline_for_asset(asset_path)
        time.sleep(0.5)

    logger.info("\n--- All asset pipelines orchestrated. ---")
    logger.info("Check individual agent logs and the 'ingest_processed' and 'graphics_output' folders for results.")
    logger.info("KPI Tracker Contents: %s", orchestrator.ingest_agent.kpi_tracker)
    logger.info("Ragas Evaluation Completed: %s", orchestrator.ingest_agent.kpi_tracker.get("ragas_score_avg", "N/A"))


if __name__ == "__main__":
    asyncio.run(main())
