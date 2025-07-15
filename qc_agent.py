import json
import logging
from datetime import datetime
import os
import time

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.context import attach, detach

# Removed: from opentelemetry.sdk.trace import TracerProvider
# Removed: from opentelemetry.sdk.trace.export import BatchSpanProcessor
# Removed: from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


# --- Configuration for the QC Agent ---
CONFIG = {
    "log_level": "DEBUG",
    "log_file": "qc_agent.log"
}

# --- Setup Logging ---
logger = logging.getLogger("QCAgent")
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


class QCAgent:
    """
    The QC Agent is responsible for performing quality control checks on ingested assets
    and then handing off enriched metadata to downstream agents, such as the Graphics Agent.
    This demonstrates inter-agent message passing.
    """

    def __init__(self, config):  # Added config parameter
        self.config = config  # Store config as an instance variable
        logger.info("QC Agent initialized.")
        # --- OpenTelemetry Tracer Setup for QC Agent ---
        # Removed the TracerProvider setup from here.
        # It is now expected that the pipeline_orchestrator.py will set up the global
        # TracerProvider, and this agent will simply retrieve it.

        # Get the OpenTelemetry tracer for this module
        self.otel_tracer = trace.get_tracer(__name__)
        logger.info("QC Agent instance fully initialized and ready with OpenTelemetry tracer.")

    def perform_qc_checks(self, asset_info, parent_span=None):
        """
        Simulates various quality control checks on the asset information.
        :param asset_info: Dictionary containing processed asset metadata.
        :param parent_span: The OpenTelemetry Span from the calling context.
        :return: Updated asset_info with QC results.
        """
        # Ensure the parent_span context is active for this function
        token = None
        if parent_span:
            token = attach(trace.set_span_in_context(parent_span))

        try:
            # Removed 'parent=parent_span' from start_as_current_span
            with self.otel_tracer.start_as_current_span("perform_qc_checks", kind=trace.SpanKind.SERVER) as span:
                span.set_attribute("asset.title", asset_info.get('metadata', {}).get('title', 'N/A'))
                logger.info(f"Starting QC checks for asset: {asset_info.get('metadata', {}).get('title', 'N/A')}")
                logger.debug(f"Thought Chain: Initiating QC checks based on asset info.")

                qc_status = "PASS"
                qc_flags = []

                metadata = asset_info.get("metadata", {})

                # Simulated QC Check 1: Check for critical metadata presence (beyond basic ingest policy)
                with self.otel_tracer.start_as_current_span("qc_check_ai_summary",
                                                            kind=trace.SpanKind.INTERNAL) as s_summary_check:
                    if not metadata.get("ai_summary") or "no specified genre" in metadata.get("ai_summary", "").lower():
                        qc_flags.append("AI summary is generic or missing specific genre details.")
                        qc_status = "REVIEW"  # Needs human review, but not a hard FAIL for pipeline
                        logger.debug("Thought Chain: AI summary check flagged.")
                        s_summary_check.set_status(trace.StatusCode.ERROR,
                                                   description="AI summary generic or missing genre.")
                        s_summary_check.set_attribute("qc_flag.ai_summary_issue", True)
                    else:
                        s_summary_check.set_status(trace.StatusCode.OK)
                        s_summary_check.set_attribute("qc_flag.ai_summary_issue", False)

                # Simulated QC Check 2: Check for resolution compliance (if video)
                with self.otel_tracer.start_as_current_span("qc_check_resolution",
                                                            kind=trace.SpanKind.INTERNAL) as s_resolution_check:
                    ffprobe_data = asset_info.get("ffprobe_data", {})
                    video_stream = next((s for s in ffprobe_data.get("streams", []) if s.get("codec_type") == "video"),
                                        None)
                    if video_stream and (video_stream.get("width", 0) < 1920 or video_stream.get("height", 0) < 1080):
                        qc_flags.append(
                            f"Resolution ({video_stream.get('width')}x{video_stream.get('height')}) is below desired 1080p.")
                        qc_status = "REVIEW"
                        logger.debug("Thought Chain: Resolution check flagged.")
                        s_resolution_check.set_status(trace.StatusCode.ERROR, description="Resolution below 1080p.")
                        s_resolution_check.set_attribute("qc_flag.low_resolution", True)
                        s_resolution_check.set_attribute("video.width", video_stream.get('width'))
                        s_resolution_check.set_attribute("video.height", video_stream.get('height'))
                    elif not video_stream:
                        qc_flags.append("No video stream detected for QC resolution check.")
                        qc_status = "REVIEW"
                        logger.debug("Thought Chain: No video stream for resolution check.")
                        s_resolution_check.set_status(trace.StatusCode.ERROR,
                                                      description="No video stream for resolution check.")
                        s_resolution_check.set_attribute("qc_flag.no_video_stream", True)
                    else:
                        s_resolution_check.set_status(trace.StatusCode.OK)
                        s_resolution_check.set_attribute("qc_flag.low_resolution", False)

                # Simulated QC Check 3: Check for audio channels
                with self.otel_tracer.start_as_current_span("qc_check_audio_channels",
                                                            kind=trace.SpanKind.INTERNAL) as s_audio_check:
                    audio_stream = next((s for s in ffprobe_data.get("streams", []) if s.get("codec_type") == "audio"),
                                        None)
                    if audio_stream and audio_stream.get("channels", 0) < 2:
                        qc_flags.append(f"Audio channels ({audio_stream.get('channels')}) are mono, stereo preferred.")
                        qc_status = "REVIEW"
                        logger.debug("Thought Chain: Audio channel check flagged.")
                        s_audio_check.set_status(trace.StatusCode.ERROR,
                                                 description="Audio channels mono, stereo preferred.")
                        s_audio_check.set_attribute("qc_flag.mono_audio", True)
                        s_audio_check.set_attribute("audio.channels", audio_stream.get('channels'))
                    elif not audio_stream:
                        qc_flags.append("No audio stream detected for QC channel check.")
                        qc_status = "REVIEW"
                        logger.debug("Thought Chain: No audio stream for channel check.")
                        s_audio_check.set_status(trace.StatusCode.ERROR,
                                                 description="No audio stream for channel check.")
                        s_audio_check.set_attribute("qc_flag.no_audio_stream", True)
                    else:
                        s_audio_check.set_status(trace.StatusCode.OK)
                        s_audio_check.set_attribute("qc_flag.mono_audio", False)

                # Update asset_info with QC results
                asset_info["qc_status"] = qc_status
                asset_info["qc_flags"] = qc_flags
                asset_info["qc_timestamp"] = datetime.now().isoformat()

                span.set_attribute("qc.status", qc_status)
                span.set_attribute("qc.flags", json.dumps(qc_flags))

                if qc_status == "REVIEW":
                    logger.warning(
                        f"QC completed with status '{qc_status}' for {metadata.get('title', 'N/A')}. Flags: {qc_flags}")
                    logger.debug("Thought Chain: QC completed with review flags.")
                    span.set_status(trace.StatusCode.ERROR, description="QC completed with review flags.")
                else:
                    logger.info(f"QC completed with status '{qc_status}' for {metadata.get('title', 'N/A')}.")
                    logger.debug("Thought Chain: QC completed successfully.")
                    span.set_status(trace.StatusCode.OK)

                return asset_info
        finally:
            # Detach the context if it was attached
            if token:
                detach(token)

    def run_qc_checks(self, processed_ingest_info, parent_span=None):
        """
        Public method to run QC checks, designed to be called by the orchestrator.
        :param processed_ingest_info: Dictionary containing processed asset metadata from Ingest Agent.
        :param parent_span: The OpenTelemetry Span from the calling context.
        :return: Updated asset_info with QC results.
        """
        logger.info(f"QC Agent received asset for QC: {processed_ingest_info.get('metadata', {}).get('title', 'N/A')}")
        return self.perform_qc_checks(processed_ingest_info, parent_span=parent_span)

    def send_to_graphics_agent(self, enriched_metadata, parent_span=None):
        """
        Simulates sending enriched metadata to the Graphics Agent for further processing.
        This is the inter-agent message passing step.
        :param enriched_metadata: The metadata dictionary to send.
        :param parent_span: The OpenTelemetry Span from the calling context.
        :return: The simulated message payload that would be sent.
        """
        # Ensure the parent_span context is active for this function
        token = None
        if parent_span:
            token = attach(trace.set_span_in_context(parent_span))

        try:
            # Removed 'parent=parent_span' from start_as_current_span
            with self.otel_tracer.start_as_current_span("send_to_graphics_agent", kind=trace.SpanKind.PRODUCER) as span:
                span.set_attribute("asset.title", enriched_metadata.get('title', 'N/A'))
                logger.info(
                    f"Sending enriched metadata to Graphics Agent for asset: {enriched_metadata.get('title', 'N/A')}")
                logger.debug(f"Thought Chain: Preparing message for Graphics Agent.")

                simulated_message = {
                    "event_type": "METADATA_ENRICHED_FOR_GRAPHICS",
                    "timestamp": datetime.now().isoformat(),
                    "asset_id": enriched_metadata.get("checksum_sha256"),
                    "metadata_payload": enriched_metadata
                }

                logger.info(f"Simulated message sent to Graphics Agent: {json.dumps(simulated_message, indent=2)}")
                logger.debug("Thought Chain: Message sent to Graphics Agent (simulated).")

                span.set_attribute("message.event_type", simulated_message["event_type"])
                span.set_attribute("message.asset_id", simulated_message["asset_id"])
                span.set_attribute("message.payload_size_bytes", len(json.dumps(simulated_message).encode('utf-8')))
                span.set_status(trace.StatusCode.OK)

                # Simulate a slight delay for network/queue transmission
                time.sleep(0.1)
                logger.info("Message transmission simulated.")
                return simulated_message  # Return the message for orchestration
        finally:
            # Detach the context if it was attached
            if token:
                detach(token)


# --- Main execution block for demonstration and verification ---
if __name__ == "__main__":
    logger.info("--- Starting QC Agent Inter-Agent Message Passing Test ---")

    # Simulate an asset info structure that the Ingest Agent would hand off
    # This structure should mimic the 'Context Shared' output from the Ingest Agent
    sample_ingested_asset_info = {
        "original_path": "./ingest_source/my_episode_s01e01.mp4",
        "ingest_start_time": "2025-07-11T10:00:00.000000",
        "status": "COMPLETED",
        "format_valid": True,
        "format_issues": [],
        "transcode_needed": False,
        "ffprobe_data": {
            "format": {"filename": "my_episode_s01e01.mp4", "duration": 1200.0, "size": 100000000,
                       "format_name": "mp4"},
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
                {"codec_type": "audio", "codec_name": "aac", "channels": 2}
            ]
        },
        "metadata": {
            "source_file": "my_episode_s01e01.mp4",
            "ingest_timestamp": "2025-07-11T10:00:05.000000",
            "checksum_sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "file_size_bytes": 100000000,
            "duration": 1200.0,
            "title": "My First Episode",
            "asset_type": "episode",
            "ai_summary": "A thrilling sci-fi adventure in space. Explores themes of discovery and survival.",
            "ai_keywords": ["sci-fi", "adventure", "space", "discovery", "survival"],
            "mood": "exciting",
            "genre": "Sci-Fi",
            "language": "English",
            "ei_rating": "E",
            "normalized_file_path": "./ingest_processed/asset_xyz_my_first_episode.mp4",
            "normalized_file_name": "asset_xyz_my_first_episode.mp4",
            "compliance_status": "PASS",
            "compliance_flags": [],
            "escalate_to_human": False
        },
        "normalized_file_path": "./ingest_processed/asset_xyz_my_first_episode.mp4",
        "ingest_end_time": "2025-07-11T10:00:10.000000"
    }

    qc_agent = QCAgent(CONFIG)  # Pass CONFIG here for standalone test

    # 1. Simulate receiving asset from Ingest Agent and perform QC checks
    logger.info("Simulating asset reception from Ingest Agent and performing QC.")
    # In a standalone test, there's no parent span from an orchestrator, so pass None
    qc_processed_asset_info = qc_agent.perform_qc_checks(sample_ingested_asset_info, parent_span=None)

    # 2. Extract enriched metadata to send to Graphics Agent
    enriched_metadata_for_graphics = qc_processed_asset_info.get("metadata", {})

    # 3. Simulate sending enriched metadata to Graphics Agent
    logger.info("\nSimulating sending enriched metadata to Graphics Agent.")
    # In a standalone test, there's no parent span from an orchestrator, so pass None
    graphics_message_payload = qc_agent.send_to_graphics_agent(enriched_metadata_for_graphics, parent_span=None)

    logger.info("\n--- QC Agent Inter-Agent Message Passing Test Completed ---")

    # Ensure all buffered log messages are written to the file before exiting
    # Removed logging.shutdown() from here, as orchestrator will handle it
