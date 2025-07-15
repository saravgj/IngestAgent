import json
import logging
from datetime import datetime
import time
import os  # Import os module for file operations

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.context import attach, detach  # For context propagation
from opentelemetry.trace import SpanKind  # For defining span kind

# --- Configuration for the Graphics Agent ---
CONFIG = {
    "log_level": "DEBUG",
    "log_file": "graphics_agent.log"  # Define the log file name
}

# --- Setup Logging ---
# Get the logger instance for GraphicsAgent
logger = logging.getLogger("GraphicsAgent")
logger.setLevel(getattr(logging, CONFIG["log_level"].upper()))

# Add handlers only if they don't already exist to prevent duplicate logs when imported
if not logger.handlers:
    # Clear log file on each run for demonstration purposes
    if os.path.exists(CONFIG["log_file"]):
        os.remove(CONFIG["log_file"])

    file_handler = logging.FileHandler(CONFIG["log_file"])
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(stream_handler)


class GraphicsAgent:
    """
    The Graphics Agent is responsible for processing enriched metadata to generate
    various graphical assets (e.g., thumbnails, title cards, overlays).
    It demonstrates receiving messages from other agents (e.g., QC Agent).
    """

    def __init__(self):
        logger.info("Graphics Agent initialized.")
        # Get the OpenTelemetry tracer for this module.
        # It is expected that the pipeline_orchestrator.py has already set up the global TracerProvider.
        self.otel_tracer = trace.get_tracer(__name__)
        logger.info("Graphics Agent instance fully initialized and ready with OpenTelemetry tracer.")

    def receive_metadata(self, message_payload, parent_span=None):
        """
        Simulates receiving a message containing enriched metadata from another agent.
        :param message_payload: The full message payload, expected to contain 'metadata_payload'.
        :param parent_span: The OpenTelemetry Span from the calling context (optional).
        """
        # Ensure the parent_span context is active for this function
        token = None
        if parent_span:
            token = attach(trace.set_span_in_context(parent_span))

        try:
            # Removed 'parent=parent_span' from start_as_current_span
            with self.otel_tracer.start_as_current_span("receive_metadata", kind=SpanKind.CONSUMER) as span:
                logger.info(f"Graphics Agent received a message.")
                logger.debug(f"Thought Chain: Parsing incoming message payload.")
                span.set_attribute("message.event_type", message_payload.get("event_type"))
                span.set_attribute("message.asset_id", message_payload.get("asset_id"))

                event_type = message_payload.get("event_type")
                timestamp = message_payload.get("timestamp")
                asset_id = message_payload.get("asset_id")
                enriched_metadata = message_payload.get("metadata_payload")

                if not enriched_metadata:
                    logger.error(
                        "Received message without 'metadata_payload'. Cannot proceed with graphics generation.")
                    logger.debug("Thought Chain: Missing metadata payload. Aborting graphics processing.")
                    span.set_status(trace.StatusCode.ERROR, description="Missing metadata_payload in message.")
                    return False

                title = enriched_metadata.get("title", "Untitled Asset")
                asset_type = enriched_metadata.get("asset_type", "unknown")
                ai_summary = enriched_metadata.get("ai_summary", "No summary provided.")
                ai_keywords = enriched_metadata.get("ai_keywords", [])

                span.set_attribute("asset.title", title)
                span.set_attribute("asset.type", asset_type)
                logger.info(f"Processing graphics for asset: '{title}' (ID: {asset_id})")
                logger.debug(f"Thought Chain: Extracted key metadata for graphics generation.")

                # --- Simulate Graphics Generation Subtasks ---

                # Subtask 1: Generate Thumbnail/Key Art
                logger.debug("Thought Chain: Initiating thumbnail generation.")
                # Removed 'parent_span=span' from sub-span calls
                thumbnail_path = self._generate_thumbnail(title, asset_id, asset_type)
                if thumbnail_path:
                    logger.info(f"Generated thumbnail: {thumbnail_path}")
                    enriched_metadata["graphics_thumbnail_path"] = thumbnail_path
                    span.set_attribute("graphics.thumbnail_generated", True)
                    span.set_attribute("graphics.thumbnail_path", thumbnail_path)
                else:
                    logger.warning("Failed to generate thumbnail.")
                    span.set_attribute("graphics.thumbnail_generated", False)
                logger.debug("Thought Chain: Thumbnail generation complete.")

                # Subtask 2: Prepare Title Card/Overlay Text
                logger.debug("Thought Chain: Preparing title card text.")
                # Removed 'parent_span=span' from sub-span calls
                title_card_text = self._prepare_title_card(title, ai_summary)
                if title_card_text:
                    logger.info(f"Prepared title card text: '{title_card_text[:50]}...'")
                    enriched_metadata["graphics_title_card_text"] = title_card_text
                    span.set_attribute("graphics.title_card_generated", True)
                    span.set_attribute("graphics.title_card_text_preview", title_card_text[:50])
                else:
                    logger.warning("Failed to prepare title card text.")
                    span.set_attribute("graphics.title_card_generated", False)
                logger.debug("Thought Chain: Title card text preparation complete.")

                # Subtask 3: Suggest Tags for Visual Search (using AI keywords)
                logger.debug("Thought Chain: Suggesting visual tags from AI keywords.")
                # Removed 'parent_span=span' from sub-span calls
                visual_tags = self._suggest_visual_tags(ai_keywords)
                if visual_tags:
                    logger.info(f"Suggested visual tags: {', '.join(visual_tags)}")
                    enriched_metadata["graphics_visual_tags"] = visual_tags
                    span.set_attribute("graphics.visual_tags_suggested", True)
                    span.set_attribute("graphics.visual_tags_list", json.dumps(visual_tags))
                else:
                    logger.warning("No visual tags suggested.")
                    span.set_attribute("graphics.visual_tags_suggested", False)
                logger.debug("Thought Chain: Visual tag suggestion complete.")

                logger.info(f"Graphics generation complete for asset: '{title}'.")
                logger.debug("Thought Chain: All graphics subtasks completed.")
                span.set_status(trace.StatusCode.OK)
                return True
        finally:
            # Detach the context if it was attached
            if token:
                detach(token)

    def _generate_thumbnail(self, title, asset_id, asset_type, parent_span=None):
        """
        Simulates generating a thumbnail image.
        :param parent_span: The OpenTelemetry Span from the calling context.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("generate_thumbnail", kind=SpanKind.INTERNAL) as span:
            span.set_attribute("thumbnail.title", title)
            span.set_attribute("thumbnail.asset_id", asset_id)
            span.set_attribute("thumbnail.asset_type", asset_type)
            time.sleep(0.05)  # Simulate work
            thumbnail_path = f"./graphics_output/{asset_id}_thumbnail.png"
            span.set_attribute("thumbnail.output_path", thumbnail_path)
            span.set_status(trace.StatusCode.OK)
            return thumbnail_path

    def _prepare_title_card(self, title, summary, parent_span=None):
        """
        Simulates preparing text for a title card or on-screen overlay.
        :param parent_span: The OpenTelemetry Span from the calling context.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("prepare_title_card", kind=SpanKind.INTERNAL) as span:
            span.set_attribute("title_card.source_title", title)
            span.set_attribute("title_card.source_summary", summary)
            time.sleep(0.02)  # Simulate work
            title_card_text = f"TITLE: {title}\nSUMMARY: {summary}"
            span.set_attribute("title_card.output_text_preview", title_card_text[:50])
            span.set_status(trace.StatusCode.OK)
            return title_card_text

    def _suggest_visual_tags(self, keywords, parent_span=None):
        """
        Simulates suggesting visual tags based on AI keywords.
        :param parent_span: The OpenTelemetry Span from the calling context.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("suggest_visual_tags", kind=SpanKind.INTERNAL) as span:
            span.set_attribute("visual_tags.source_keywords", json.dumps(keywords))
            time.sleep(0.01)  # Simulate work
            # Simple logic: just return keywords for now
            visual_tags = [tag for tag in keywords if len(tag) > 3]  # Filter short tags
            span.set_attribute("visual_tags.output_list", json.dumps(visual_tags))
            span.set_status(trace.StatusCode.OK)
            return visual_tags


# --- Main execution block for demonstration and verification ---
if __name__ == "__main__":
    logger.info("--- Starting Graphics Agent Inter-Agent Message Reception Test ---")

    # Simulate the message payload that the QC Agent would send
    # This should match the structure produced by QCAgent.send_to_graphics_agent
    simulated_message_from_qc_agent = {
        "event_type": "METADATA_ENRICHED_FOR_GRAPHICS",
        "timestamp": datetime.now().isoformat(),
        "asset_id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        "metadata_payload": {
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
            "escalate_to_human": False,
            "qc_status": "PASS",  # QC Agent would add these
            "qc_flags": [],
            "qc_timestamp": datetime.now().isoformat()
        }
    }

    graphics_agent = GraphicsAgent()

    # Simulate receiving the message and processing it
    logger.info("Simulating receiving enriched metadata from QC Agent.")
    # Pass None for parent_span as no OTel client is initialized here for standalone test
    success = graphics_agent.receive_metadata(simulated_message_from_qc_agent, parent_span=None)

    # --- Verification of Output (via logs) ---
    logger.info("\n--- Verifying Graphics Agent Processing (via logs) ---")
    if success:
        logger.info("Verification PASSED: Graphics Agent successfully processed the received metadata.")
        # Further checks could involve inspecting the 'enriched_metadata' object after processing
        # if it were returned or stored by the Graphics Agent. For this simulation,
        # we rely on the internal logs of the Graphics Agent.
    else:
        logger.error("Verification FAILED: Graphics Agent failed to process the received metadata.")

    logger.info("--- Graphics Agent Inter-Agent Message Reception Test Completed ---")

    # Removed logging.shutdown() from here, as orchestrator will handle it
