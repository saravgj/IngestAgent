import json
import logging
from datetime import datetime
import time
import os  # Import os module for file operations

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

    def receive_metadata(self, message_payload):
        """
        Simulates receiving a message containing enriched metadata from another agent.
        :param message_payload: The full message payload, expected to contain 'metadata_payload'.
        """
        logger.info(f"Graphics Agent received a message.")
        logger.debug(f"Thought Chain: Parsing incoming message payload.")

        event_type = message_payload.get("event_type")
        timestamp = message_payload.get("timestamp")
        asset_id = message_payload.get("asset_id")
        enriched_metadata = message_payload.get("metadata_payload")

        if not enriched_metadata:
            logger.error("Received message without 'metadata_payload'. Cannot proceed with graphics generation.")
            logger.debug("Thought Chain: Missing metadata payload. Aborting graphics processing.")
            return False

        title = enriched_metadata.get("title", "Untitled Asset")
        asset_type = enriched_metadata.get("asset_type", "unknown")
        ai_summary = enriched_metadata.get("ai_summary", "No summary provided.")
        ai_keywords = enriched_metadata.get("ai_keywords", [])

        logger.info(f"Processing graphics for asset: '{title}' (ID: {asset_id})")
        logger.debug(f"Thought Chain: Extracted key metadata for graphics generation.")

        # --- Simulate Graphics Generation Subtasks ---

        # Subtask 1: Generate Thumbnail/Key Art
        logger.debug("Thought Chain: Initiating thumbnail generation.")
        thumbnail_path = self._generate_thumbnail(title, asset_id, asset_type)
        if thumbnail_path:
            logger.info(f"Generated thumbnail: {thumbnail_path}")
            enriched_metadata["graphics_thumbnail_path"] = thumbnail_path
        else:
            logger.warning("Failed to generate thumbnail.")
        logger.debug("Thought Chain: Thumbnail generation complete.")

        # Subtask 2: Prepare Title Card/Overlay Text
        logger.debug("Thought Chain: Preparing title card text.")
        title_card_text = self._prepare_title_card(title, ai_summary)
        if title_card_text:
            logger.info(f"Prepared title card text: '{title_card_text[:50]}...'")
            enriched_metadata["graphics_title_card_text"] = title_card_text
        else:
            logger.warning("Failed to prepare title card text.")
        logger.debug("Thought Chain: Title card text preparation complete.")

        # Subtask 3: Suggest Tags for Visual Search (using AI keywords)
        logger.debug("Thought Chain: Suggesting visual tags from AI keywords.")
        visual_tags = self._suggest_visual_tags(ai_keywords)
        if visual_tags:
            logger.info(f"Suggested visual tags: {', '.join(visual_tags)}")
            enriched_metadata["graphics_visual_tags"] = visual_tags
        else:
            logger.warning("No visual tags suggested.")
        logger.debug("Thought Chain: Visual tag suggestion complete.")

        logger.info(f"Graphics generation complete for asset: '{title}'.")
        logger.debug("Thought Chain: All graphics subtasks completed.")
        return True

    def _generate_thumbnail(self, title, asset_id, asset_type):
        """
        Simulates generating a thumbnail image.
        In a real system, this would involve image processing libraries (e.g., Pillow, OpenCV)
        or calling a dedicated thumbnailing service.
        """
        time.sleep(0.05)  # Simulate work
        return f"./graphics_output/{asset_id}_thumbnail.png"

    def _prepare_title_card(self, title, summary):
        """
        Simulates preparing text for a title card or on-screen overlay.
        """
        time.sleep(0.02)  # Simulate work
        return f"TITLE: {title}\nSUMMARY: {summary}"

    def _suggest_visual_tags(self, keywords):
        """
        Simulates suggesting visual tags based on AI keywords.
        """
        time.sleep(0.01)  # Simulate work
        # Simple logic: just return keywords for now
        return [tag for tag in keywords if len(tag) > 3]  # Filter short tags


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
    success = graphics_agent.receive_metadata(simulated_message_from_qc_agent)

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
