import json
import logging
from datetime import datetime
import os  # Import os module for file operations
import time  # Import time for sleep

# --- Configuration for the QC Agent ---
CONFIG = {
    "log_level": "DEBUG",
    "log_file": "qc_agent.log"  # Define the log file name
}

# --- Setup Logging ---
# Get the logger instance for QCAgent
logger = logging.getLogger("QCAgent")
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


class QCAgent:
    """
    The QC Agent is responsible for performing quality control checks on ingested assets
    and then handing off enriched metadata to downstream agents, such as the Graphics Agent.
    This demonstrates inter-agent message passing.
    """

    def __init__(self):
        logger.info("QC Agent initialized.")

    def perform_qc_checks(self, asset_info):
        """
        Simulates various quality control checks on the asset information.
        :param asset_info: Dictionary containing processed asset metadata.
        :return: Updated asset_info with QC results.
        """
        logger.info(f"Starting QC checks for asset: {asset_info.get('metadata', {}).get('title', 'N/A')}")
        logger.debug(f"Thought Chain: Initiating QC checks based on asset info.")

        qc_status = "PASS"
        qc_flags = []

        metadata = asset_info.get("metadata", {})

        # Simulated QC Check 1: Check for critical metadata presence (beyond basic ingest policy)
        # Example: Ensure AI summary is not empty
        if not metadata.get("ai_summary") or "no specified genre" in metadata.get("ai_summary", "").lower():
            qc_flags.append("AI summary is generic or missing specific genre details.")
            qc_status = "REVIEW"  # Needs human review, but not a hard FAIL for pipeline
            logger.debug("Thought Chain: AI summary check flagged.")

        # Simulated QC Check 2: Check for resolution compliance (if video)
        ffprobe_data = asset_info.get("ffprobe_data", {})
        video_stream = next((s for s in ffprobe_data.get("streams", []) if s.get("codec_type") == "video"), None)
        if video_stream and (video_stream.get("width", 0) < 1920 or video_stream.get("height", 0) < 1080):
            qc_flags.append(
                f"Resolution ({video_stream.get('width')}x{video_stream.get('height')}) is below desired 1080p.")
            qc_status = "REVIEW"
            logger.debug("Thought Chain: Resolution check flagged.")
        elif not video_stream:
            qc_flags.append("No video stream detected for QC resolution check.")
            qc_status = "REVIEW"
            logger.debug("Thought Chain: No video stream for resolution check.")

        # Simulated QC Check 3: Check for audio channels
        audio_stream = next((s for s in ffprobe_data.get("streams", []) if s.get("codec_type") == "audio"), None)
        if audio_stream and audio_stream.get("channels", 0) < 2:
            qc_flags.append(f"Audio channels ({audio_stream.get('channels')}) are mono, stereo preferred.")
            qc_status = "REVIEW"
            logger.debug("Thought Chain: Audio channel check flagged.")
        elif not audio_stream:
            qc_flags.append("No audio stream detected for QC channel check.")
            qc_status = "REVIEW"
            logger.debug("Thought Chain: No audio stream for channel check.")

        # Update asset_info with QC results
        asset_info["qc_status"] = qc_status
        asset_info["qc_flags"] = qc_flags
        asset_info["qc_timestamp"] = datetime.now().isoformat()

        if qc_status == "REVIEW":
            logger.warning(
                f"QC completed with status '{qc_status}' for {metadata.get('title', 'N/A')}. Flags: {qc_flags}")
            logger.debug("Thought Chain: QC completed with review flags.")
        else:
            logger.info(f"QC completed with status '{qc_status}' for {metadata.get('title', 'N/A')}.")
            logger.debug("Thought Chain: QC completed successfully.")

        return asset_info

    def send_to_graphics_agent(self, enriched_metadata):
        """
        Simulates sending enriched metadata to the Graphics Agent for further processing.
        This is the inter-agent message passing step.
        :param enriched_metadata: The metadata dictionary to send.
        :return: The simulated message payload that would be sent.
        """
        logger.info(f"Sending enriched metadata to Graphics Agent for asset: {enriched_metadata.get('title', 'N/A')}")
        logger.debug(f"Thought Chain: Preparing message for Graphics Agent.")

        # In a real system, this would be:
        # 1. Publishing to a message queue (e.g., Kafka, RabbitMQ)
        # 2. Making an HTTP POST request to the Graphics Agent's API endpoint
        # 3. Calling a shared library function if agents are in the same process/monorepo

        # For simulation, we'll log the "message" that would be sent.
        # The Graphics Agent would then "receive" this same structure.

        simulated_message = {
            "event_type": "METADATA_ENRICHED_FOR_GRAPHICS",
            "timestamp": datetime.now().isoformat(),
            "asset_id": enriched_metadata.get("checksum_sha256"),  # Using checksum as a unique ID
            "metadata_payload": enriched_metadata  # The actual enriched metadata
        }

        logger.info(f"Simulated message sent to Graphics Agent: {json.dumps(simulated_message, indent=2)}")
        logger.debug("Thought Chain: Message sent to Graphics Agent (simulated).")

        # Simulate a slight delay for network/queue transmission
        time.sleep(0.1)
        logger.info("Message transmission simulated.")
        return simulated_message  # Return the message for orchestration


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
            "genre": "Sci-Fi",  # Added for metadata completeness check
            "language": "English",  # Added for metadata completeness check
            "ei_rating": "E",  # Added for kids content check
            "normalized_file_path": "./ingest_processed/asset_xyz_my_first_episode.mp4",
            "normalized_file_name": "asset_xyz_my_first_episode.mp4",
            "compliance_status": "PASS",
            "compliance_flags": [],
            "escalate_to_human": False
        },
        "normalized_file_path": "./ingest_processed/asset_xyz_my_first_episode.mp4",
        "ingest_end_time": "2025-07-11T10:00:10.000000"
    }

    qc_agent = QCAgent()

    # 1. Simulate receiving asset from Ingest Agent and perform QC checks
    logger.info("Simulating asset reception from Ingest Agent and performing QC.")
    qc_processed_asset_info = qc_agent.perform_qc_checks(sample_ingested_asset_info)

    # 2. Extract enriched metadata to send to Graphics Agent
    enriched_metadata_for_graphics = qc_processed_asset_info.get("metadata", {})

    # 3. Simulate sending enriched metadata to Graphics Agent
    logger.info("\nSimulating sending enriched metadata to Graphics Agent.")
    # The return value is now captured for the orchestrator
    graphics_message_payload = qc_agent.send_to_graphics_agent(enriched_metadata_for_graphics)

    logger.info("\n--- QC Agent Inter-Agent Message Passing Test Completed ---")

    # Ensure all buffered log messages are written to the file before exiting
    # Removed logging.shutdown() from here, as orchestrator will handle it
