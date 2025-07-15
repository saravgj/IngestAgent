import os
import json
import hashlib
import time
import logging
from datetime import datetime
import requests
import sys

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.context import attach, detach
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# --- Configuration for the Ingest Agent ---
CONFIG = {
    "watch_folders": ["./ingest_source"],
    "output_folder": "./ingest_processed",
    "policies": {
        "min_duration_seconds": 10,
        "max_duration_seconds": 3600,
        "allowed_formats": ["mp4", "mov", "mxf"],
        "required_metadata_fields": ["title", "genre", "language"],
        "ad_duration_limit_seconds": 60,
        "max_file_size_mb": 1024 * 40
    },
    "log_level": "DEBUG",
    "log_file": "ingest_agent.log"
}

# --- Setup Logging ---
logger = logging.getLogger("IngestAgent")
logger.setLevel(getattr(logging, CONFIG["log_level"].upper()))

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


class IngestAgent:
    """
    The Ingest Agent is responsible for intelligently acquiring, verifying,
    tagging, and preparing media assets for downstream processing.
    """

    def __init__(self, config):
        self.config = config
        self.processed_files = set()
        self.processed_checksums = set()
        self._setup_directories()
        logger.info("Ingest Agent initialized.")
        logger.debug(f"Configuration loaded: {json.dumps(self.config, indent=2)}")

        self.total_ingests = 0
        self.successful_ingests = 0
        self.total_gemini_calls = 0
        self.successful_gemini_calls = 0
        self.total_metadata_fields_expected = 0
        self.total_metadata_fields_present = 0
        self.total_handoff_latency = 0.0
        self.handoff_count = 0
        self.duplicate_asset_count = 0
        self.retry_attempts = 0

        # --- OpenTelemetry Tracer Setup for Ingest Agent ---
        # Removed the TracerProvider setup from here.
        # It is now expected that the pipeline_orchestrator.py will set up the global
        # TracerProvider, and this agent will simply retrieve it.

        # Get the OpenTelemetry tracer for this module
        self.otel_tracer = trace.get_tracer(__name__)
        logger.info("Ingest Agent instance fully initialized and ready with OpenTelemetry tracer.")

    def _setup_directories(self):
        """
        Ensures watch and output directories exist.
        """
        for folder in self.config["watch_folders"]:
            os.makedirs(folder, exist_ok=True)
            logger.info(f"Ensured watch folder exists: {folder}")
        os.makedirs(self.config["output_folder"], exist_ok=True)
        logger.info(f"Ensured output folder exists: {self.config['output_folder']}")

    def _calculate_checksum(self, file_path, algorithm="sha256", parent_span=None):
        """
        Calculates the checksum of a file.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("_calculate_checksum", kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("file.path", file_path)
            span.set_attribute("checksum.algorithm", algorithm)
            try:
                hasher = hashlib.new(algorithm)
                with open(file_path, 'rb') as f:
                    while chunk := f.read(8192):
                        hasher.update(chunk)
                checksum_val = hasher.hexdigest()
                logger.debug(f"Calculated {algorithm} checksum for {file_path}")
                span.set_status(trace.StatusCode.OK)
                span.set_attribute("checksum.value", checksum_val)
                return checksum_val
            except Exception as e:
                logger.error(f"Error calculating checksum for {file_path}: {e}")
                span.set_status(trace.StatusCode.ERROR, description=f"Error calculating checksum: {e}")
                span.record_exception(e)
                return None

    def _simulate_ffprobe(self, file_path, parent_span=None):
        """
        Simulates FFprobe output for format detection and technical checks.
        In a real scenario, this would execute FFprobe via subprocess.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("_simulate_ffprobe", kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("file.path", file_path)
            file_extension = os.path.splitext(file_path)[1].lower().lstrip('.')
            file_size_bytes = os.path.getsize(file_path)
            file_name_lower = os.path.basename(file_path).lower()

            if "corrupted_header" in file_name_lower:
                logger.warning(f"Simulating FFprobe failure for {file_path} due to suspected corruption by name.")
                span.set_attribute("ffprobe.simulated_failure", True)
                span.set_status(trace.StatusCode.ERROR, description="Simulated corrupted header")
                return {}

            simulated_size_bytes = file_size_bytes
            if "oversized_file_test" in file_name_lower:
                simulated_size_bytes = 50 * 1024 * 1024 * 1024
                logger.warning(f"Simulating FFprobe reporting 50GB size for oversized file: {file_path}")
                span.set_attribute("ffprobe.simulated_oversize", True)

            file_size_mb = simulated_size_bytes / (1024 * 1024)

            simulated_data = {
                "format": {
                    "filename": os.path.basename(file_path),
                    "nb_streams": 2,
                    "format_name": file_extension,
                    "duration": file_size_mb * 60 / 100,
                    "size": simulated_size_bytes
                },
                "streams": [
                    {
                        "codec_type": "video",
                        "codec_name": "h264" if file_extension in ["mp4", "mov"] else "mpeg2video",
                        "width": 1920,
                        "height": 1080,
                        "avg_frame_rate": "25/1",
                        "display_aspect_ratio": "16:9"
                    },
                    {
                        "codec_type": "audio",
                        "codec_name": "aac" if file_extension in ["mp4", "mov"] else "mp3",
                        "channels": 2
                    }
                ]
            }
            logger.debug(f"Simulated FFprobe for {file_path}: {simulated_data}")
            span.set_status(trace.StatusCode.OK)
            span.set_attribute("ffprobe.data", json.dumps(simulated_data))
            return simulated_data

    def _call_gemini_api(self, prompt, schema=None, parent_span=None):
        """
        Makes a call to the Gemini API to generate content.
        Tracks tool usage success rate.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("_call_gemini_api", kind=trace.SpanKind.CLIENT) as span:
            self.total_gemini_calls += 1
            span.set_attribute("llm.prompt", prompt)
            if schema:
                span.set_attribute("llm.schema", json.dumps(schema))

            chatHistory = []
            chatHistory.append({"role": "user", "parts": [{"text": prompt}]})
            payload = {"contents": chatHistory}

            if schema:
                payload["generationConfig"] = {
                    "responseMimeType": "application/json",
                    "responseSchema": schema
                }

            apiKey = ""
            if 'GOOGLE_API_KEY' in os.environ and not (hasattr(self, '__app_id') and self.__app_id):
                apiKey = os.environ.get('GOOGLE_API_KEY')
                logger.debug("Using API key from GOOGLE_API_KEY environment variable.")
            elif not apiKey:
                logger.warning(
                    "API Key is empty. Ensure GOOGLE_API_KEY environment variable is set if running locally, or that Canvas is injecting it.")
                span.set_status(trace.StatusCode.ERROR, description="API Key not found.")
                return None

            apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={apiKey}";

            try:
                response = requests.post(apiUrl, headers={'Content-Type': 'application/json'}, json=payload)
                response.raise_for_status()
                result = response.json()

                if result.get("candidates") and result["candidates"][0].get("content") and result["candidates"][0][
                    "content"].get("parts"):
                    self.successful_gemini_calls += 1
                    raw_text = result["candidates"][0]["content"]["parts"][0]["text"]
                    logger.debug(f"LLM call made. Recording will be handled by Langfuse via OTel.")
                    span.set_status(trace.StatusCode.OK)
                    span.set_attribute("llm.response", raw_text)

                    if schema:
                        try:
                            parsed_json = json.loads(raw_text)
                            span.set_attribute("llm.parsed_json", json.dumps(parsed_json))
                            return parsed_json
                        except json.JSONDecodeError as e:
                            logger.error(
                                f"Error decoding JSON from Gemini API response with schema: {e}. Raw text: {raw_text}")
                            span.set_status(trace.StatusCode.ERROR, description=f"JSONDecodeError: {e}")
                            span.record_exception(e)
                            return None
                    return raw_text
                else:
                    logger.warning(f"Gemini API response missing expected content structure: {result}")
                    span.set_status(trace.StatusCode.ERROR, description=f"Missing content in LLM response: {result}")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Error calling Gemini API: {e}")
                span.set_status(trace.StatusCode.ERROR, description=f"RequestException: {e}")
                span.record_exception(e)
                return None
            except Exception as e:
                logger.critical(f"An unexpected error occurred during Gemini API call: {e}", exc_info=True)
                span.set_status(trace.StatusCode.ERROR, description=f"Unexpected error during LLM call: {e}")
                span.record_exception(e)
                return None

    def _real_ai_enrichment(self, metadata, content_type="episode", parent_span=None):
        """
        Performs real AI-based metadata enrichment using a generative model.
        Ensures a dictionary is always returned, even if enrichment fails.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("_real_ai_enrichment", kind=trace.SpanKind.INTERNAL) as span:
            print("DEBUG: Entering _real_ai_enrichment.")
            sys.stdout.flush()
            logger.info(f"Performing real AI enrichment for asset type: {content_type}")
            logger.debug(
                f"Thought Chain: Starting AI enrichment. Initial metadata: {metadata.get('title', 'N/A')}, type: {content_type}")
            span.set_attribute("asset.type", content_type)
            span.set_attribute("initial.metadata.title", metadata.get("title", "N/A"))

            enriched_metadata = metadata.copy()

            # AI Summary Generation
            with self.otel_tracer.start_as_current_span("ai_summary_generation",
                                                        kind=trace.SpanKind.INTERNAL) as s_summary:
                prompt_summary = f"Generate a very short, concise summary (max 2 sentences) for a media asset with title '{enriched_metadata.get('title', 'N/A')}' and genre '{enriched_metadata.get('genre', 'N/A')}'. Focus only on observable content. Do not invent details."
                logger.debug(
                    f"Thought Chain: Attempting to generate AI summary based on prompt: '{prompt_summary[:70]}...'")
                print("DEBUG: Calling _call_gemini_api for summary generation.")
                sys.stdout.flush()
                try:
                    summary = self._call_gemini_api(prompt_summary, parent_span=s_summary)
                    print(f"DEBUG: _call_gemini_api (summary) returned: {summary}")
                    sys.stdout.flush()
                    if summary:
                        enriched_metadata["ai_summary"] = summary
                        logger.debug(
                            f"Thought Chain: Successfully generated AI summary. Justification: LLM provided concise overview based on title/genre.")
                        s_summary.set_status(trace.StatusCode.OK)
                        s_summary.set_attribute("ai.summary.generated", True)
                        s_summary.set_attribute("ai.summary.text", summary)
                    else:
                        logger.warning("Failed to generate AI summary. Skipping summary enrichment.")
                        logger.debug(
                            f"Thought Chain: Failed to generate AI summary. Justification: LLM call failed or returned empty. Skipping summary step.")
                        s_summary.set_status(trace.StatusCode.ERROR, description="Failed to generate AI summary.")
                        s_summary.set_attribute("ai.summary.generated", False)
                except Exception as e:
                    logger.error(f"Error during AI summary generation: {e}", exc_info=True)
                    print(f"DEBUG: Error in summary generation: {e}")
                    sys.stdout.flush()
                    s_summary.set_status(trace.StatusCode.ERROR,
                                         description=f"Exception during summary generation: {e}")
                    s_summary.record_exception(e)

            # AI Keywords Generation
            with self.otel_tracer.start_as_current_span("ai_keywords_generation",
                                                        kind=trace.SpanKind.INTERNAL) as s_keywords:
                keywords_schema = {
                    "type": "ARRAY",
                    "items": {
                        "type": "STRING"
                    }
                }
                prompt_keywords = f"Extract up to 5 keywords from the following media asset description: '{enriched_metadata.get('title', 'N/A')} - {enriched_metadata.get('ai_summary', '')}'. Return only the keywords as a JSON array of strings. Do not add any other text."
                logger.debug(
                    f"Thought Chain: Attempting to generate AI keywords based on prompt: '{prompt_keywords[:70]}...'")
                print("DEBUG: Calling _call_gemini_api for keywords generation.")
                sys.stdout.flush()
                try:
                    keywords_response = self._call_gemini_api(prompt_keywords, schema=keywords_schema,
                                                              parent_span=s_keywords)
                    print(f"DEBUG: _call_gemini_api (keywords) returned: {keywords_response}")
                    sys.stdout.flush()
                    if keywords_response and isinstance(keywords_response, list):
                        enriched_metadata["ai_keywords"] = keywords_response
                        logger.debug(
                            f"Thought Chain: Successfully generated AI keywords. Justification: LLM provided relevant keywords in expected format.")
                        s_keywords.set_status(trace.StatusCode.OK)
                        s_keywords.set_attribute("ai.keywords.generated", True)
                        s_keywords.set_attribute("ai.keywords.list", json.dumps(keywords_response))
                    else:
                        logger.warning(
                            f"Failed to generate AI keywords or unexpected format: {keywords_response}. Skipping keyword enrichment.")
                        logger.debug(
                            f"Thought Chain: Failed to generate AI keywords. Justification: LLM call failed or returned unexpected format. Skipping keyword step.")
                        s_keywords.set_status(trace.StatusCode.ERROR,
                                              description="Failed to generate AI keywords or unexpected format.")
                        s_keywords.set_attribute("ai.keywords.generated", False)
                except Exception as e:
                    logger.error(f"Error during AI keywords generation: {e}", exc_info=True)
                    print(f"DEBUG: Error in keywords generation: {e}")
                    sys.stdout.flush()
                    s_keywords.set_status(trace.StatusCode.ERROR,
                                          description=f"Exception during keywords generation: {e}")
                    s_keywords.record_exception(e)

            # AI Mood Generation
            with self.otel_tracer.start_as_current_span("ai_mood_generation", kind=trace.SpanKind.INTERNAL) as s_mood:
                if "mood" not in enriched_metadata:
                    prompt_mood = f"Based on the genre '{enriched_metadata.get('genre', 'N/A')}' and summary '{enriched_metadata.get('ai_summary', '')}', what is the primary mood of this content (e.g., 'exciting', 'calm', 'humorous', 'serious')? Respond with only one word."
                    logger.debug(
                        f"Thought Chain: Attempting to generate AI mood based on prompt: '{prompt_mood[:70]}...'")
                    print("DEBUG: Calling _call_gemini_api for mood generation.")
                    sys.stdout.flush()
                    try:
                        mood = self._call_gemini_api(prompt_mood, parent_span=s_mood)
                        print(f"DEBUG: _call_gemini_api (mood) returned: {mood}")
                        sys.stdout.flush()
                        if mood:
                            enriched_metadata["mood"] = mood.strip().lower()
                            logger.debug(
                                f"Thought Chain: Successfully generated AI mood. Justification: LLM provided a single-word mood based on context.")
                            s_mood.set_status(trace.StatusCode.OK)
                            s_mood.set_attribute("ai.mood.generated", True)
                            s_mood.set_attribute("ai.mood.value", enriched_metadata["mood"])
                        else:
                            logger.warning("Failed to generate AI mood. Skipping mood enrichment.")
                            logger.debug(
                                f"Thought Chain: Failed to generate AI mood. Justification: LLM call failed or returned empty. Skipping mood step.")
                            s_mood.set_status(trace.StatusCode.ERROR, description="Failed to generate AI mood.")
                            s_mood.set_attribute("ai.mood.generated", False)
                    except Exception as e:
                        logger.error(f"Error during AI mood generation: {e}", exc_info=True)
                        print(f"DEBUG: Error in mood generation: {e}")
                        sys.stdout.flush()
                        s_mood.set_status(trace.StatusCode.ERROR, description=f"Exception during mood generation: {e}")
                        s_mood.record_exception(e)
                else:
                    logger.debug(
                        f"Thought Chain: Mood already present in metadata. Justification: Existing metadata takes precedence. Skipping AI mood generation.")
                    s_mood.set_attribute("ai.mood.skipped", True)
                    s_mood.set_attribute("ai.mood.existing_value", enriched_metadata["mood"])

            logger.debug(f"Real AI enrichment complete. New metadata: {enriched_metadata}")
            logger.debug(f"Thought Chain: AI enrichment process concluded.")
            print("DEBUG: Exiting _real_ai_enrichment.")
            sys.stdout.flush()
            span.set_status(trace.StatusCode.OK)
            return enriched_metadata

    def _get_asset_type_from_path(self, file_path, parent_span=None):
        """
        Determines asset type based on watch folder or naming conventions.
        For simplicity, we'll use a basic heuristic here.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("_get_asset_type_from_path", kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("file.path", file_path)
            logger.debug(f"Thought Chain: Determining asset type for {os.path.basename(file_path)}")
            asset_type = "episode"
            if "ad" in os.path.basename(file_path).lower():
                logger.debug("Thought Chain: Identified as 'ad' based on filename heuristic.")
                asset_type = "ad"
            elif "trailer" in os.path.basename(file_path).lower():
                logger.debug("Thought Chain: Identified as 'trailer' based on filename heuristic.")
                asset_type = "trailer"
            logger.debug(f"Thought Chain: Determined asset type: {asset_type}.")
            span.set_attribute("asset.type.determined", asset_type)
            span.set_status(trace.StatusCode.OK)
            return asset_type

    def asset_acquisition(self, source_path, parent_span=None):
        """
        Monitors content sources and detects new files.
        For this simulation, it scans a local directory.
        Tracks duplicate asset incidents.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("asset_acquisition", kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("source.path", source_path)
            logger.debug(f"Thought Chain: Starting asset acquisition from {source_path}")
            new_assets = []
            try:
                for root, _, files in os.walk(source_path):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        if file_path in self.processed_files:
                            logger.debug(f"Thought Chain: File path {file_path} already processed. Skipping.")
                            span.add_event("file_skipped", {"reason": "already_processed", "file.path": file_path})
                            continue

                        if not file_name.endswith(('.json', '.xml', '.txt')):
                            checksum = self._calculate_checksum(file_path, parent_span=span)
                            logger.debug(
                                f"DEBUG: Checking file {file_name}. Calculated checksum: {checksum}. Processed checksums: {self.processed_checksums}")
                            if checksum and checksum in self.processed_checksums:
                                logger.info(f"Detected duplicate asset (checksum match): {file_path}. Skipping.")
                                logger.debug(
                                    f"Thought Chain: Checksum {checksum} for {file_path} already processed. Skipping. Justification: Content hash matches previously processed asset.")
                                self.processed_files.add(file_path)
                                self.duplicate_asset_count += 1
                                span.add_event("duplicate_asset_skipped",
                                               {"checksum": checksum, "file.path": file_path})
                                continue

                            new_assets.append(file_path)
                            self.processed_files.add(file_path)
                            logger.debug(f"Thought Chain: Detected new asset {file_path}. Adding to queue.")
                            span.add_event("new_asset_detected", {"file.path": file_path})
                span.set_status(trace.StatusCode.OK)
                span.set_attribute("new_assets.count", len(new_assets))
            except Exception as e:
                logger.error(f"Error during asset acquisition from {source_path}: {e}")
                span.set_status(trace.StatusCode.ERROR, description=f"Error during asset acquisition: {e}")
                span.record_exception(e)
            logger.debug(f"Thought Chain: Asset acquisition complete. Found {len(new_assets)} new assets.")
            return new_assets

    def format_detection_and_validation(self, asset_path, metadata, parent_span=None):
        """
        Performs technical checks on the asset format.
        Now includes logic to respect operator override for transcoding.
        Returns initial_transcode_required to indicate if transcode would have been needed without override.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("format_detection_and_validation",
                                                    kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("asset.path", asset_path)
            span.set_attribute("metadata.operator_override_transcode_needed",
                               metadata.get("operator_override_transcode_needed"))
            print(f"DEBUG: Entering format_detection_and_validation for {asset_path}")
            sys.stdout.flush()
            logger.info(f"Starting format detection for: {asset_path}")
            logger.debug(f"Thought Chain: Initiating FFprobe simulation for {asset_path}")

            print("DEBUG: Calling _simulate_ffprobe.")
            sys.stdout.flush()
            # Removed 'parent_span=span' from sub-span calls
            ffprobe_data = self._simulate_ffprobe(asset_path)
            print(f"DEBUG: _simulate_ffprobe returned: {ffprobe_data}")
            sys.stdout.flush()

            format_valid = True
            issues = []
            transcode_needed = False
            initial_transcode_required = False  # New flag to track if transcode was needed before override

            if not ffprobe_data:
                print("DEBUG: FFprobe data is empty.")
                sys.stdout.flush()
                format_valid = False
                issues.append("Could not get format information (FFprobe simulation failed).")
                logger.debug(
                    f"Thought Chain: FFprobe simulation failed. Justification: Simulated corrupted header or unreadable file. Format invalid.")
                span.set_attribute("format.ffprobe_failed", True)
                span.set_status(trace.StatusCode.ERROR, description="FFprobe simulation failed.")
                return format_valid, issues, transcode_needed, ffprobe_data, initial_transcode_required

            print("DEBUG: Checking file extension.")
            sys.stdout.flush()
            file_extension = ffprobe_data["format"]["format_name"]
            if file_extension not in self.config["policies"]["allowed_formats"]:
                format_valid = False
                issues.append(
                    f"Unsupported file format: {file_extension}. Allowed: {', '.join(self.config['policies']['allowed_formats'])}")
                transcode_needed = True
                initial_transcode_required = True  # Set this if transcode is needed
                logger.debug(
                    f"Thought Chain: Format '{file_extension}' is not allowed. Justification: Policy 'allowed_formats' violated. Flagging for transcode.")
                span.add_event("format_issue", {"type": "unsupported_extension", "extension": file_extension})
            else:
                logger.debug(
                    f"Thought Chain: File extension '{file_extension}' is allowed. Justification: Complies with 'allowed_formats' policy.")

            print("DEBUG: Checking video stream.")
            sys.stdout.flush()
            video_stream = next((s for s in ffprobe_data["streams"] if s["codec_type"] == "video"), None)
            if video_stream:
                if video_stream["codec_name"] not in ["h264", "hevc"]:
                    format_valid = False
                    issues.append(f"Unsupported video codec: {video_stream['codec_name']}.")
                    transcode_needed = True
                    initial_transcode_required = True  # Set this if transcode is needed
                    logger.debug(
                        f"Thought Chain: Video codec '{video_stream['codec_name']}' is unsupported. Justification: Policy violation. Flagging for transcode.")
                    span.add_event("format_issue",
                                   {"type": "unsupported_video_codec", "codec": video_stream['codec_name']})
                if video_stream["width"] < 1280 or video_stream["height"] < 720:
                    format_valid = False
                    issues.append(f"Low resolution: {video_stream['width']}x{video_stream['height']}. HD required.")
                    transcode_needed = True
                    initial_transcode_required = True  # Set this if transcode is needed
                    logger.debug(
                        f"Thought Chain: Resolution {video_stream['width']}x{video_stream['height']} is too low. Justification: Policy violation. Flagging for transcode.")
                    span.add_event("format_issue", {"type": "low_resolution",
                                                    "resolution": f"{video_stream['width']}x{video_stream['height']}"})
            else:
                format_valid = False
                issues.append("No video stream detected.")
                transcode_needed = True
                initial_transcode_required = True  # Set this if transcode is needed
                logger.debug(
                    f"Thought Chain: No video stream detected. Justification: Essential stream missing. Flagging for transcode.")
                span.add_event("format_issue", {"type": "no_video_stream"})

            print("DEBUG: Checking audio stream.")
            sys.stdout.flush()
            audio_stream = next((s for s in ffprobe_data["streams"] if s.get("codec_type") == "audio"), None)
            if audio_stream:
                if audio_stream.get("codec_name") not in ["aac", "ac3"]:
                    format_valid = False
                    issues.append(f"Unsupported audio codec: {audio_stream.get('codec_name')}.")
                    transcode_needed = True
                    initial_transcode_required = True  # Set this if transcode is needed
                    logger.debug(
                        f"Thought Chain: Audio codec '{audio_stream.get('codec_name')}' is unsupported. Justification: Policy violation. Flagging for transcode.")
                    span.add_event("format_issue",
                                   {"type": "unsupported_audio_codec", "codec": audio_stream.get('codec_name')})
            else:
                format_valid = False
                issues.append("No audio stream detected.")
                transcode_needed = True
                initial_transcode_required = True  # Set this if transcode is needed
                logger.debug(
                    f"Thought Chain: No audio stream detected. Justification: Essential stream missing. Flagging for transcode.")
                span.add_event("format_issue", {"type": "no_audio_stream"})

            print("DEBUG: Applying operator override logic.")
            sys.stdout.flush()
            # Apply operator override logic
            if metadata.get("operator_override_transcode_needed") is True:
                if initial_transcode_required:  # Only override if transcode was actually needed
                    transcode_needed = False  # Operator says: don't transcode, just pass it
                    issues.append("Transcode requirement overridden by operator decision.")
                    logger.info(f"Operator override detected: Transcoding for {asset_path} is skipped as instructed.")
                    logger.debug(
                        f"Thought Chain: Operator override applied. Justification: Explicit operator instruction to bypass transcode for non-compliant format.")
                    span.set_attribute("transcode.overridden", True)
                else:
                    logger.debug(
                        f"Thought Chain: Operator override for transcode present, but not applicable as transcode was not initially needed. Justification: Override is irrelevant without initial transcode flag.")
            else:
                logger.debug(
                    f"Thought Chain: No operator override for transcode detected. Justification: Proceeding with standard transcode decision.")

            if transcode_needed:
                logger.warning(
                    f"Format non-compliant for {asset_path}. Transcode job triggered (simulated). Issues: {issues}")
                logger.debug(
                    f"Thought Chain: Decision - Transcode needed due to format issues. Justification: Format issues detected and no override present.")
                span.set_attribute("transcode.needed", True)
                span.set_status(trace.StatusCode.ERROR, description="Format non-compliant, transcode needed.")
            elif not format_valid:
                logger.warning(f"Format issues detected for {asset_path}. Issues: {issues}")
                logger.debug(
                    f"Thought Chain: Decision - Format issues detected, but not triggering transcode (policy dependent). Justification: Format invalid but not requiring transcode based on policy or override.")
                span.set_status(trace.StatusCode.ERROR, description="Format issues detected.")
            else:
                logger.info(f"Format validation successful for {asset_path}.")
                logger.debug(
                    f"Thought Chain: Decision - Format is compliant. No transcode needed. Justification: All format policies passed.")
                span.set_status(trace.StatusCode.OK)

            span.set_attribute("format.valid", format_valid)
            span.set_attribute("format.issues", json.dumps(issues))
            span.set_attribute("transcode.initial_required", initial_transcode_required)

            print("DEBUG: Exiting format_detection_and_validation.")
            sys.stdout.flush()
            return format_valid, issues, transcode_needed, ffprobe_data, initial_transcode_required

    def metadata_extraction_and_enrichment(self, asset_path, ffprobe_data, parent_span=None):
        """
        Extracts embedded/sidecar metadata and enriches it.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("metadata_extraction_and_enrichment",
                                                    kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("asset.path", asset_path)
            logger.info(f"Starting metadata extraction for: {asset_path}")
            logger.debug(f"Thought Chain: Initiating metadata extraction and enrichment for {asset_path}")
            metadata = {
                "source_file": os.path.basename(asset_path),
                "ingest_timestamp": datetime.now().isoformat(),
                "checksum_sha256": self._calculate_checksum(file_path=asset_path, parent_span=span),
                "file_size_bytes": ffprobe_data["format"].get("size"),
                "duration": ffprobe_data["format"].get("duration")
            }
            logger.debug(f"Thought Chain: Basic metadata extracted from file system and FFprobe data.")
            span.set_attribute("metadata.basic_extracted", True)

            base_name = os.path.splitext(asset_path)[0]
            sidecar_path = f"{base_name}.json"
            if os.path.exists(sidecar_path):
                with self.otel_tracer.start_as_current_span("read_sidecar_metadata",
                                                            kind=trace.SpanKind.INTERNAL) as s_sidecar:
                    try:
                        with open(sidecar_path, 'r') as f:
                            sidecar_metadata = json.load(f)
                            metadata.update(sidecar_metadata)
                        logger.info(f"Extracted metadata from sidecar: {sidecar_path}")
                        logger.debug(
                            f"Thought Chain: Sidecar JSON found and merged. Justification: External metadata source provided.")
                        s_sidecar.set_status(trace.StatusCode.OK)
                        s_sidecar.set_attribute("sidecar.found", True)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Could not parse sidecar JSON {sidecar_path}: {e}")
                        logger.debug(
                            f"Thought Chain: Failed to parse sidecar JSON. Justification: JSON syntax error. Proceeding without initial override flags.")
                        s_sidecar.set_status(trace.StatusCode.ERROR,
                                             description=f"JSONDecodeError reading sidecar: {e}")
                        s_sidecar.record_exception(e)
                    except Exception as e:
                        logger.warning(f"Error reading sidecar {sidecar_path}: {e}")
                        logger.debug(
                            f"Thought Chain: Error reading initial sidecar. Justification: File access issue. Proceeding without initial override flags.")
                        s_sidecar.set_status(trace.StatusCode.ERROR, description=f"Error reading sidecar: {e}")
                        s_sidecar.record_exception(e)
            else:
                logger.info(f"No sidecar JSON found for {asset_path}.")
                logger.debug(
                    f"Thought Chain: No sidecar JSON found. Justification: No external metadata provided. Proceeding without it.")
                span.set_attribute("sidecar.found", False)

            if "title" not in metadata:
                metadata["title"] = os.path.splitext(os.path.basename(asset_path))[0].replace('_', ' ').title()
                logger.debug(f"Thought Chain: Title not found. Justification: Inferred from filename as per policy.")
                span.set_attribute("metadata.title.inferred", True)
            else:
                logger.debug(
                    f"Thought Chain: Title already present in metadata. Justification: Existing metadata takes precedence.")
                span.set_attribute("metadata.title.inferred", False)

            # Removed 'parent_span=span' from sub-span calls
            asset_type = self._get_asset_type_from_path(file_path=asset_path)
            metadata["asset_type"] = asset_type
            logger.info(f"Determined asset type: {asset_type}")
            span.set_attribute("asset.type", asset_type)

            # Ensure _real_ai_enrichment always returns a dictionary
            # Pass the current span as parent_span for nested AI enrichment calls
            # Removed 'parent_span=span' from sub-span calls
            enriched_metadata_from_ai = self._real_ai_enrichment(metadata, asset_type)
            if enriched_metadata_from_ai is None:
                logger.error("AI enrichment returned None. Using original metadata for further processing.")
                final_metadata = metadata
                span.set_status(trace.StatusCode.ERROR, description="AI enrichment returned None.")
            else:
                final_metadata = enriched_metadata_from_ai
                span.set_status(trace.StatusCode.OK)

            logger.info(f"Metadata extraction and enrichment complete for {asset_path}.")
            logger.debug(f"Thought Chain: Metadata extraction and enrichment process concluded.")
            logger.debug(f"Final metadata: {json.dumps(final_metadata, indent=2)}")
            span.set_attribute("final.metadata", json.dumps(final_metadata))
            return final_metadata

    def content_normalization(self, metadata, parent_span=None):
        """
        Standardizes naming and performs other normalization tasks.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("content_normalization", kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("source.file", metadata.get('source_file', 'N/A'))
            logger.info(f"Starting content normalization for: {metadata.get('source_file', 'N/A')}")
            logger.debug(f"Thought Chain: Initiating content normalization for {metadata.get('source_file', 'N/A')}")

            original_file_name = metadata.get('source_file')
            asset_path = os.path.join(self.config["watch_folders"][0],
                                      original_file_name)

            file_extension = os.path.splitext(original_file_name)[1]

            asset_id = f"asset_{hashlib.md5(asset_path.encode()).hexdigest()[:8]}"
            normalized_name = f"{asset_id}_{metadata.get('title', 'untitled').replace(' ', '_').lower()}{file_extension}"
            normalized_path = os.path.join(self.config["output_folder"], normalized_name)
            logger.debug(
                f"Thought Chain: Determined normalized name: {normalized_name}. Justification: Standard naming convention applied.")
            span.set_attribute("normalized.name", normalized_name)
            span.set_attribute("normalized.path", normalized_path)

            try:
                if metadata.get("checksum_sha256") not in self.processed_checksums:
                    os.replace(asset_path, normalized_path)
                    logger.info(f"Normalized and moved {original_file_name} to {normalized_path}")
                    logger.debug(
                        f"Thought Chain: File successfully moved to normalized path. Justification: Unique content, moved to standard location. Adding checksum to processed set.")
                    self.processed_checksums.add(metadata["checksum_sha256"])
                    span.set_attribute("file.moved", True)
                    span.set_attribute("checksum.added_to_processed", metadata["checksum_sha256"])
                else:
                    logger.info(f"Skipping move for duplicate file {original_file_name}. Already processed content.")
                    logger.debug(
                        f"Thought Chain: Not moving duplicate file. Justification: Content hash matches existing processed asset. Content already exists in normalized form.")
                    normalized_path = "DUPLICATE_SKIPPED_MOVE"
                    normalized_name = "DUPLICATE_SKIPPED_MOVE"
                    span.set_attribute("file.moved", False)
                    span.set_attribute("duplicate.skipped_move", True)

            except Exception as e:
                logger.error(f"Error during content normalization (moving file) for {asset_path}: {e}")
                logger.debug(
                    f"Thought Chain: File move failed. Justification: File system error during move. Normalization failed.")
                span.set_status(trace.StatusCode.ERROR, description=f"Error during file move: {e}")
                span.record_exception(e)
                return None, metadata

            metadata["normalized_file_path"] = normalized_path
            metadata["normalized_file_name"] = normalized_name

            logger.info(f"Content normalization complete for {asset_path}.")
            logger.debug(f"Thought Chain: Content normalization process concluded.")
            span.set_status(trace.StatusCode.OK)
            return normalized_path, metadata

    def policy_compliance_check(self, metadata, parent_span=None):
        """
        Checks asset against predefined policy rules.
        Updates metadata completeness KPI.
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("policy_compliance_check", kind=trace.SpanKind.INTERNAL) as span:
            span.set_attribute("asset.title", metadata.get('title', 'N/A'))
            logger.info(f"Starting policy compliance check for asset: {metadata.get('title', 'N/A')}")
            logger.debug(f"Thought Chain: Initiating policy compliance check.")
            compliance_status = True
            compliance_flags = []
            escalate_to_human = False

            total_required_for_asset = len(self.config["policies"]["required_metadata_fields"])
            present_required_for_asset = 0
            self.total_metadata_fields_expected += total_required_for_asset

            for field in self.config["policies"]["required_metadata_fields"]:
                if field in metadata and metadata[field]:
                    present_required_for_asset += 1
                    logger.debug(
                        f"Thought Chain: Required field '{field}' is present. Justification: Metadata completeness criteria met for this field.")
                else:
                    compliance_flags.append(f"Missing required metadata field: {field}.")
                    compliance_status = False
                    escalate_to_human = True
                    logger.debug(
                        f"Thought Chain: Missing required field '{field}'. Justification: Policy 'required_metadata_fields' violated. Escalation triggered.")
                    span.add_event("compliance_issue", {"type": "missing_metadata", "field": field})
            self.total_metadata_fields_present += present_required_for_asset
            span.set_attribute("metadata.required_fields.present", present_required_for_asset)
            span.set_attribute("metadata.required_fields.total", total_required_for_asset)

            duration = metadata.get("duration")
            span.set_attribute("asset.duration", duration)
            span.set_attribute("asset.type", metadata.get('asset_type', 'N/A'))
            logger.debug(
                f"Thought Chain: Checking duration policy. Duration: {duration:.2f}s, Asset Type: {metadata.get('asset_type', 'N/A')}")
            if duration is not None:
                if metadata["asset_type"] == "ad":
                    if duration > self.config["policies"]["ad_duration_limit_seconds"]:
                        compliance_flags.append(
                            f"Ad duration ({duration:.2f}s) exceeds limit ({self.config['policies']['ad_duration_limit_seconds']}s).")
                        compliance_status = False
                        escalate_to_human = True
                        logger.debug(
                            "Thought Chain: Ad duration policy violated. Justification: Duration exceeds ad-specific limit. Escalation triggered.")
                        span.add_event("compliance_issue", {"type": "ad_duration_exceeded", "duration": duration})
                else:
                    if duration < self.config["policies"]["min_duration_seconds"]:
                        compliance_flags.append(
                            f"Duration ({duration:.2f}s) below minimum ({self.config['policies']['min_duration_seconds']}s).")
                        compliance_status = False
                        escalate_to_human = True
                        logger.debug(
                            f"Thought Chain: General content duration below minimum. Justification: Duration too short. Escalation triggered.")
                        span.add_event("compliance_issue", {"type": "min_duration_violated", "duration": duration})
                    if duration > self.config["policies"]["max_duration_seconds"]:
                        compliance_flags.append(
                            f"Duration ({duration:.2f}s) exceeds maximum ({self.config['policies']['max_duration_seconds']}s).")
                        compliance_status = False
                        escalate_to_human = True
                        logger.debug(
                            f"Thought Chain: General content duration exceeds maximum. Justification: Duration too long. Escalation triggered.")
                        span.add_event("compliance_issue", {"type": "max_duration_exceeded", "duration": duration})
            else:
                compliance_flags.append("Duration metadata missing.")
                compliance_status = False
                escalate_to_human = True
                logger.debug(
                    f"Thought Chain: Duration metadata missing. Justification: Cannot check policy without duration. Escalation triggered.")
                span.add_event("compliance_issue", {"type": "missing_duration"})

            file_size_mb = metadata.get("file_size_bytes", 0) / (1024 * 1024)
            span.set_attribute("file.size_mb", file_size_mb)
            logger.debug(
                f"Thought Chain: Checking file size policy. Size: {file_size_mb:.2f} MB, Max: {self.config['policies']['max_file_size_mb']} MB.")
            if file_size_mb > self.config["policies"]["max_file_size_mb"]:
                compliance_flags.append(
                    f"File size ({file_size_mb:.2f} MB) exceeds maximum allowed ({self.config['policies']['max_file_size_mb']} MB).")
                compliance_status = False
                escalate_to_human = True
                logger.debug(
                    f"Thought Chain: File size policy violated. Justification: File size exceeds configured limit. Escalation triggered.")
                span.add_event("compliance_issue", {"type": "file_size_exceeded", "size_mb": file_size_mb})
            else:
                logger.debug(
                    f"Thought Chain: File size is within allowed limits. Justification: Complies with file size policy.")

            logger.debug(f"Thought Chain: Checking E/I rating for kids content.")
            if metadata.get("asset_type") == "episode" and metadata.get("genre", "").lower() == "kids":
                if not metadata.get("ei_rating"):
                    compliance_flags.append("Kids content missing E/I rating.")
                    compliance_status = False
                    escalate_to_human = True
                    logger.debug(
                        f"Thought Chain: Kids content missing E/I rating. Justification: Policy requires E/I rating for kids content. Escalation triggered.")
                    span.add_event("compliance_issue", {"type": "missing_ei_rating"})
                else:
                    logger.debug(
                        f"Thought Chain: Kids content has E/I rating. Justification: Complies with E/I rating policy.")

            if not compliance_status:
                logger.warning(
                    f"Policy compliance issues for {metadata.get('title', 'N/A')}. Flags: {compliance_flags}")
                logger.debug(
                    f"Thought Chain: Decision - Policy compliance failed. Status: FAIL. Justification: One or more policy flags were set.")
                if escalate_to_human:
                    logger.error(f"Escalating asset {metadata.get('title', 'N/A')} to human for review.")
                    logger.debug(
                        f"Thought Chain: Decision - Escalating to human due to critical policy breach. Justification: Escalation flag set by policy check.")
                    span.set_attribute("escalate_to_human", True)
                span.set_status(trace.StatusCode.ERROR, description="Policy compliance failed.")
            else:
                logger.info(f"Policy compliance check passed for {metadata.get('title', 'N/A')}.")
                logger.debug(
                    f"Thought Chain: Decision - Policy compliance passed. Status: PASS. Justification: All policy checks passed without flags.")
                span.set_status(trace.StatusCode.OK)

            metadata["compliance_status"] = "PASS" if compliance_status else "FAIL"
            metadata["compliance_flags"] = compliance_flags
            metadata["escalate_to_human"] = escalate_to_human
            span.set_attribute("compliance.status", metadata["compliance_status"])
            span.set_attribute("compliance.flags", json.dumps(compliance_flags))
            logger.debug(f"Thought Chain: Policy compliance check concluded.")
            return compliance_status, compliance_flags, escalate_to_human, metadata

    def event_driven_hand_off(self, processed_asset_info, parent_span=None):
        """
        Triggers hand-off to downstream agents (simulated by printing).
        """
        # Removed 'parent=parent_span' from start_as_current_span
        with self.otel_tracer.start_as_current_span("event_driven_hand_off", kind=trace.SpanKind.PRODUCER) as span:
            span.set_attribute("asset.normalized_file_name", processed_asset_info.get('normalized_file_name', 'N/A'))
            logger.info(
                f"Handing off asset to downstream agents: {processed_asset_info.get('normalized_file_name', 'N/A')}")
            logger.debug(f"Thought Chain: Initiating hand-off process.")
            logger.info(f"Context Shared (simulated hand-off):\n{json.dumps(processed_asset_info, indent=2)}")
            span.set_attribute("handoff.payload", json.dumps(processed_asset_info))

            if processed_asset_info.get("compliance_status") == "FAIL":
                logger.warning(
                    "Asset handed off with compliance failures. Downstream agents should handle accordingly.")
                logger.debug(
                    "Thought Chain: Hand-off includes compliance failures. Justification: Asset has compliance flags. Notifying downstream for conditional processing.")
                span.set_status(trace.StatusCode.ERROR, description="Hand-off with compliance failures.")
                span.set_attribute("handoff.compliance_failed", True)
            else:
                logger.debug(
                    "Thought Chain: Hand-off completed successfully. Justification: Asset is compliant or issues were handled internally.")
                span.set_status(trace.StatusCode.OK)
            logger.info("Hand-off complete.")

    def ingest_asset_pipeline(self, asset_path, parent_span=None):
        """
        Orchestrates the entire ingestion pipeline for a single asset.
        Tracks ingest success rate and handoff latency.
        """
        # Ensure the parent_span context is active for this function
        token = None
        if parent_span:
            token = attach(trace.set_span_in_context(parent_span))

        # Initialize processed_asset_info outside try block to ensure it's always defined
        processed_asset_info = {
            "original_path": asset_path,
            "ingest_start_time": datetime.now().isoformat(),
            "status": "IN_PROGRESS"
        }

        span = None  # Initialize span to None
        try:
            # Removed 'parent=parent_span' from start_as_current_span
            with self.otel_tracer.start_as_current_span("ingest_asset_pipeline",
                                                        kind=trace.SpanKind.SERVER) as span_context:
                span = span_context  # Assign the span object to the outer 'span' variable
                print(f"DEBUG: Entering ingest_asset_pipeline (first line) for {asset_path}")
                sys.stdout.flush()
                self.total_ingests += 1
                logger.info(f"--- Starting ingestion pipeline for: {asset_path} ---")
                logger.debug(f"Thought Chain: Pipeline started for {asset_path}.")
                span.set_attribute("asset.path", asset_path)

                initial_metadata = {}
                base_name = os.path.splitext(asset_path)[0]
                sidecar_path = f"{base_name}.json"
                if os.path.exists(sidecar_path):
                    # Removed 'parent=span' from sub-span calls
                    with self.otel_tracer.start_as_current_span("load_initial_sidecar_metadata",
                                                                kind=trace.SpanKind.INTERNAL) as s_initial_sidecar:
                        try:
                            with open(sidecar_path, 'r') as f:
                                initial_metadata = json.load(f)
                            logger.debug(f"Thought Chain: Loaded initial metadata from sidecar for override checks.")
                            s_initial_sidecar.set_status(trace.StatusCode.OK)
                            s_initial_sidecar.set_attribute("sidecar.initial_loaded", True)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Could not parse sidecar JSON {sidecar_path}: {e}")
                            logger.debug(
                                f"Thought Chain: Failed to parse initial sidecar JSON. Justification: JSON syntax error. Proceeding without initial override flags.")
                            s_initial_sidecar.set_status(trace.StatusCode.ERROR,
                                                         description=f"JSONDecodeError loading initial sidecar: {e}")
                            s_initial_sidecar.record_exception(e)
                        except Exception as e:
                            logger.warning(f"Error reading sidecar {sidecar_path}: {e}")
                            logger.debug(
                                f"Thought Chain: Error reading initial sidecar. Justification: File access issue. Proceeding without initial override flags.")
                            s_initial_sidecar.set_status(trace.StatusCode.ERROR,
                                                         description=f"Error reading initial sidecar: {e}")
                            s_initial_sidecar.record_exception(e)
                else:
                    logger.info(f"No initial sidecar JSON found for {asset_path}.")
                    span.set_attribute("sidecar.initial_loaded", False)

                # 1. Format Detection & Validation
                print("DEBUG: ingest_asset_pipeline - Step 1: Format Detection & Validation.")
                sys.stdout.flush()
                logger.debug("Thought Chain: Executing Format Detection & Validation step.")
                # Removed 'parent_span=span' from sub-span calls
                format_valid, format_issues, transcode_needed, ffprobe_data, initial_transcode_required = \
                    self.format_detection_and_validation(asset_path, initial_metadata)
                processed_asset_info.update({
                    "format_valid": format_valid,
                    "format_issues": format_issues,
                    "transcode_needed": transcode_needed,
                    "initial_transcode_required": initial_transcode_required,
                    "ffprobe_data": ffprobe_data
                })
                span.set_attribute("pipeline.step1.status", "completed")

                if not format_valid:
                    if initial_transcode_required and initial_metadata.get(
                            "operator_override_transcode_needed") is True:
                        processed_asset_info["status"] = "COMPLETED_WITH_OVERRIDE"
                        logger.info(
                            f"Ingestion for {asset_path} proceeds due to operator override despite format issues.")
                        span.set_attribute("pipeline.status", "COMPLETED_WITH_OVERRIDE")
                    elif initial_transcode_required and not initial_metadata.get("operator_override_transcode_needed"):
                        processed_asset_info["status"] = "TRANSCODE_NEEDED"
                        logger.info(f"Ingestion for {asset_path} completed, but transcode is needed.")
                        span.set_attribute("pipeline.status", "TRANSCODE_NEEDED")
                    else:
                        processed_asset_info["status"] = "FAILED_FORMAT_VALIDATION"
                        logger.error(
                            f"Ingestion failed for {asset_path}: Format invalid and transcode not applicable/possible.")
                        # Removed 'parent_span=span' from sub-span calls
                        self.event_driven_hand_off(processed_asset_info)
                        span.set_status(trace.StatusCode.ERROR, description="Failed format validation.")
                        span.set_attribute("pipeline.status", "FAILED_FORMAT_VALIDATION")
                        return processed_asset_info
                else:
                    logger.info(f"Format validation successful for {asset_path}.")
                    processed_asset_info["status"] = "IN_PROGRESS"
                    span.set_attribute("pipeline.status", "IN_PROGRESS")

                # 2. Metadata Extraction & Enrichment
                print("DEBUG: ingest_asset_pipeline - Step 2: Metadata Extraction & Enrichment.")
                sys.stdout.flush()
                logger.debug("Thought Chain: Executing Metadata Extraction & Enrichment step.")
                # Removed 'parent_span=span' from sub-span calls
                metadata = self.metadata_extraction_and_enrichment(asset_path, ffprobe_data)
                if "operator_override_transcode_needed" in initial_metadata:
                    metadata["operator_override_transcode_needed"] = initial_metadata[
                        "operator_override_transcode_needed"]
                    logger.debug(
                        "Thought Chain: Carried forward 'operator_override_transcode_needed' flag to final metadata. Justification: Preserving operator instruction.")
                processed_asset_info["metadata"] = metadata
                span.set_attribute("pipeline.step2.status", "completed")

                # 3. Content Normalization
                print("DEBUG: ingest_asset_pipeline - Step 3: Content Normalization.")
                sys.stdout.flush()
                logger.debug("Thought Chain: Executing Content Normalization step.")
                # Removed 'parent_span=span' from sub-span calls
                normalized_path, updated_metadata = self.content_normalization(metadata)
                if not normalized_path or normalized_path == "DUPLICATE_SKIPPED_MOVE":
                    if normalized_path == "DUPLICATE_SKIPPED_MOVE":
                        processed_asset_info["status"] = "DUPLICATE_SKIPPED"
                        logger.info(f"Ingestion pipeline skipped for {asset_path}: Duplicate content detected.")
                        logger.debug(
                            "Thought Chain: Decision - Pipeline terminated due to duplicate content. Justification: Asset content already exists in the system.")
                        span.set_attribute("pipeline.status", "DUPLICATE_SKIPPED")
                    else:
                        processed_asset_info["status"] = "FAILED_NORMALIZATION"
                        logger.error(f"Ingestion failed for {asset_path}: Content normalization failed.")
                        logger.debug(
                            "Thought Chain: Decision - Pipeline terminated due to normalization failure. Justification: File could not be moved or renamed.")
                        span.set_attribute("pipeline.status", "FAILED_NORMALIZATION")
                    # Removed 'parent_span=span' from sub-span calls
                    self.event_driven_hand_off(processed_asset_info)
                    span.set_status(trace.StatusCode.ERROR, description="Failed normalization.")
                    return processed_asset_info
                processed_asset_info["normalized_file_path"] = normalized_path
                processed_asset_info["metadata"] = updated_metadata
                span.set_attribute("pipeline.step3.status", "completed")

                # 4. Policy Compliance Check
                print("DEBUG: ingest_asset_pipeline - Step 4: Policy Compliance Check.")
                sys.stdout.flush()
                logger.debug("Thought Chain: Executing Policy Compliance Check step.")
                # Removed 'parent_span=span' from sub-span calls
                compliance_status, compliance_flags, escalate_to_human, final_metadata = \
                    self.policy_compliance_check(updated_metadata)
                processed_asset_info["metadata"] = final_metadata
                span.set_attribute("pipeline.step4.status", "completed")

                if processed_asset_info["status"] == "COMPLETED_WITH_OVERRIDE":
                    if not compliance_status:
                        processed_asset_info["status"] = "COMPLETED_WITH_OVERRIDE_AND_COMPLIANCE_ISSUES"
                        processed_asset_info["compliance_flags"].extend(compliance_flags)
                        processed_asset_info["escalate_to_human"] = True
                        span.set_attribute("pipeline.status", "COMPLETED_WITH_OVERRIDE_AND_COMPLIANCE_ISSUES")
                elif processed_asset_info["status"] == "TRANSCODE_NEEDED":
                    if not compliance_status:
                        processed_asset_info["status"] = "TRANSCODE_NEEDED_AND_FAILED_COMPLIANCE"
                        processed_asset_info["compliance_flags"].extend(compliance_flags)
                        processed_asset_info["escalate_to_human"] = True
                        span.set_attribute("pipeline.status", "TRANSCODE_NEEDED_AND_FAILED_COMPLIANCE")
                elif processed_asset_info["status"] == "IN_PROGRESS":
                    processed_asset_info["status"] = "COMPLETED" if compliance_status else "FAILED_COMPLIANCE"
                    span.set_attribute("pipeline.status", processed_asset_info["status"])

                if escalate_to_human:
                    processed_asset_info["escalate_to_human"] = True
                    span.set_attribute("pipeline.escalate_to_human", True)

                processed_asset_info["ingest_end_time"] = datetime.now().isoformat()
                if processed_asset_info["status"] in ["COMPLETED", "COMPLETED_WITH_OVERRIDE",
                                                      "COMPLETED_WITH_OVERRIDE_AND_COMPLIANCE_ISSUES"]:
                    self.successful_ingests += 1
                    span.set_status(trace.StatusCode.OK)
                else:
                    span.set_status(trace.StatusCode.ERROR,
                                    description=f"Pipeline finished with status: {processed_asset_info['status']}")

                # 5. Event-Driven Hand-off
                print("DEBUG: ingest_asset_pipeline - Step 5: Event-Driven Hand-off.")
                sys.stdout.flush()
                logger.debug("Thought Chain: Executing Event-Driven Hand-off step.")
                # Removed 'parent_span=span' from sub-span calls
                self.event_driven_hand_off(processed_asset_info)
                span.set_attribute("pipeline.step5.status", "completed")

                logger.info(f"--- Ingestion pipeline completed for: {asset_path} ---")
                logger.debug(f"Thought Chain: Pipeline completed successfully for {asset_path}.")
                return processed_asset_info

        except Exception as e:
            logger.critical(f"Unhandled error in ingestion pipeline for {asset_path}: {e}", exc_info=True)
            processed_asset_info["status"] = "CRITICAL_FAILURE"
            processed_asset_info["error_message"] = str(e)
            processed_asset_info["ingest_end_time"] = datetime.now().isoformat()

            # Ensure 'span' is accessible and update its status if it exists
            if span:  # Check if span was successfully created
                span.set_status(trace.StatusCode.ERROR, description=f"Critical failure: {e}")
                span.record_exception(e)
                span.set_attribute("pipeline.status", "CRITICAL_FAILURE")
            else:
                logger.error("Error occurred before OpenTelemetry span could be created or accessed.")

            # Removed 'parent_span=parent_span' from sub-span calls
            self.event_driven_hand_off(processed_asset_info)
            return processed_asset_info

        finally:
            # Detach the context if it was attached
            if token:
                detach(token)

    def _report_kpis(self):
        # This method is no longer strictly needed as Langfuse captures metrics
        logger.info("\n--- Ingest Agent KPI Report (Deprecated with Langfuse) ---")
        logger.info("KPIs are now primarily viewed in the Langfuse dashboard.")
        logger.info("--- KPI Report End ---\n")

    def run(self, interval_seconds=5):
        logger.info(f"Ingest Agent starting to monitor watch folders every {interval_seconds} seconds...")
        logger.debug("Thought Chain: Entering main monitoring loop.")
        while True:
            for folder in self.config["watch_folders"]:
                logger.debug(f"Thought Chain: Checking watch folder: {folder}")
                new_assets = self.asset_acquisition(folder)
                for asset_path in new_assets:
                    # In a continuous monitoring loop, each asset processing could be a new trace
                    # or part of a longer-running trace. For simplicity, we'll assume orchestrator
                    # manages the trace context.
                    # This `run` method is typically not called directly in the orchestrated setup.
                    pass  # The orchestration script will call ingest_asset_pipeline directly


# --- Main execution block ---
if __name__ == "__main__":
    # This block is for direct execution of ingest_agent.py, not used when imported by orchestrator
    # It will not send traces to Langfuse unless a TracerProvider is explicitly set up here.
    if os.path.exists("./ingest_source"):
        for f in os.listdir("./ingest_source"):
            os.remove(os.path.join("./ingest_source", f))
    if os.path.exists("./ingest_processed"):
        for f in os.listdir("./ingest_processed"):
            os.remove(os.path.join("./ingest_processed", f))
    if os.path.exists("ingest_agent.log"):
        os.remove("ingest_agent.log")

    os.makedirs("./ingest_source", exist_ok=True)

    logger.info("--- Starting Ingest Agent Standalone Test (No OpenTelemetry Tracing Here) ---")
    duplicate_content = b"This is the unique content for the duplicate test files."

    file_name_orig = "original_content_video.mp4"
    file_path_orig = os.path.join("./ingest_source", file_name_orig)
    with open(file_path_orig, "wb") as f:
        f.write(duplicate_content)
    logger.info(f"Created initial duplicate test file: {file_name_orig}")

    agent = IngestAgent(CONFIG)

    # Pass None for parent_span as no OTel client is initialized here for standalone test
    first_asset_detected = agent.asset_acquisition("./ingest_source", parent_span=None)
    if first_asset_detected:
        agent.ingest_asset_pipeline(first_asset_detected[0], parent_span=None)
    logger.info(f"Finished processing initial duplicate file: {file_name_orig}")
    time.sleep(1)

    file_name_dup1 = "duplicate_content_version_1.mp4"
    file_path_dup1 = os.path.join("./ingest_source", file_name_dup1)
    with open(file_path_dup1, "wb") as f:
        f.write(duplicate_content)
    logger.info(f"Created first duplicate test file: {file_name_dup1}")

    file_name_dup2 = "another_copy_of_content.mp4"
    file_path_dup2 = os.path.join("./ingest_source", file_name_dup2)
    with open(file_path_dup2, "wb") as f:
        f.write(duplicate_content)
    logger.info(f"Created second duplicate test file: {file_name_dup2}\n")
    time.sleep(1)

    logger.info("--- Scanning for newly added duplicate files ---")
    remaining_assets = agent.asset_acquisition("./ingest_source", parent_span=None)
    for asset_path in remaining_assets:
        agent.ingest_asset_pipeline(asset_path, parent_span=None)

    logger.info("--- Duplicate File Uploads Test Completed ---")

    logger.info("\n--- Starting Other Test Cases ---")
    if os.path.exists("./ingest_source"):
        for f in os.listdir("./ingest_source"):
            os.remove(os.path.join("./ingest_source", f))
    if os.path.exists("./ingest_processed"):
        for f in os.listdir("./ingest_processed"):
            os.remove(os.path.join("./ingest_processed", f))

    with open("./ingest_source/my_episode_s01e01.mp4", "w") as f:
        f.write("This is a dummy video file content for testing.")
    with open("./ingest_source/my_episode_s01e01.json", "w") as f:
        json.dump({"title": "My First Episode", "genre": "Sci-Fi", "language": "English", "ei_rating": "E"}, f,
                  indent=2)

    with open("./ingest_source/bad_format_video.avi", "w") as f:
        f.write("This is a dummy AVI file, which is not allowed.")

    with open("./ingest_source/long_ad.mp4", "wb") as f:
        f.write(b"A" * (1024 * 1024 * 101))

    with open("./ingest_source/ad_missing_meta.mp4", "w") as f:
        f.write("This is a dummy ad file with intentionally missing metadata.")

    with open("./ingest_source/corrupted_header.mp4", "wb") as f:
        os.urandom(50)

    with open("./ingest_source/oversized_file_test.mp4", "wb") as f:
        f.write(b"This file is small, but will be simulated as 50GB.")

    with open("./ingest_source/historical_anomaly.mp4", "w") as f:
        f.write("This is a dummy video file about a historical anomaly.")
    with open("./ingest_source/historical_anomaly.json", "w") as f:
        json.dump({"title": "The Great Fire of London (1966)", "genre": "Historical Documentary", "language": "English",
                   "description": "A documentary detailing the devastating fire that swept through London in the year 1966, causing widespread destruction.",
                   "historical_period": "20th Century"}, f, indent=2)

    logger.info("Other dummy test files created in ./ingest_source.")

    other_assets = agent.asset_acquisition("./ingest_source", parent_span=None)
    for asset_path in other_assets:
        agent.ingest_asset_pipeline(asset_path, parent_span=None)

    logger.info("--- Other Test Cases Completed ---")

    logger.info("Ingest Agent finished processing all test files.")
    logger.info(f"Check '{CONFIG['log_file']}' for detailed logs and '{CONFIG['output_folder']}' for processed files.")

    agent._report_kpis()  # Call for standalone report

