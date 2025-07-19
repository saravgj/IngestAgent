import json
import hashlib
import time
from datetime import datetime
import asyncio
from typing import List
import requests
import os
import logging

# OpenTelemetry Imports
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.context import attach, detach
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Ragas Imports
from ragas.llms.base import BaseRagasLLM, Generation
from ragas.embeddings.base import BaseRagasEmbeddings
from ragas.metrics import answer_relevancy
from datasets import Dataset
from ragas import evaluate
from langchain_core.prompt_values import PromptValue
import pandas as pd

# Configuration
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

# Setup Logging
logger = logging.getLogger("IngestAgent")
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


# Gemini LLM Implementation
class GeminiRagasLLM(BaseRagasLLM):
    def __init__(self):
        super().__init__()
        self.api_key = "AIzaSyCT2S1jv98BwcbJ1ti9-YWUy2Pp2Ms2Em0"
        self.model_name = "gemini-1.5-pro-latest"
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models/"

    def _get_api_url(self):
        return f"{self.base_url}{self.model_name}:generateContent?key={self.api_key}"

    def _process_prompt(self, prompt):
        """Handle all prompt formats that Ragas might send"""
        if isinstance(prompt, PromptValue):
            return prompt.text
        elif isinstance(prompt, str):
            return prompt
        elif isinstance(prompt, dict) and 'text' in prompt:
            return prompt['text']
        elif isinstance(prompt, tuple) and len(prompt) == 2:
            return str(prompt[1])
        else:
            logger.error(f"Unexpected prompt type: {type(prompt)}. Content: {str(prompt)[:200]}")
            return str(prompt)

    def generate_text(self, prompt: str, **kwargs) -> str:
        """Synchronous text generation"""
        actual_prompt = self._process_prompt(prompt)
        logger.debug(f"Processing prompt: {actual_prompt[:200]}...")

        try:
            response = requests.post(
                self._get_api_url(),
                headers={'Content-Type': 'application/json'},
                json={
                    "contents": [{
                        "parts": [{"text": actual_prompt}]
                    }]
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()

            if result.get("candidates") and result["candidates"][0].get("content"):
                return result["candidates"][0]["content"]["parts"][0]["text"]
            logger.error("Unexpected response format from Gemini API")
            return ""
        except requests.exceptions.RequestException as e:
            logger.error(f"Gemini API request failed: {e}")
            return ""
        except Exception as e:
            logger.critical(f"Unexpected error in generate_text: {e}", exc_info=True)
            return ""

    async def agenerate_text(self, prompt: str, **kwargs) -> str:
        """Async text generation"""
        return await asyncio.to_thread(self.generate_text, prompt)

    def generate(self, prompts: List[PromptValue], **kwargs) -> List[Generation]:
        """Batch generation of responses"""
        return [Generation(text=self.generate_text(prompt)) for prompt in prompts]

    async def agenerate(self, prompts: List[PromptValue], **kwargs) -> List[Generation]:
        """Async batch generation"""
        tasks = [self.agenerate_text(prompt) for prompt in prompts]
        responses = await asyncio.gather(*tasks)
        return [Generation(text=response) for response in responses]


# Gemini Embeddings Implementation
class GeminiRagasEmbeddings(BaseRagasEmbeddings):
    def __init__(self):
        super().__init__()
        self.api_key = os.environ.get("GOOGLE_API_KEY", "")
        if not self.api_key:
            logger.warning("GOOGLE_API_KEY environment variable not set")

    def _call_gemini_embed_api(self, text: str) -> List[float]:
        """Internal method to call Gemini Embedding API"""
        try:
            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/embedding-001:embedContent?key={self.api_key}",
                headers={'Content-Type': 'application/json'},
                json={
                    "model": {"baseModel": "embedding-001"},
                    "content": {"parts": [{"text": text}]}
                },
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            if result and result.get("embedding"):
                return result["embedding"]["values"]
            logger.error("Gemini Embedding API response missing expected content structure")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling Gemini Embedding API: {e}")
            return []
        except Exception as e:
            logger.critical(f"Unexpected error during Gemini Embedding API call: {e}", exc_info=True)
            return []

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Synchronously embed a list of documents"""
        return [self._call_gemini_embed_api(text) for text in texts]

    def embed_query(self, text: str) -> List[float]:
        """Synchronously embed a single query text"""
        return self._call_gemini_embed_api(text)

    async def aembed_documents(self, texts: List[str]) -> List[List[float]]:
        """Asynchronously embed a list of documents"""
        return await asyncio.to_thread(self.embed_documents, texts)

    async def aembed_query(self, text: str) -> List[float]:
        """Asynchronously embed a single query text"""
        return await asyncio.to_thread(self.embed_query, text)


# Main Ingest Agent Class
class IngestAgent:
    def __init__(self, config):
        self.config = config
        self.kpi_tracker = {
            "total_ingests": 0,
            "successful_ingests": 0,
            "total_gemini_calls": 0,
            "successful_gemini_calls": 0,
            "total_metadata_fields_expected": 0,
            "total_metadata_fields_present": 0,
            "total_handoff_latency": 0.0,
            "handoff_count": 0,
            "duplicate_asset_count": 0,
            "retry_attempts": 0,
            "ragas_score_avg": "N/A"
        }
        self.processed_files = set()
        self.processed_checksums = set()

        # Setup OpenTelemetry
        self.otel_tracer = trace.get_tracer(__name__)

        # Initialize Gemini components
        self.gemini_ragas_llm = GeminiRagasLLM()
        self.gemini_ragas_embeddings = GeminiRagasEmbeddings()

        # Setup directories
        self._setup_directories()
        logger.info("Ingest Agent initialized")

    def _setup_directories(self):
        """Ensure watch and output directories exist"""
        for folder in self.config["watch_folders"]:
            os.makedirs(folder, exist_ok=True)
            logger.info(f"Ensured watch folder exists: {folder}")
        os.makedirs(self.config["output_folder"], exist_ok=True)
        logger.info(f"Ensured output folder exists: {self.config['output_folder']}")

    def _calculate_checksum(self, file_path, algorithm="sha256", parent_span=None):
        """Calculate file checksum"""
        with self.otel_tracer.start_as_current_span("_calculate_checksum") as span:
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
        """Simulate FFprobe output"""
        with self.otel_tracer.start_as_current_span("_simulate_ffprobe") as span:
            span.set_attribute("file.path", file_path)
            file_extension = os.path.splitext(file_path)[1].lower().lstrip('.')
            file_size_bytes = os.path.getsize(file_path)
            file_name_lower = os.path.basename(file_path).lower()

            if "corrupted_header" in file_name_lower:
                logger.warning(f"Simulating FFprobe failure for {file_path}")
                span.set_attribute("ffprobe.simulated_failure", True)
                span.set_status(trace.StatusCode.ERROR, description="Simulated corrupted header")
                return {}

            simulated_size_bytes = file_size_bytes
            if "oversized_file_test" in file_name_lower:
                simulated_size_bytes = 50 * 1024 * 1024 * 1024
                logger.warning(f"Simulating FFprobe reporting 50GB size for {file_path}")
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

    async def _real_ai_enrichment(self, metadata, content_type="episode", parent_span=None):
        """Perform AI-based metadata enrichment with Ragas evaluation"""
        with self.otel_tracer.start_as_current_span("_real_ai_enrichment") as span:
            enriched_metadata = metadata.copy()
            ragas_data = []

            # Skip if minimal metadata
            if not metadata.get('title'):
                self.kpi_tracker["ragas_score_avg"] = "SKIPPED"
                return enriched_metadata

            # 1. AI Summary Generation
            summary_prompt = f"""Generate a concise summary for:
            Title: {metadata.get('title', 'Untitled')}
            Genre: {metadata.get('genre', 'Unknown')}
            Type: {content_type}"""

            try:
                summary = await self.gemini_ragas_llm.agenerate_text(summary_prompt)
                if summary:
                    enriched_metadata["ai_summary"] = summary
                    ragas_data.append({
                        "question": "Summarize this media content",
                        "answer": summary,
                        "contexts": [
                            f"Title: {metadata.get('title', 'Untitled')}",
                            f"Type: {content_type}",
                            f"File: {metadata.get('source_file', 'Unknown')}"
                        ],
                        "ground_truths": ["Accurate summary describing the content"]
                    })
            except Exception as e:
                logger.error(f"Summary generation failed: {e}")

            # Only evaluate if we have data
            if not ragas_data:
                self.kpi_tracker["ragas_score_avg"] = "N/A"
                return enriched_metadata

            # Ragas Evaluation
            try:
                result = evaluate(
                    Dataset.from_list(ragas_data),
                    metrics=[answer_relevancy],
                    llm=self.gemini_ragas_llm,
                    embeddings=self.gemini_ragas_embeddings
                )

                if hasattr(result, 'to_pandas'):
                    df = result.to_pandas()
                    if "answer_relevancy" in df.columns:
                        avg_score = df["answer_relevancy"].mean()
                        if not pd.isna(avg_score):
                            self.kpi_tracker["ragas_score_avg"] = round(avg_score, 3)
                            enriched_metadata["ragas_evaluation"] = df.to_dict()
            except Exception as e:
                logger.error(f"Ragas evaluation failed: {e}")
                self.kpi_tracker["ragas_score_avg"] = "N/A"

            return enriched_metadata

    def _get_asset_type_from_path(self, file_path, parent_span=None):
        """Determine asset type from path"""
        with self.otel_tracer.start_as_current_span("_get_asset_type_from_path") as span:
            span.set_attribute("file.path", file_path)
            asset_type = "episode"
            if "ad" in os.path.basename(file_path).lower():
                asset_type = "ad"
            elif "trailer" in os.path.basename(file_path).lower():
                asset_type = "trailer"
            span.set_attribute("asset.type.determined", asset_type)
            return asset_type

    def asset_acquisition(self, source_path, parent_span=None):
        """Monitor content sources for new files"""
        with self.otel_tracer.start_as_current_span("asset_acquisition") as span:
            span.set_attribute("source.path", source_path)
            new_assets = []
            try:
                for root, _, files in os.walk(source_path):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        if file_path in self.processed_files:
                            continue

                        if not file_name.endswith(('.json', '.xml', '.txt')):
                            checksum = self._calculate_checksum(file_path, parent_span=span)
                            if checksum and checksum in self.processed_checksums:
                                self.kpi_tracker["duplicate_asset_count"] += 1
                                continue

                        new_assets.append(file_path)
                        self.processed_files.add(file_path)
            except Exception as e:
                logger.error(f"Error during asset acquisition: {e}")
                span.set_status(trace.StatusCode.ERROR, description=f"Error during asset acquisition: {e}")
            return new_assets

    def format_detection_and_validation(self, asset_path, metadata, parent_span=None):
        """Perform technical checks on asset format"""
        with self.otel_tracer.start_as_current_span("format_detection_and_validation") as span:
            ffprobe_data = self._simulate_ffprobe(asset_path)
            format_valid = True
            issues = []
            transcode_needed = False
            initial_transcode_required = False

            if not ffprobe_data:
                return False, ["FFprobe simulation failed"], False, {}, False

            file_extension = ffprobe_data["format"]["format_name"]
            if file_extension not in self.config["policies"]["allowed_formats"]:
                format_valid = False
                issues.append(f"Unsupported format: {file_extension}")
                transcode_needed = True
                initial_transcode_required = True

            video_stream = next((s for s in ffprobe_data["streams"] if s["codec_type"] == "video"), None)
            if video_stream:
                if video_stream["codec_name"] not in ["h264", "hevc"]:
                    format_valid = False
                    issues.append(f"Unsupported video codec: {video_stream['codec_name']}")
                    transcode_needed = True
                    initial_transcode_required = True
                if video_stream["width"] < 1280 or video_stream["height"] < 720:
                    format_valid = False
                    issues.append(f"Low resolution: {video_stream['width']}x{video_stream['height']}")
                    transcode_needed = True
                    initial_transcode_required = True
            else:
                format_valid = False
                issues.append("No video stream detected")
                transcode_needed = True
                initial_transcode_required = True

            audio_stream = next((s for s in ffprobe_data["streams"] if s.get("codec_type") == "audio"), None)
            if audio_stream:
                if audio_stream.get("codec_name") not in ["aac", "ac3"]:
                    format_valid = False
                    issues.append(f"Unsupported audio codec: {audio_stream.get('codec_name')}")
                    transcode_needed = True
                    initial_transcode_required = True
            else:
                format_valid = False
                issues.append("No audio stream detected")
                transcode_needed = True
                initial_transcode_required = True

            if metadata.get("operator_override_transcode_needed") is True and initial_transcode_required:
                transcode_needed = False
                issues.append("Transcode requirement overridden by operator")

            return format_valid, issues, transcode_needed, ffprobe_data, initial_transcode_required

    async def metadata_extraction_and_enrichment(self, asset_path, ffprobe_data, parent_span=None):
        """Extract and enrich metadata"""
        with self.otel_tracer.start_as_current_span("metadata_extraction_and_enrichment") as span:
            metadata = {
                "source_file": os.path.basename(asset_path),
                "ingest_timestamp": datetime.now().isoformat(),
                "checksum_sha256": self._calculate_checksum(asset_path),
                "file_size_bytes": ffprobe_data["format"].get("size"),
                "duration": ffprobe_data["format"].get("duration")
            }

            base_name = os.path.splitext(asset_path)[0]
            sidecar_path = f"{base_name}.json"
            if os.path.exists(sidecar_path):
                try:
                    with open(sidecar_path, 'r') as f:
                        sidecar_metadata = json.load(f)
                    metadata.update(sidecar_metadata)
                except Exception as e:
                    logger.warning(f"Error reading sidecar {sidecar_path}: {e}")

            if "title" not in metadata:
                metadata["title"] = os.path.splitext(os.path.basename(asset_path))[0].replace('_', ' ').title()

            metadata["asset_type"] = self._get_asset_type_from_path(asset_path)
            enriched_metadata = await self._real_ai_enrichment(metadata, metadata["asset_type"])
            return enriched_metadata if enriched_metadata is not None else metadata

    def content_normalization(self, metadata, parent_span=None):
        """Standardize naming and perform normalization"""
        with self.otel_tracer.start_as_current_span("content_normalization") as span:
            original_file_name = metadata.get('source_file')
            asset_path = os.path.join(self.config["watch_folders"][0], original_file_name)
            file_extension = os.path.splitext(original_file_name)[1]

            asset_id = f"asset_{hashlib.md5(asset_path.encode()).hexdigest()[:8]}"
            normalized_name = f"{asset_id}_{metadata.get('title', 'untitled').replace(' ', '_').lower()}{file_extension}"
            normalized_path = os.path.join(self.config["output_folder"], normalized_name)

            try:
                if metadata.get("checksum_sha256") not in self.processed_checksums:
                    os.replace(asset_path, normalized_path)
                    self.processed_checksums.add(metadata["checksum_sha256"])
            except Exception as e:
                logger.error(f"Error during content normalization: {e}")
                return None, metadata

            metadata["normalized_file_path"] = normalized_path
            metadata["normalized_file_name"] = normalized_name
            return normalized_path, metadata

    def policy_compliance_check(self, metadata, parent_span=None):
        """Check asset against policy rules"""
        with self.otel_tracer.start_as_current_span("policy_compliance_check") as span:
            compliance_status = True
            compliance_flags = []
            escalate_to_human = False

            for field in self.config["policies"]["required_metadata_fields"]:
                if field not in metadata or not metadata[field]:
                    compliance_flags.append(f"Missing required metadata field: {field}.")
                    compliance_status = False
                    escalate_to_human = True

            duration = metadata.get("duration")
            if duration is not None:
                if metadata["asset_type"] == "ad" and duration > self.config["policies"]["ad_duration_limit_seconds"]:
                    compliance_flags.append(f"Ad duration exceeds limit")
                    compliance_status = False
                    escalate_to_human = True
                elif duration < self.config["policies"]["min_duration_seconds"]:
                    compliance_flags.append(f"Duration below minimum")
                    compliance_status = False
                    escalate_to_human = True
                elif duration > self.config["policies"]["max_duration_seconds"]:
                    compliance_flags.append(f"Duration exceeds maximum")
                    compliance_status = False
                    escalate_to_human = True
            else:
                compliance_flags.append("Duration metadata missing")
                compliance_status = False
                escalate_to_human = True

            file_size_mb = metadata.get("file_size_bytes", 0) / (1024 * 1024)
            if file_size_mb > self.config["policies"]["max_file_size_mb"]:
                compliance_flags.append(f"File size exceeds maximum")
                compliance_status = False
                escalate_to_human = True

            metadata["compliance_status"] = "PASS" if compliance_status else "FAIL"
            metadata["compliance_flags"] = compliance_flags
            metadata["escalate_to_human"] = escalate_to_human
            return compliance_status, compliance_flags, escalate_to_human, metadata

    def event_driven_hand_off(self, processed_asset_info, parent_span=None):
        """Trigger hand-off to downstream agents"""
        with self.otel_tracer.start_as_current_span("event_driven_hand_off") as span:
            logger.info(f"Handing off asset: {processed_asset_info.get('normalized_file_name', 'N/A')}")
            if processed_asset_info.get("compliance_status") == "FAIL":
                logger.warning("Asset handed off with compliance failures")
            return True

    async def ingest_asset_pipeline(self, asset_path, parent_span=None):
        """Orchestrate the entire ingestion pipeline"""
        token = None
        if parent_span:
            token = attach(trace.set_span_in_context(parent_span))

        processed_asset_info = {
            "original_path": asset_path,
            "ingest_start_time": datetime.now().isoformat(),
            "status": "IN_PROGRESS"
        }

        span = None
        try:
            with self.otel_tracer.start_as_current_span("ingest_asset_pipeline") as span_context:
                span = span_context
                self.kpi_tracker["total_ingests"] += 1

                # 1. Format Detection & Validation
                format_valid, format_issues, transcode_needed, ffprobe_data, initial_transcode_required = \
                    self.format_detection_and_validation(asset_path, {})
                processed_asset_info.update({
                    "format_valid": format_valid,
                    "format_issues": format_issues,
                    "transcode_needed": transcode_needed,
                    "initial_transcode_required": initial_transcode_required,
                    "ffprobe_data": ffprobe_data
                })

                if not format_valid:
                    processed_asset_info["status"] = "FAILED_FORMAT_VALIDATION"
                    self.event_driven_hand_off(processed_asset_info)
                    return processed_asset_info

                # 2. Metadata Extraction & Enrichment
                metadata = await self.metadata_extraction_and_enrichment(asset_path, ffprobe_data)
                processed_asset_info["metadata"] = metadata

                # 3. Content Normalization
                normalized_path, updated_metadata = self.content_normalization(metadata)
                if not normalized_path:
                    processed_asset_info["status"] = "FAILED_NORMALIZATION"
                    self.event_driven_hand_off(processed_asset_info)
                    return processed_asset_info

                processed_asset_info.update({
                    "normalized_file_path": normalized_path,
                    "metadata": updated_metadata
                })

                # 4. Policy Compliance Check
                compliance_status, compliance_flags, escalate_to_human, final_metadata = \
                    self.policy_compliance_check(updated_metadata)
                processed_asset_info["metadata"] = final_metadata

                if compliance_status:
                    processed_asset_info["status"] = "COMPLETED"
                    self.kpi_tracker["successful_ingests"] += 1
                else:
                    processed_asset_info["status"] = "FAILED_COMPLIANCE"
                    processed_asset_info["escalate_to_human"] = True

                # 5. Event-Driven Hand-off
                self.event_driven_hand_off(processed_asset_info)
                processed_asset_info["ingest_end_time"] = datetime.now().isoformat()

                return processed_asset_info

        except Exception as e:
            logger.critical(f"Unhandled error in ingestion pipeline: {e}", exc_info=True)
            processed_asset_info.update({
                "status": "CRITICAL_FAILURE",
                "error_message": str(e),
                "ingest_end_time": datetime.now().isoformat()
            })
            self.event_driven_hand_off(processed_asset_info)
            return processed_asset_info
        finally:
            if token:
                detach(token)

    def _report_kpis(self):
        """Report key performance indicators"""
        logger.info("\n--- Ingest Agent KPI Report ---")
        logger.info(f"Total Ingests: {self.kpi_tracker['total_ingests']}")
        logger.info(f"Successful Ingests: {self.kpi_tracker['successful_ingests']}")
        logger.info(f"Duplicate Assets Skipped: {self.kpi_tracker['duplicate_asset_count']}")
        logger.info(f"Total Gemini API Calls: {self.kpi_tracker['total_gemini_calls']}")
        logger.info(f"Successful Gemini API Calls: {self.kpi_tracker['successful_gemini_calls']}")
        logger.info(f"Metadata Fields Expected: {self.kpi_tracker['total_metadata_fields_expected']}")
        logger.info(f"Metadata Fields Present: {self.kpi_tracker['total_metadata_fields_present']}")

        if self.kpi_tracker["total_metadata_fields_expected"] > 0:
            completeness = (self.kpi_tracker["total_metadata_fields_present"] /
                            self.kpi_tracker["total_metadata_fields_expected"]) * 100
            logger.info(f"Metadata Completeness: {completeness:.2f}%")

        logger.info(f"Ragas Evaluation Score: {self.kpi_tracker['ragas_score_avg']}")
        logger.info("--- KPI Report End ---")

    def run(self, interval_seconds=5):
        """Main monitoring loop"""
        logger.info(f"Starting to monitor watch folders every {interval_seconds} seconds...")
        while True:
            for folder in self.config["watch_folders"]:
                new_assets = self.asset_acquisition(folder)
                for asset_path in new_assets:
                    asyncio.run(self.ingest_asset_pipeline(asset_path))
            time.sleep(interval_seconds)


if __name__ == "__main__":
    # Clean up directories for demo
    for folder in ["./ingest_source", "./ingest_processed"]:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                os.remove(os.path.join(folder, f))
        else:
            os.makedirs(folder)

    if os.path.exists("ingest_agent.log"):
        os.remove("ingest_agent.log")

    # Create test files
    test_files = [
        ("test_video.mp4", b"Dummy video content"),
        ("test_video.json", json.dumps({"title": "Test Video", "genre": "Demo", "language": "English"}))
    ]

    for filename, content in test_files:
        with open(os.path.join("./ingest_source", filename), "wb" if isinstance(content, bytes) else "w") as f:
            f.write(content)

    # Initialize and run agent
    agent = IngestAgent(CONFIG)

    # Process test files
    assets = agent.asset_acquisition("./ingest_source")
    for asset in assets:
        asyncio.run(agent.ingest_asset_pipeline(asset))

    # Report results
    agent._report_kpis()
    logger.info("Ingest Agent completed processing test files")