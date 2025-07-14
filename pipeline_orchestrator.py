import os
import json
import time
import logging
from datetime import datetime
import sys

# Import the agents
from ingest_agent import IngestAgent, CONFIG as INGEST_CONFIG
from qc_agent import QCAgent
from graphics_agent import GraphicsAgent
from ad_agent import AdAgent  # Import AdAgent
from scheduling_agent import SchedulingAgent  # Import SchedulingAgent

# --- Trulens Imports ---
from trulens.core import TruSession
from trulens.app import TruApp  # Changed import path to the recommended trulens.app
from trulens.apps.custom import instrument  # Re-added instrument for clarity, though used in ingest_agent.py

# --- Setup Logging for Orchestrator ---
logging.basicConfig(
    level=logging.DEBUG,  # Set to INFO for less verbose output during normal runs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PipelineOrchestrator")

# Clear orchestrator log file on each run
LOG_FILE = "pipeline_orchestrator.log"
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)


def clean_directory(directory_path):
    """Removes all files from a given directory."""
    if os.path.exists(directory_path):
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.error(f'Failed to delete {file_path}. Reason: {e}')
        logger.info(f"Cleaned directory: {directory_path}")
    else:
        os.makedirs(directory_path, exist_ok=True)
        logger.info(f"Created directory: {directory_path}")


def setup_initial_environment():
    """Ensures all necessary directories are clean and ready."""
    clean_directory("./ingest_source")
    clean_directory("./ingest_processed")
    clean_directory("./graphics_output")
    # Clean agent-specific logs as well for a fresh start
    # This is the ONLY place where agent logs should be removed to avoid conflicts
    if os.path.exists("ingest_agent.log"):
        os.remove("ingest_agent.log")
        logger.info("Cleaned ingest_agent.log")
    if os.path.exists("qc_agent.log"):
        os.remove("qc_agent.log")
        logger.info("Cleaned qc_agent.log")
    if os.path.exists("graphics_agent.log"):
        os.remove("graphics_agent.log")
        logger.info("Cleaned graphics_agent.log")
    if os.path.exists("ad_agent.log"):
        os.remove("ad_agent.log")
        logger.info("Cleaned ad_agent.log")
    if os.path.exists("scheduling_agent.log"):  # Added for scheduling agent
        os.remove("scheduling_agent.log")
        logger.info("Cleaned scheduling_agent.log")
    # --- NEW: Also remove the Trulens database file for a clean start ---
    if os.path.exists("default.sqlite"):
        os.remove("default.sqlite")
        logger.info("Cleaned Trulens database: default.sqlite")


def simulate_successful_pipeline_scenario(ingest_agent_tru, qc_agent, graphics_agent, ad_agent, scheduling_agent, tru):
    """
    Simulates a full end-to-end pipeline run for a successful asset.
    """
    logger.info("\n--- Running SUCCESSFUL Pipeline Scenario ---")
    print("DEBUG: Inside simulate_successful_pipeline_scenario.")
    sys.stdout.flush()

    # Create a dummy file for ingest
    file_name = "test_episode_successful.mp4"
    sidecar_name = "test_episode_successful.json"
    file_path = os.path.join(INGEST_CONFIG["watch_folders"][0], file_name)
    sidecar_path = os.path.join(INGEST_CONFIG["watch_folders"][0], sidecar_name)

    # Ensure this dummy file is large enough to pass duration policy
    simulated_size_mb = 20  # Corresponds to 12 seconds duration (20 * 60 / 100)
    with open(file_path, "wb") as f:
        f.write(b"This is a dummy video file content for a successful test." * (1024 * 1024 * simulated_size_mb // 50))
    metadata = {
        "title": "The Grand Adventure",
        "genre": "Fantasy",
        "language": "English",
        "ei_rating": "E"
    }
    with open(sidecar_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Orchestrator: Created test asset '{file_name}' and its sidecar metadata.")
    time.sleep(0.5)  # Give file system a moment

    logger.info("\nOrchestrator: Triggering Ingest Agent to process the asset.")
    print("DEBUG: Calling ingest_agent_tru.ingest_asset_pipeline for successful scenario.")
    sys.stdout.flush()
    # Ingest Agent processes the asset
    with tru.start_recording(ingest_agent_tru) as record_context:  # Re-enabled TruLens recording
        ingested_asset_info = ingest_agent_tru.ingest_asset_pipeline(file_path)

    print(
        f"DEBUG: ingest_agent_tru.ingest_asset_pipeline returned: {ingested_asset_info.get('status', 'N/A') if isinstance(ingested_asset_info, dict) else str(ingested_asset_info)}")
    sys.stdout.flush()

    record_context.update_record(ingested_asset_info)  # Re-enabled record update

    if ingested_asset_info and isinstance(ingested_asset_info, dict) and ingested_asset_info.get(
            "status") == "COMPLETED":
        logger.info(
            f"Orchestrator: Ingest Agent successfully processed '{file_name}'. Status: {ingested_asset_info['status']}.")

        # QC Agent performs checks
        logger.info("\nOrchestrator: Triggering QC Agent to perform quality checks.")
        qc_processed_asset_info = qc_agent.perform_qc_checks(ingested_asset_info)
        logger.info(f"Orchestrator: QC Agent completed checks. Status: {qc_processed_asset_info['qc_status']}.")

        # Graphics Agent receives metadata
        logger.info("\nOrchestrator: Triggering Graphics Agent to generate graphics.")
        graphics_message_payload = qc_agent.send_to_graphics_agent(qc_processed_asset_info.get("metadata", {}))
        graphics_agent.receive_metadata(graphics_message_payload)
        logger.info("Orchestrator: Graphics Agent processed metadata.")

        # Ad Agent schedules ads (example content schedule)
        logger.info("\nOrchestrator: Triggering Ad Agent to schedule ads.")
        sample_content_schedule = [
            {"content_id": ingested_asset_info["metadata"]["title"], "region": "US", "ad_slot_duration": 30,
             "target_audience": "general"}
        ]
        scheduled_ads = ad_agent.schedule_ads(sample_content_schedule)
        logger.info(f"Orchestrator: Ad Agent scheduled ads: {scheduled_ads}")

        # Scheduling Agent receives processed asset info
        logger.info("\nOrchestrator: Triggering Scheduling Agent to schedule content.")
        scheduling_agent.receive_processed_asset_info(ingested_asset_info)
        logger.info("Orchestrator: Scheduling Agent processed asset info.")

        logger.info("\nSuccessful Pipeline Scenario Result: SUCCESS")
        print("DEBUG: simulate_successful_pipeline_scenario returning True.")
        sys.stdout.flush()
        return True
    else:
        logger.error(
            f"Orchestrator: Ingest Agent failed or did not complete for {file_name}. Status: {ingested_asset_info.get('status', 'N/A') if isinstance(ingested_asset_info, dict) else str(ingested_asset_info)}. Pipeline aborted for this asset.")
        logger.info("\nSuccessful Pipeline Scenario Result: FAILURE")
        print("DEBUG: simulate_successful_pipeline_scenario returning False.")
        sys.stdout.flush()
        return False


def simulate_error_handling_scenario(ingest_agent_tru, tru):
    """
    Simulates a scenario where an asset fails early in the pipeline (e.g., corrupted file).
    """
    logger.info("\n--- Running ERROR HANDLING Pipeline Scenario ---")
    print("DEBUG: Inside simulate_error_handling_scenario.")
    sys.stdout.flush()

    # Create a dummy corrupted file
    file_name = "corrupted_header_test.mp4"
    file_path = os.path.join(INGEST_CONFIG["watch_folders"][0], file_name)
    with open(file_path, "wb") as f:
        f.write(os.urandom(50))  # Random bytes to simulate corruption
    logger.info(f"Orchestrator: Created simulated corrupted asset '{file_name}'.")
    time.sleep(0.5)

    logger.info("\nOrchestrator: Triggering Ingest Agent to process the corrupted asset (expected to fail).")
    print("DEBUG: Calling ingest_agent_tru.ingest_asset_pipeline for error handling scenario.")
    sys.stdout.flush()
    # Removed TruLens recording for ingest_asset_pipeline
    with tru.start_recording(ingest_agent_tru) as record_context:  # Re-enabled TruLens recording
        ingested_asset_info = ingest_agent_tru.ingest_asset_pipeline(file_path)

    print(
        f"DEBUG: ingest_agent_tru.ingest_asset_pipeline returned: {ingested_asset_info.get('status', 'N/A') if isinstance(ingested_asset_info, dict) else str(ingested_asset_info)}")
    sys.stdout.flush()

    record_context.update_record(ingested_asset_info)  # Re-enabled record update

    if ingested_asset_info and isinstance(ingested_asset_info, dict) and ingested_asset_info.get(
            "status") == "FAILED_FORMAT_VALIDATION":
        logger.info(
            f"Orchestrator: Ingest Agent correctly reported failure for '{file_name}'. Status: {ingested_asset_info['status']}. This is expected behavior.")
        logger.info("Orchestrator: **NOT handing off to QC, Graphics, or Scheduling Agents for this failed asset.**")
        logger.info(f"--- Error Handling Scenario for: {file_name} Completed (Expected Failure Handled) ---")
        logger.info("\nError Handling Pipeline Scenario Result: SUCCESS (Error Handled)")
        print("DEBUG: simulate_error_handling_scenario returning True.")
        sys.stdout.flush()
        return True
    else:
        logger.error(
            f"Orchestrator: Ingest Agent did NOT correctly handle failure for '{file_name}'. Status: {ingested_asset_info.get('status', 'N/A') if isinstance(ingested_asset_info, dict) else str(ingested_asset_info)}.")
        logger.info(f"--- Error Handling Scenario for: {file_name} Completed (UNEXPECTED Failure) ---")
        logger.info("\nError Handling Pipeline Scenario Result: FAILURE (Error Not Handled)")
        print("DEBUG: simulate_error_handling_scenario returning False.")
        sys.stdout.flush()
        return False


def simulate_operator_override_scenario(ingest_agent_tru, qc_agent, graphics_agent, ad_agent, scheduling_agent, tru):
    """
    Simulates a scenario where an operator override allows a non-compliant asset to proceed.
    """
    logger.info("\n--- Running OPERATOR OVERRIDE Pipeline Scenario ---")
    print("DEBUG: Inside simulate_operator_override_scenario.")
    sys.stdout.flush()

    # Create a dummy file with an unsupported format but an override flag
    file_name = "bad_format_video_override.avi"
    sidecar_name = "bad_format_video_override.json"
    file_path = os.path.join(INGEST_CONFIG["watch_folders"][0], file_name)
    sidecar_path = os.path.join(INGEST_CONFIG["watch_folders"][0], sidecar_name)

    with open(file_path, "w") as f:
        f.write("This is a dummy AVI file, which is not allowed by default policy.")
    metadata = {
        "title": "Override Test Video",
        "genre": "Test",
        "language": "English",
        "operator_override_transcode_needed": True  # Operator override flag
    }
    with open(sidecar_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Orchestrator: Created test asset '{file_name}' with operator override metadata.")
    time.sleep(0.5)

    logger.info("\nOrchestrator: Triggering Ingest Agent to process the override asset.")
    print("DEBUG: Calling ingest_agent_tru.ingest_asset_pipeline for operator override scenario.")
    sys.stdout.flush()
    # Removed TruLens recording for ingest_asset_pipeline
    with tru.start_recording(ingest_agent_tru) as record_context:  # Re-enabled TruLens recording
        ingested_asset_info = ingest_agent_tru.ingest_asset_pipeline(file_path)

    print(
        f"DEBUG: ingest_agent_tru.ingest_asset_pipeline returned: {ingested_asset_info.get('status', 'N/A') if isinstance(ingested_asset_info, dict) else str(ingested_asset_info)}")
    sys.stdout.flush()

    record_context.update_record(ingested_asset_info)  # Re-enabled record update

    # The status for an overridden asset should now be "COMPLETED_WITH_OVERRIDE"
    if ingested_asset_info and isinstance(ingested_asset_info, dict) and ingested_asset_info.get("status") in [
        "COMPLETED", "COMPLETED_WITH_OVERRIDE", "COMPLETED_WITH_OVERRIDE_AND_COMPLIANCE_ISSUES"]:
        logger.info(
            f"Orchestrator: Ingest Agent successfully processed '{file_name}' with override. Status: {ingested_asset_info['status']}.")

        # QC Agent performs checks
        logger.info("\nOrchestrator: Triggering QC Agent to perform quality checks.")
        qc_processed_asset_info = qc_agent.perform_qc_checks(ingested_asset_info)
        logger.info(f"Orchestrator: QC Agent completed checks. Status: {qc_processed_asset_info['qc_status']}.")

        # Graphics Agent receives metadata
        logger.info("\nOrchestrator: Triggering Graphics Agent to generate graphics.")
        graphics_message_payload = qc_agent.send_to_graphics_agent(qc_processed_asset_info.get("metadata", {}))
        graphics_agent.receive_metadata(graphics_message_payload)
        logger.info("Orchestrator: Graphics Agent processed metadata.")

        # Ad Agent schedules ads (example content schedule)
        logger.info("\nOrchestrator: Triggering Ad Agent to schedule ads.")
        sample_content_schedule = [
            {"content_id": ingested_asset_info["metadata"]["title"], "region": "US", "ad_slot_duration": 30,
             "target_audience": "general"}
        ]
        scheduled_ads = ad_agent.schedule_ads(sample_content_schedule)
        logger.info(f"Orchestrator: Ad Agent scheduled ads: {scheduled_ads}")

        # Scheduling Agent receives processed asset info
        logger.info("\nOrchestrator: Triggering Scheduling Agent to schedule content.")
        scheduling_agent.receive_processed_asset_info(ingested_asset_info)
        logger.info("Orchestrator: Scheduling Agent processed asset info.")

        logger.info("\nOperator Override Pipeline Scenario Result: SUCCESS")
        print("DEBUG: simulate_operator_override_scenario returning True.")
        sys.stdout.flush()
        return True
    else:
        logger.error(
            f"Orchestrator: Ingest Agent failed or did not complete for {file_name}. Status: {ingested_asset_info.get('status', 'N/A') if isinstance(ingested_asset_info, dict) else str(ingested_asset_info)}. Pipeline aborted for this asset.")
        logger.info("\nOperator Override Pipeline Scenario Result: FAILURE")
        print("DEBUG: simulate_operator_override_scenario returning False.")
        sys.stdout.flush()
        return False


# --- Main Orchestration Logic ---
if __name__ == "__main__":
    try:  # Added a try-except block to catch top-level exceptions
        logger.info("Pipeline Orchestrator initialized. Agents are ready.")

        # --- IMPORTANT: Clean environment BEFORE initializing agents to ensure Trulens starts fresh ---
        setup_initial_environment()

        # --- NEW: Initialize Tru here, after cleanup ---
        global tru  # Declare tru as global here
        tru = TruSession()
        logger.info("Initialized Trulens database after cleanup.")
        print("DEBUG: Tru instance initialized. Proceeding to agent initialization.")
        sys.stdout.flush()  # Flush stdout

        # Initialize agents
        print("DEBUG: Attempting to initialize IngestAgent...")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: Initializing IngestAgent...")
        ingest_agent = IngestAgent(INGEST_CONFIG)
        print("DEBUG: IngestAgent initialized successfully.")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: IngestAgent initialized.")

        # --- TEMPORARY: Test direct Gemini API call without TruLens instrumentation ---
        print("\nDEBUG: --- STARTING DIRECT GEMINI API TEST ---")
        sys.stdout.flush()
        test_prompt = "Say hello in one word."
        print(f"DEBUG: Calling ingest_agent._call_gemini_api with prompt: '{test_prompt}'")
        sys.stdout.flush()
        direct_gemini_response = ingest_agent._call_gemini_api(test_prompt)
        print(f"DEBUG: Direct Gemini API response: '{direct_gemini_response}'")
        sys.stdout.flush()
        print("DEBUG: --- ENDING DIRECT GEMINI API TEST ---\n")
        sys.stdout.flush()
        # --- END TEMPORARY TEST ---

        # --- Use the raw IngestAgent instance directly, bypassing TruLens wrapping for now ---
        # Re-enabled TruCustomApp wrapping for ingest_agent
        ingest_agent_tru = TruApp(ingest_agent, app_id="IngestAgentPipeline")  # Changed to TruApp
        logger.info("IngestAgent instance wrapped with TruApp for instrumentation.")

        print("DEBUG: Attempting to initialize QCAgent...")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: Initializing QCAgent...")
        qc_agent = QCAgent()
        print("DEBUG: QCAgent initialized successfully.")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: QCAgent initialized.")

        print("DEBUG: Attempting to initialize GraphicsAgent...")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: Initializing GraphicsAgent...")
        graphics_agent = GraphicsAgent()
        print("DEBUG: GraphicsAgent initialized successfully.")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: GraphicsAgent initialized.")

        print("DEBUG: Attempting to initialize AdAgent...")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: Initializing AdAgent...")
        # Now calling static methods directly on the class
        ad_blocks_data = AdAgent._get_ad_blocks()
        regional_revenue_data = AdAgent._simulate_regional_revenue_data()
        ad_agent = AdAgent(ad_blocks_data=ad_blocks_data, regional_revenue_data=regional_revenue_data)
        print("DEBUG: AdAgent initialized successfully.")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: AdAgent initialized.")

        print("DEBUG: Attempting to initialize SchedulingAgent...")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: Initializing SchedulingAgent...")
        scheduling_agent = SchedulingAgent(config={"scheduling_window_days": 7})  # Pass config
        print("DEBUG: SchedulingAgent initialized successfully.")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: SchedulingAgent initialized.")

        print("DEBUG: All agents initialized. Starting scenario simulations.")
        sys.stdout.flush()  # Flush stdout
        logger.debug("Orchestrator: Starting scenario simulations.")

        print("DEBUG: About to call simulate_successful_pipeline_scenario.")  # NEW DIAGNOSTIC PRINT
        sys.stdout.flush()

        # Run scenarios
        scenario_results = {}
        # Pass the raw agent instance to the simulation functions
        scenario_results["successful_pipeline"] = simulate_successful_pipeline_scenario(ingest_agent_tru, qc_agent,
                                                                                        graphics_agent, ad_agent,
                                                                                        scheduling_agent, tru)
        print(f"DEBUG: simulate_successful_pipeline_scenario returned: {scenario_results['successful_pipeline']}")
        sys.stdout.flush()

        print("DEBUG: Calling simulate_error_handling_scenario...")
        sys.stdout.flush()
        scenario_results["error_handling_pipeline"] = simulate_error_handling_scenario(ingest_agent_tru, tru)
        print(f"DEBUG: simulate_error_handling_scenario returned: {scenario_results['error_handling_pipeline']}")
        sys.stdout.flush()

        print("DEBUG: Calling simulate_operator_override_scenario...")
        sys.stdout.flush()
        scenario_results["operator_override_pipeline"] = simulate_operator_override_scenario(ingest_agent_tru, qc_agent,
                                                                                             graphics_agent, ad_agent,
                                                                                             scheduling_agent, tru)
        print(f"DEBUG: simulate_operator_override_scenario returned: {scenario_results['operator_override_pipeline']}")
        sys.stdout.flush()

        print("\n--- All Pipeline Scenarios Completed ---")
        sys.stdout.flush()  # Flush stdout
        for scenario, result in scenario_results.items():
            logger.info(f"{scenario}: {'SUCCESS' if result else 'FAILURE'}")

        # --- Continuous Monitoring for Trulens Logging ---
        # This section will run the IngestAgent's monitoring loop for a short period
        # to ensure Trulens has time to log multiple LLM calls.
        logger.info("\n--- Starting continuous monitoring for Trulens logging (approx. 30 seconds) ---")
        monitor_duration = 30  # seconds
        start_time = time.time()

        # Create some dummy files to be processed during monitoring
        clean_directory("./ingest_source")  # This will clear the ingest_source again
        for i in range(5):  # Create 5 files
            file_name = f"monitor_test_file_{i}.mp4"
            sidecar_name = f"monitor_test_file_{i}.json"
            file_path = os.path.join(INGEST_CONFIG["watch_folders"][0], file_name)
            sidecar_path = os.path.join(INGEST_CONFIG["watch_folders"][0], sidecar_name)
            with open(file_path, "w") as f:
                f.write(f"This is dummy content for monitor test file {i}.")
            metadata = {
                "title": f"Monitor Test Episode {i}",
                "genre": "Documentary",
                "language": "English",
                "ei_rating": "G"
            }
            with open(sidecar_path, "w") as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Created monitor test file: {file_name}")

        # Re-enabled TruLens recording for the continuous monitoring loop
        end_monitor_time = time.time() + monitor_duration
        while time.time() < end_monitor_time:
            for folder in ingest_agent.config["watch_folders"]:
                new_assets = ingest_agent_tru.asset_acquisition(folder)  # Use the TruLens-wrapped agent
                for asset_path in new_assets:
                    with tru.start_recording(ingest_agent_tru) as record_context:  # Re-enabled TruLens recording
                        ingested_asset_info = ingest_agent_tru.ingest_asset_pipeline(asset_path)
                    record_context.update_record(ingested_asset_info)  # Re-enabled record update
            time.sleep(INGEST_CONFIG["policies"]["min_duration_seconds"] / 2)  # Shorter sleep for more frequent scans

        logger.info(f"--- Continuous monitoring finished after {monitor_duration} seconds ---")
        sys.stdout.flush()  # Flush stdout

        # Give Trulens a moment to write any final buffered data
        logger.info("Giving Trulens a moment to finalize database writes...")
        sys.stdout.flush()  # Flush stdout
        time.sleep(5)  # Increased sleep for better chances of Trulens logging

        logger.info("\n--- Orchestration Finished ---")
        sys.stdout.flush()  # Flush stdout
        ingest_agent._report_kpis()  # Report KPIs at the end of the orchestration

        # Run the TruLens dashboard
        logger.info("\n--- Starting TruLens Dashboard ---")
        sys.stdout.flush()  # Flush stdout
        tru.run_dashboard()  # This will launch the UI in your browser
        logger.info("TruLens Dashboard started. Open your browser to view results.")
        sys.stdout.flush()  # Flush stdout

    except Exception as e:
        logger.critical(f"An unhandled error occurred during orchestration: {e}", exc_info=True)
        # Ensure logs are flushed even on critical errors
        logging.shutdown()
        sys.exit(1)  # Explicitly exit with an error code

