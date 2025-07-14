import os
import json

# Define the base directory for ingest source files
INGEST_SOURCE_DIR = "../ingest_source"

# Ensure the ingest_source directory exists
os.makedirs(INGEST_SOURCE_DIR, exist_ok=True)

print(f"Creating dummy files in: {os.path.abspath(INGEST_SOURCE_DIR)}\n")

# --- 1. Valid Media File with Sidecar Metadata ---
# Purpose: To test successful ingestion, validation, metadata extraction, normalization, and hand-off.
file_name_1 = "my_episode_s01e01.mp4"
sidecar_name_1 = "my_episode_s01e01.json"
file_path_1 = os.path.join(INGEST_SOURCE_DIR, file_name_1)
sidecar_path_1 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_1)

with open(file_path_1, "w") as f:
    f.write("This is a dummy video file content for a valid episode.")
print(f"Created dummy video: {file_name_1}")

metadata_1 = {
    "title": "My First Episode",
    "genre": "Sci-Fi",
    "language": "English",
    "director": "Jane Doe",
    "episode_number": 1,
    "season_number": 1,
    "ei_rating": "E" # Example: E/I for kids content, if your policy checks this
}
with open(sidecar_path_1, "w") as f:
    json.dump(metadata_1, f, indent=2)
print(f"Created sidecar JSON: {sidecar_name_1}\n")


# --- 2. Media File with Unsupported Format ---
# Purpose: To test format detection and validation for non-compliant formats.
file_name_2 = "bad_format_video.avi" # Assuming .avi is not in allowed_formats
file_path_2 = os.path.join(INGEST_SOURCE_DIR, file_name_2)

with open(file_path_2, "w") as f:
    f.write("This is a dummy AVI file, which is likely not allowed by policy.")
print(f"Created dummy unsupported format file: {file_name_2}\n")


# --- 3. Media File Violating Policy Rules (e.g., Simulated Duration Limit) ---
# Purpose: To test policy compliance checks. The agent simulates duration based on file size.
# We'll make this file significantly larger to simulate exceeding duration limits.
file_name_3 = "long_ad_campaign_x.mp4"
file_path_3 = os.path.join(INGEST_SOURCE_DIR, file_name_3)

# Simulate a large file size (e.g., 10MB) to trigger a "long duration" flag
# The agent's _simulate_ffprobe uses: duration = file_size_mb * 60 / 100
# So, 10MB * 60 / 100 = 6 seconds. If ad_duration_limit_seconds is 60, this won't fail.
# Let's make it much larger to ensure it fails the ad duration limit (e.g., 100MB for a 60-second ad limit)
# 100MB * 60 / 100 = 60 seconds. If ad_duration_limit_seconds is 60, this will be exactly the limit.
# To exceed, let's make it 101MB.
simulated_size_mb = 101
with open(file_path_3, "wb") as f: # Use 'wb' for writing bytes
    f.write(b"A" * (1024 * 1024 * simulated_size_mb)) # Write 101 MB of 'A's
print(f"Created dummy large file (simulated long ad): {file_name_3}")

sidecar_name_3 = "long_ad_campaign_x.json"
sidecar_path_3 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_3)
metadata_3 = {
    "title": "Long Ad Campaign X",
    "genre": "Commercial",
    "language": "English",
    "asset_type": "ad" # Explicitly set asset type to trigger ad-specific policies
}
with open(sidecar_path_3, "w") as f:
    json.dump(metadata_3, f, indent=2)
print(f"Created sidecar JSON for long ad: {sidecar_name_3}\n")


# --- 4. Media File with Missing or Incomplete Metadata ---
# Purpose: To test identification of assets lacking required metadata fields.
file_name_4 = "ad_missing_meta.mp4"
file_path_4 = os.path.join(INGEST_SOURCE_DIR, file_name_4)

with open(file_path_4, "w") as f:
    f.write("This is a dummy ad file with intentionally missing metadata.")
print(f"Created dummy file with missing metadata: {file_name_4}\n")

# No sidecar JSON for this one, or an incomplete one, to simulate missing required fields.
# If you want to simulate an incomplete one:
# sidecar_name_4 = "ad_missing_meta.json"
# sidecar_path_4 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_4)
# metadata_4 = {
#     "title": "Ad with Missing Info",
#     # Missing genre and language to trigger policy failure
# }
# with open(sidecar_path_4, "w") as f:
#     json.dump(metadata_4, f, indent=2)
# print(f"Created incomplete sidecar JSON for missing meta ad: {sidecar_name_4}\n")

# --- 5. Media File Simulating Blank Frames ---
# Purpose: To represent a video file that would have blank frames, which a QC agent
#          (or a more advanced AI detection in the Ingest Agent) would flag.
file_name_5 = "video_with_blank_frames.mp4"
file_path_5 = os.path.join(INGEST_SOURCE_DIR, file_name_5)

with open(file_path_5, "w") as f:
    f.write("This is a dummy video file content. Imagine it has blank frames within.")
print(f"Created dummy file simulating blank frames: {file_name_5}\n")

# --- 6. Media File with Minimal/Ambiguous Content (No Sidecar) ---
# Purpose: To test how the LLM handles lack of explicit input during metadata enrichment.
file_name_6 = "mystery_video.mp4"
file_path_6 = os.path.join(INGEST_SOURCE_DIR, file_name_6)

with open(file_path_6, "w") as f:
    f.write("This is a dummy video file with minimal content, no sidecar.")
print(f"Created dummy file with minimal/ambiguous content: {file_name_6}\n")


print("All dummy files created. You can now run your Ingest Agent script.")
print("Remember to check the 'ingest_agent.log' file and 'ingest_processed' folder for results.")
