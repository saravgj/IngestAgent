import os
import json

# Define the base directory for ingest source files
INGEST_SOURCE_DIR = "./ingest_source"

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

# --- 7. Misleading Content File ---
# Purpose: To test how the LLM handles conflicting information between filename and sidecar,
#          and to observe if it hallucinates a reconciliation or correctly identifies ambiguity.
file_name_7 = "happy_adventure.mp4"
sidecar_name_7 = "happy_adventure.json"
file_path_7 = os.path.join(INGEST_SOURCE_DIR, file_name_7)
sidecar_path_7 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_7)

with open(file_path_7, "w") as f:
    f.write("This is a dummy video file content for a misleading test.")
print(f"Created dummy video: {file_name_7}")

metadata_7 = {
    "title": "A Tragic Drama", # Contradicts "happy_adventure" filename
    "genre": "Drama",
    "language": "English",
    "description": "A story of loss, despair, and unavoidable fate. Not a happy ending."
}
with open(sidecar_path_7, "w") as f:
    json.dump(metadata_7, f, indent=2)
print(f"Created sidecar JSON with misleading content: {sidecar_name_7}\n")

# --- 8. Nonsensical/Garbage Content File ---
# Purpose: To test if the LLM attempts to create plausible metadata from completely incoherent input.
file_name_8 = "random_gibberish.mp4"
sidecar_name_8 = "random_gibberish.json"
file_path_8 = os.path.join(INGEST_SOURCE_DIR, file_name_8)
sidecar_path_8 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_8)

with open(file_path_8, "w") as f:
    f.write("asdfjkl;qweruiopzxcvbnm,./1234567890!@#$%^&*()")
print(f"Created dummy video: {file_name_8}")

metadata_8 = {
    "title": "asdfjkl;qweruiopzxcvbnm", # Nonsensical title
    "genre": "Nonsense",
    "language": "Gibberish",
    "description": "This content is completely random and has no discernible meaning or structure. It's just a string of characters."
}
with open(sidecar_path_8, "w") as f:
    json.dump(metadata_8, f, indent=2)
print(f"Created sidecar JSON with nonsensical content: {sidecar_name_8}\n")


# --- 9. Highly Abstract/Conceptual Content File ---
# Purpose: To test if the LLM can stick to the abstract nature of the input without making concrete details.
file_name_9 = "concept_of_freedom.mp4"
sidecar_name_9 = "concept_of_freedom.json"
file_path_9 = os.path.join(INGEST_SOURCE_DIR, file_name_9)
sidecar_path_9 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_9)

with open(file_path_9, "w") as f:
    f.write("This is a dummy video file representing an abstract concept.")
print(f"Created dummy video: {file_name_9}")

metadata_9 = {
    "title": "The Concept of Freedom",
    "genre": "Philosophical Documentary",
    "language": "English",
    "description": "An exploration of the multifaceted and evolving understanding of liberty and autonomy across different cultures and historical periods."
}
with open(sidecar_path_9, "w") as f:
    json.dump(metadata_9, f, indent=2)
print(f"Created sidecar JSON with abstract content: {sidecar_name_9}\n")


# --- 10. Highly Repetitive Content File ---
# Purpose: To test if the LLM generates unique metadata or simply repeats patterns.
file_name_10 = "repeating_pattern_video.mp4"
sidecar_name_10 = "repeating_pattern_video.json"
file_path_10 = os.path.join(INGEST_SOURCE_DIR, file_name_10)
sidecar_path_10 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_10)

with open(file_path_10, "w") as f:
    f.write("Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat. Repeat.")
print(f"Created dummy video: {file_name_10}")

metadata_10 = {
    "title": "A Study in Repetition",
    "genre": "Experimental",
    "language": "English",
    "description": "This video consists solely of a repeating visual and auditory pattern designed to explore the effects of monotony."
}
with open(sidecar_path_10, "w") as f:
    json.dump(metadata_10, f, indent=2)
print(f"Created sidecar JSON with repetitive content: {sidecar_name_10}\n")

# --- 11. Highly Specific/Niche Content File ---
# Purpose: To test if the LLM invents details when faced with very specialized terminology it might not fully understand.
file_name_11 = "quantum_chromodynamics_lecture.mp4"
sidecar_name_11 = "quantum_chromodynamics_lecture.json"
file_path_11 = os.path.join(INGEST_SOURCE_DIR, file_name_11)
sidecar_path_11 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_11)

with open(file_path_11, "w") as f:
    f.write("This is a dummy video file for a highly specific physics lecture.")
print(f"Created dummy video: {file_name_11}")

metadata_11 = {
    "title": "Introduction to Quantum Chromodynamics",
    "genre": "Educational",
    "language": "English",
    "description": "A graduate-level lecture covering the strong nuclear force, quarks, gluons, and asymptotic freedom within the Standard Model of particle physics.",
    "lecturer": "Dr. Alice Smith",
    "course_code": "PHY701"
}
with open(sidecar_path_11, "w") as f:
    json.dump(metadata_11, f, indent=2)
print(f"Created sidecar JSON with niche content: {sidecar_name_11}\n")


# --- 12. Contradictory Numerical/Factual Data File ---
# Purpose: To specifically test for factual hallucination when presented with conflicting numerical or factual data.
file_name_12 = "historical_anomaly.mp4"
sidecar_name_12 = "historical_anomaly.json"
file_path_12 = os.path.join(INGEST_SOURCE_DIR, file_name_12)
sidecar_path_12 = os.path.join(INGEST_SOURCE_DIR, sidecar_name_12)

with open(file_path_12, "w") as f:
    f.write("This is a dummy video file about a historical anomaly.")
print(f"Created dummy video: {file_name_12}")

metadata_12 = {
    "title": "The Great Fire of London (1966)", # Incorrect year
    "genre": "Historical Documentary",
    "language": "English",
    "description": "A documentary detailing the devastating fire that swept through London in the year 1966, causing widespread destruction.",
    "historical_period": "20th Century" # Contradicts actual 17th century event
}
with open(sidecar_path_12, "w") as f:
    json.dump(metadata_12, f, indent=2)
print(f"Created sidecar JSON with contradictory factual data: {sidecar_name_12}\n")

# --- 13. Corrupted Header Simulation File ---
# Purpose: To simulate a file with a corrupted or unreadable header, causing FFprobe to fail.
# This file will be intentionally small and contain non-video data.
file_name_13 = "corrupted_header.mp4"
file_path_13 = os.path.join(INGEST_SOURCE_DIR, file_name_13)

# Write some random bytes that are unlikely to form a valid MP4 header
with open(file_path_13, "wb") as f:
    f.write(os.urandom(50)) # 50 random bytes

print(f"Created dummy file simulating corrupted header: {file_name_13}\n")

# --- 14. Duplicate File Uploads (Same Content, Different Names) ---
# Purpose: To test de-duplication based on content checksums.
# We'll create one "original" file and two "duplicates" with different names.
duplicate_content = b"This is the unique content for the duplicate test files."

file_name_14_orig = "original_content_video.mp4"
file_path_14_orig = os.path.join(INGEST_SOURCE_DIR, file_name_14_orig)
with open(file_path_14_orig, "wb") as f:
    f.write(duplicate_content)
print(f"Created original duplicate test file: {file_name_14_orig}")

file_name_14_dup1 = "duplicate_content_version_1.mp4"
file_path_14_dup1 = os.path.join(INGEST_SOURCE_DIR, file_name_14_dup1)
with open(file_path_14_dup1, "wb") as f:
    f.write(duplicate_content)
print(f"Created first duplicate test file: {file_name_14_dup1}")

file_name_14_dup2 = "another_copy_of_content.mp4"
file_path_14_dup2 = os.path.join(INGEST_SOURCE_DIR, file_name_14_dup2)
with open(file_path_14_dup2, "wb") as f:
    f.write(duplicate_content)
print(f"Created second duplicate test file: {file_name_14_dup2}\n")


# --- 15. Oversized File Handling ---
# Purpose: To test the policy that flags files exceeding a maximum size.
# The actual file size will be small, but _simulate_ffprobe will report it as 50GB.
file_name_15 = "oversized_file_test.mp4"
file_path_15 = os.path.join(INGEST_SOURCE_DIR, file_name_15)

# Write a small amount of content; the simulation logic will override its size.
with open(file_path_15, "wb") as f:
    f.write(b"This file is small, but will be simulated as 50GB.")
print(f"Created dummy oversized file (simulated): {file_name_15}\n")


print("All dummy files created. You can now run your Ingest Agent script.")
print("Remember to check the 'ingest_agent.log' file and 'ingest_processed' folder for results.")
