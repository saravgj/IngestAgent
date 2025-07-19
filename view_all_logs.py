import os

# Define the paths to your log files
LOG_FILES = [
    "pipeline_orchestrator.log",
    "ingest_agent.log",
    "qc_agent.log",
    "graphics_agent.log"
]

def view_logs(log_file_paths):
    """
    Reads and prints the content of multiple log files,
    with clear headers for each file.
    """
    print("\n" + "="*80)
    print("                 AGGREGATED PIPELINE LOGS                 ")
    print("="*80 + "\n")

    for log_file in log_file_paths:
        print(f"\n{'*'*10} START OF {log_file} {'*'*10}\n")
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                    if content.strip(): # Only print if there's actual content
                        print(content)
                    else:
                        print(f"--- {log_file} is empty ---")
            except Exception as e:
                print(f"Error reading {log_file}: {e}")
        else:
            print(f"--- {log_file} not found ---")
        print(f"\n{'*'*10} END OF {log_file} {'*'*10}\n")
        print("-" * 80) # Separator between files

    print("\n" + "="*80)
    print("                 END OF AGGREGATED LOGS                 ")
    print("="*80 + "\n")


if __name__ == "__main__":
    # Before running this script, ensure you have run pipeline_orchestrator.py
    # to generate the log files.
    print("Attempting to read and display all log files. Please ensure you have run 'pipeline_orchestrator.py' first.")
    view_logs(LOG_FILES)