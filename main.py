import sys
from extract_module import extract_data
from transform import transform_data
from schema_manager import evolve_schema
from load import load_data

def main():
    """
    Runs the complete ETL pipeline for a single file.
    """
    
    # --- 1. SET YOUR INPUT FILE HERE ---
    file_to_process = "mock_input.txt"
    # -----------------------------------
    
    print(f"--- Starting ETL Pipeline for: {file_to_process} ---")
    
    # STEP 1: EXTRACT
    print("\n[Step 1/4] EXTRACTING...")
    raw_text = extract_data(file_to_process)
    
    if not raw_text:
        print(f"Extraction failed or file was empty. Stopping pipeline.")
        return

    # STEP 2: TRANSFORM
    print("\n[Step 2/4] TRANSFORMING (AI Parsing)...")
    df = transform_data(raw_text)
    
    # --- THIS IS THE FIX ---
    # We will no longer stop the pipeline if the AI fails.
    # We will just print a warning and continue.
    if df.is_empty():
        print(f"Warning: AI transformation failed to extract data.")
        print("Continuing to Step 3 and 4 with an empty DataFrame.")
        # The 'return' statement has been removed.
    else:
        print(f"AI successfully extracted {len(df)} records.")
    # --- END OF FIX ---

    # STEP 3: EVOLVE SCHEMA
    # This step will now run even if the DataFrame is empty.
    print("\n[Step 3/4] EVOLVING SCHEMA...")
    evolve_schema(df)
    
    # STEP 4: LOAD DATA
    # This step will also run. Your `load.py` already
    # has a check for an empty DataFrame, so it is safe.
    print("\n[Step 4/4] LOADING DATA...")
    load_data(df)
    
    print("\n--- ETL Pipeline Finished ---")

if __name__ == "__main__":
    # Ensure Ollama is running before starting!
    main()