import polars as pl
from pymongo import MongoClient
from datetime import datetime

# --- 1. Database Connection (Same as your old file) ---
def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client["etl_project"]

# --- 2. Schema Inference (Same as your old file) ---
def infer_schema(df: pl.DataFrame) -> dict:
    """Infers a simple schema from a Polars DataFrame."""
    # This creates a dictionary like {"column_name": "DataType"}
    return {col: str(dtype) for col, dtype in df.schema.items()}

# --- 3. NEW: Get Latest Schema ---
def get_latest_schema(registry_collection):
    """Finds the schema document with the highest version number."""
    # .sort("version", -1) finds the highest version.
    latest = registry_collection.find_one(sort=[("version", -1)])
    return latest

# --- 4. NEW: Compare Schemas (The "Diff" Logic) ---
def compare_schemas(old_schema: dict, new_schema: dict) -> list:
    """Compares two schema dictionaries and generates a list of changes."""
    changes = []
    old_fields = set(old_schema.keys())
    new_fields = set(new_schema.keys())

    added_fields = new_fields - old_fields
    removed_fields = old_fields - new_fields
    common_fields = old_fields & new_fields

    for field in added_fields:
        changes.append({
            "action": "add",
            "field": field,
            "new_type": new_schema[field]
        })

    for field in removed_fields:
        changes.append({
            "action": "remove",
            "field": field,
            "old_type": old_schema[field]
        })

    for field in common_fields:
        if old_schema[field] != new_schema[field]:
            changes.append({
                "action": "modify",
                "field": field,
                "old_type": old_schema[field],
                "new_type": new_schema[field]
            })
            
    return changes

# --- 5. NEW: The Main "Evolve" Function ---
def evolve_schema(df: pl.DataFrame):
    """
    The main function to infer, compare, and save a new schema version
    if and only if changes are detected.
    """
    db = get_db()
    registry = db["schema_registry"] # Your collection for schemas
    
    print("Starting schema evolution...")

    # Step 1: Infer the new schema from the DataFrame
    new_schema = infer_schema(df)
    
    # Step 2: Get the most recent schema from the database
    latest_schema_doc = get_latest_schema(registry)
    
    if latest_schema_doc:
        # A schema already exists, let's compare
        latest_schema = latest_schema_doc["schema"]
        version = latest_schema_doc["version"] + 1
        
        # Step 3: Compare old and new
        changes = compare_schemas(latest_schema, new_schema)
        
        if not changes:
            # The schemas are identical. Do nothing.
            print("Schema is identical. No evolution required.")
            return latest_schema_doc
            
        print(f"Schema changes detected. Evolving from v{version-1} to v{version}.")
        
    else:
        # This is the very first schema.
        print("No existing schema found. Creating v1.")
        latest_schema = {}
        version = 1
        # Create a "diff" for the first version
        changes = [{"action": "create", "field": f} for f in new_schema.keys()]

    # Step 4: Save the new version to the database
    new_schema_document = {
        "version": version,
        "created_at": datetime.now(),
        "schema": new_schema,
        "changes": changes  # This is the "diff"
    }
    
    registry.insert_one(new_schema_document)
    print(f"Successfully saved schema v{version}.")
    
    return new_schema_document