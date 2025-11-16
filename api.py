import json
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient
import ollama

# --- Setup ---
app = FastAPI()
ollama_client = ollama.Client()

# --- Pydantic Model ---
# This defines the expected request format for our /query endpoint
class QueryRequest(BaseModel):
    question: str

# --- Database Connection ---
def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client["etl_project"]

def get_latest_schema():
    """Fetches the most recent schema from the registry."""
    db = get_db()
    registry = db["schema_registry"]
    latest_schema_doc = registry.find_one(sort=[("version", -1)])
    
    if latest_schema_doc:
        # We just need the schema part, not the whole document
        return latest_schema_doc["schema"]
    return None

# --- AI Query Prompt ---
def create_mongo_query_prompt(schema: dict, question: str) -> str:
    """Creates the detailed prompt for the AI to translate a question."""
    
    # Convert the schema dictionary to a clean JSON string
    schema_str = json.dumps(schema, indent=2)
    
    prompt = f"""
    You are an expert MongoDB query translator. Your job is to translate a user's
    natural language question into a valid MongoDB aggregation pipeline.

    RULES:
    1.  Respond with *ONLY* a valid, minified JSON array for the pipeline.
    2.  Do not add any explanation, preamble, or markdown formatting (like ```json).
    3.  The collection to be queried is named 'transformed_data'.
    4.  Use the following schema to understand the data structure:
    
    SCHEMA:
    {schema_str}
    
    USER QUESTION:
    "{question}"
    
    MONGODB AGGREGATION PIPELINE:
    """
    return prompt

# --- The API Endpoint ---
@app.post("/query")
async def handle_query(request: QueryRequest):
    """
    The main API endpoint. Receives a question, translates it,
    runs the query, and returns the results.
    """
    print(f"Received query: {request.question}")
    
    # 1. Get the latest database schema
    schema = get_latest_schema()
    if not schema:
        return {"error": "No schema found. Please ingest data first."}

    # 2. Create the AI prompt
    prompt = create_mongo_query_prompt(schema, request.question)
    
    # 3. Call the local AI (Ollama)
    try:
        response = ollama_client.chat(
            model='llama3:8b',
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.0}
        )
        ai_response_string = response['message']['content']
        
        # Clean up the AI's response
        if ai_response_string.startswith("```json"):
            ai_response_string = ai_response_string[7:-3].strip()
            
        print(f"AI generated pipeline: {ai_response_string}")

        # 4. Parse the AI's response (the query)
        pipeline = json.loads(ai_response_string)

    except Exception as e:
        print(f"Error during AI translation or JSON parsing: {e}")
        print(f"AI Raw Response: {ai_response_string}")
        return {"error": "Failed to translate query", "details": str(e)}

    # 5. Run the query against the database
    try:
        db = get_db()
        collection = db["transformed_data"]
        
        # .aggregate() expects a list (the pipeline)
        results = list(collection.aggregate(pipeline))
        
        # MongoDB's _id is not JSON-serializable, so we fix it
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        print(f"Query successful, returning {len(results)} documents.")
        return {"results": results}

    except Exception as e:
        print(f"Error running MongoDB query: {e}")
        return {"error": "Failed to execute query", "details": str(e)}