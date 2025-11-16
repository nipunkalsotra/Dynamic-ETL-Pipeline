import ollama
import json
import polars as pl
import httpx  # Import the new library

# --- THIS IS THE FIX ---
# Create a custom client that explicitly disables proxies
no_proxy_client = httpx.Client(
    trust_env=False
)

# Pass our no-proxy client to the Ollama client
client = ollama.Client(
    host='http://127.0.0.1:11434',
)
# --- END OF FIX ---


def transform_data(raw_text: str) -> pl.DataFrame:
    """
    Uses a local LLM via Ollama to extract structured data from raw text.
    """
    print(f"Starting AI transformation with Ollama...")
    
    # --- THIS IS THE NEW, SMARTER PROMPT ---
    prompt = """
    You are an expert data extraction system. Your task is to read the following text and create a *single* JSON object that contains all the structured data fragments you find.
    
    Use the fragment's logical name (e.g., "metadata", "inline_json", "html_reviews", "csv_data") as the top-level key for each fragment you parse.
    
    Example Output Format:
    {
      "metadata": { "source": "...", "scraper": "..." },
      "inline_json": { "id": "prod-1001", "title": "Widget A", ... },
      "html_reviews": [ { "author": "Alice", ... }, ... ],
      "csv_data": [ ["ProductID", "Name", ...], ... ]
    }
    
    Respond with *ONLY* the single, clean, valid JSON object. 
    Do not add any text, explanation, or markdown formatting before or after the JSON.
    
    Here is the text:
    """
    # --- END OF NEW PROMPT ---

    full_message = f"{prompt}\n\n{raw_text}"

    try:
        response = client.chat(
            model='llama3:8b',
            messages=[{'role': 'user', 'content': full_message}],
            options={
                'temperature': 0.0,
                'num_predict': 8192  # <-- THIS IS THE FIX: Allow a larger response
            }
        )
        
        ai_response_string = response['message']['content']
        print("AI extraction complete. Parsing JSON...")

        # Find the first '{' and the last '}' to get only the JSON
        start_index = ai_response_string.find('{')
        end_index = ai_response_string.rfind('}')
        
        if start_index == -1 or end_index == -1:
            raise json.JSONDecodeError("Could not find valid JSON object in AI response.", ai_response_string, 0)
        
        json_only_string = ai_response_string[start_index : end_index + 1]
        
        extracted_data = json.loads(json_only_string)
        
        # We get one object, so we must wrap it in a list for Polars
        if not isinstance(extracted_data, list):
            extracted_data = [extracted_data]
            
        df = pl.DataFrame(extracted_data)
        
        print("DataFrame created successfully.")
        return df

    except json.JSONDecodeError as e:
        print(f"Error: AI did not return valid JSON. {e}")
        print(f"AI returned: {ai_response_string}") # This helps debug
        return pl.DataFrame()
        
    except Exception as e:
        if "client is not available" in str(e):
             print("Error: Ollama client is not available. Make sure Ollama is running.")
        print(f"An error occurred during transformation: {e}")
        return pl.DataFrame()