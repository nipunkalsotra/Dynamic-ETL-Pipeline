import fitz  # This is the import from PyMuPDF
import markdown
from bs4 import BeautifulSoup
from pathlib import Path

def extract_data(file_path: str) -> str:
    """
    Extracts raw text content from a given file (txt, pdf, or md).
    Uses PyMuPDF (fitz) for PDFs.
    """
    print(f"Extracting data from: {file_path}")
    
    path = Path(file_path)
    extension = path.suffix.lower()
    
    raw_text = ""
    
    try:
        if extension == '.txt':
            # 1. Handle .txt files
            with open(path, 'r', encoding='utf-8') as f:
                raw_text = f.read()
                
        elif extension == '.pdf':
            # 2. Handle .pdf files (NEW WAY)
            with fitz.open(path) as doc:
                for page in doc:
                    raw_text += page.get_text() + "\n"
                
        elif extension == '.md':
            # 3. Handle .md files
            with open(path, 'r', encoding='utf-8') as f:
                md_content = f.read()
            
            html = markdown.markdown(md_content)
            soup = BeautifulSoup(html, 'html.parser')
            raw_text = soup.get_text()
            
        else:
            # 4. Handle unsupported file types
            print(f"Warning: Unsupported file type '{extension}'. Skipping file.")
            return ""

        print("Extraction successful.")
        return raw_text

    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'")
        return ""
    except Exception as e:
        print(f"Error extracting file '{file_path}': {e}")
        return ""