"""
FastAPI Client
Helper functions to call FastAPI endpoints for embeddings, vector store, search, and LLM
"""

import requests
import pandas as pd
from typing import Dict, List, Optional, Callable
from langchain_core.documents import Document

from config.settings import FASTAPI_SERVICE_URL, VECTOR_INDEX_NAME, MONGODB_DB_NAME
from utils.json_utils import parse_json_with_cleanup

# Global registry for direct search function (set by FastAPI when running internally)
_DIRECT_SEARCH_FUNCTION: Optional[Callable] = None
_DIRECT_LLM_FUNCTION: Optional[Callable] = None

def register_direct_search_function(func: Callable):
    """Register a direct search function to avoid HTTP calls when running inside FastAPI"""
    global _DIRECT_SEARCH_FUNCTION
    _DIRECT_SEARCH_FUNCTION = func

def register_direct_llm_function(func: Callable):
    """Register a direct LLM function to avoid HTTP calls when running inside FastAPI"""
    global _DIRECT_LLM_FUNCTION
    _DIRECT_LLM_FUNCTION = func


def initialize_embeddings_via_api(api_key: str):
    """Initialize embeddings via FastAPI"""
    try:
        response = requests.post(
            f"{FASTAPI_SERVICE_URL}/api/v1/embeddings/initialize",
            json={
                "model": "text-embedding-3-large",
                "api_key": api_key
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Embeddings initialized: {result['model']}")
            return {"type": "fastapi", "model": result['model'], "available": True}
        else:
            print(f"❌ Embeddings init failed: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Embeddings init error: {e}")
        return None


def create_vector_store_via_api(df: pd.DataFrame, collection_name: str, 
                                mongodb_uri: str, db_name: str, 
                                representation_func) -> Optional[Dict]:
    """
    Create vector store via FastAPI for MongoDB.
    
    Returns a dict with collection info.
    """
    print(f"Creating MongoDB collection '{collection_name}' via FastAPI...")
    
    # Prepare documents
    documents = []
    for _, row in df.iterrows():
        content = representation_func(row.to_dict())
        if content.strip():
            documents.append({
                "page_content": content,
                "metadata": row.to_dict()  # Use original column names
            })
    
    if not documents:
        print("FATAL: No valid documents created")
        return None
    
    print(f"Sending {len(documents)} documents to FastAPI...")
    
    try:
        response = requests.post(
            f"{FASTAPI_SERVICE_URL}/api/v1/mongodb/create-collection",
            json={
                "documents": documents,
                "collection_name": collection_name,
                "mongodb_uri": mongodb_uri,
                "db_name": db_name,
                "index_name": VECTOR_INDEX_NAME,
                "drop_old": True
            },
            timeout=300  # 5 minutes for large collections
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ MongoDB collection created via API")
            
            return {
                "type": "fastapi_mongodb",
                "collection_name": collection_name,
                "num_entities": result['num_entities'],
                "db_name": result['db_name']
            }
        else:
            print(f"❌ Collection creation failed: {response.status_code}")
            print(f"   Error: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Collection creation error: {e}")
        import traceback
        traceback.print_exc()
        return None


def vector_search_via_api(query_text: str, collection_name: str, 
                          db_name: str = MONGODB_DB_NAME,
                          top_k: int = 25, return_scores: bool = True) -> List:
    """
    Perform vector search via FastAPI on MongoDB.
    
    Returns list of tuples (document, score) if return_scores=True,
    or list of documents if return_scores=False
    """
    # Use direct search function if available (when running inside FastAPI)
    global _DIRECT_SEARCH_FUNCTION
    if _DIRECT_SEARCH_FUNCTION:
        try:
            return _DIRECT_SEARCH_FUNCTION(
                query_text=query_text,
                collection_name=collection_name,
                db_name=db_name,
                top_k=top_k,
                return_scores=return_scores
            )
        except Exception as e:
            print(f"❌ Direct search failed: {e}, falling back to HTTP")
            # Fall through to HTTP request
    
    # Fall back to HTTP request
    try:
        response = requests.post(
            f"{FASTAPI_SERVICE_URL}/api/v1/mongodb/search",
            json={
                "query_text": query_text,
                "collection_name": collection_name,
                "db_name": db_name,
                "index_name": VECTOR_INDEX_NAME,
                "top_k": top_k,
                "return_scores": return_scores
            },
            timeout=600  # 10 minutes for batch processing
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Convert back to document format expected by main code
            if return_scores:
                return [
                    (Document(page_content=r['page_content'], metadata=r['metadata']), 
                     r['score'])
                    for r in result['results']
                ]
            else:
                return [
                    Document(page_content=r['page_content'], metadata=r['metadata'])
                    for r in result['results']
                ]
        else:
            print(f"❌ Search failed: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"   Error details: {error_detail}")
            except:
                print(f"   Error response: {response.text[:500]}")
            print(f"   Query text (first 200 chars): {query_text[:200]}")
            return []
            
    except Exception as e:
        print(f"❌ Search error: {e}")
        print(f"   Query text (first 200 chars): {query_text[:200]}")
        import traceback
        traceback.print_exc()
        return []


def call_llm_via_api(system_prompt: str, user_prompt: str, response_format: str = "json") -> Optional[Dict]:
    """
    Call LLM via FastAPI generic endpoint.
    
    Returns parsed JSON response or None if failed.
    """
    try:
        response = requests.post(
            f"{FASTAPI_SERVICE_URL}/api/v1/llm/chat",
            json={
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "response_format": response_format
            },
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result.get('content', '{}')

            # Parse JSON content using centralized utility
            try:
                parsed = parse_json_with_cleanup(content)
                # Handle null string as None
                if isinstance(parsed, dict) and parsed.get("chosen_term") == "null":
                    parsed["chosen_term"] = None
                return parsed
            except Exception as e:
                print(f"⚠️ Failed to parse LLM response as JSON: {e}")
                print(f"Raw content: {content[:200]}...")
                return {"chosen_term": None, "reason": "Failed to parse response", "confidence": "None"}
        else:
            print(f"❌ LLM API error: {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return {"chosen_term": None, "reason": f"API error: {response.status_code}", "confidence": "None"}
        
    except requests.RequestException as e:
        print(f"❌ LLM API connection error: {e}")
        return {"chosen_term": None, "reason": "Connection error", "confidence": "None"}
    
    except Exception as e:
        print(f"❌ LLM API call failed: {e}")
        import traceback
        traceback.print_exc()
        return {"chosen_term": None, "reason": str(e), "confidence": "None"}


def call_llm_for_new_term_via_api(system_prompt: str, human_prompt: str) -> Optional[str]:
    """
    Call LLM via FastAPI for new term recommendation.

    Returns raw JSON string response or None if failed.
    """
    try:
        response = requests.post(
            f"{FASTAPI_SERVICE_URL}/api/v1/llm/chat",
            json={
                "system_prompt": system_prompt,
                "user_prompt": human_prompt,
                "response_format": "json"
            },
            timeout=120
        )

        if response.status_code == 200:
            result = response.json()
            content = result.get('content', '{}')
            return content
        else:
            print(f"❌ LLM API error for new term: {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return None

    except requests.RequestException as e:
        print(f"❌ LLM API connection error: {e}")
        return None

    except Exception as e:
        print(f"❌ LLM API call failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def save_mappings_to_mongodb_via_api(
    final_mappings: List[Dict],
    mongodb_uri: str,
    db_name: str = MONGODB_DB_NAME,
    mappings_collection: str = "final_mappings",
    execution_metadata: Optional[Dict] = None
) -> Optional[Dict]:
    """
    Save final mappings to MongoDB via FastAPI endpoint.

    Args:
        final_mappings: List of final CDM mapping dictionaries
        mongodb_uri: MongoDB connection URI
        db_name: Database name (default: cdm_mapping)
        mappings_collection: Collection name for final mappings
        execution_metadata: Additional metadata to attach to all records

    Returns:
        Dict with status and statistics, or None if failed
    """
    try:
        print(f"\n💾 Saving mappings to MongoDB via API...")
        print(f"   Final mappings: {len(final_mappings)}")

        response = requests.post(
            f"{FASTAPI_SERVICE_URL}/api/v1/mongodb/save-mappings",
            json={
                "final_mappings": final_mappings,
                "mongodb_uri": mongodb_uri,
                "db_name": db_name,
                "mappings_collection": mappings_collection,
                "execution_metadata": execution_metadata or {}
            },
            timeout=120
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✅ {result['message']}")
            print(f"   Execution ID: {result.get('execution_id', 'N/A')}")
            print(f"   Collection: {result['mappings_collection']}")
            return result
        else:
            print(f"❌ Save mappings failed: {response.status_code}")
            print(f"   Error: {response.text}")
            return None

    except Exception as e:
        print(f"❌ Save mappings error: {e}")
        import traceback
        traceback.print_exc()
        return None

