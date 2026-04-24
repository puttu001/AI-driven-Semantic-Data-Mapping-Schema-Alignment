"""
FastAPI Gateway for CDM Mapping - MongoDB Atlas Compatible
Thin API layer - all logic stays in main code.
"""

import sys
from pathlib import Path

# Add project root to Python path to allow imports from core, utils, etc.
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, SecretStr, Field
from typing import List, Dict, Optional, Any, Annotated, Literal
import uvicorn
from langchain_openai import OpenAIEmbeddings
from langchain_mongodb import MongoDBAtlasVectorSearch
from langchain_core.documents import Document
from pymongo import MongoClient
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import io
from pathlib import Path
import time
from datetime import datetime
from dotenv import load_dotenv

from core.database import create_vector_search_index
from config import settings
from config.settings import OBJECT_PARENT_COL, CDM_TABLE_DESC_COL, GLOSSARY_DEFINITION_COL
from utils.data_processing import (
    load_and_clean_csv_file,
    validate_required_columns,
    build_cdm_glossary_dict,
    build_cdm_terms_list,
    create_cdm_representation,
    create_csv_representation
)
from utils.file_operations import save_uploaded_file, save_results_to_csv, save_results_to_mongodb
from utils.validation import run_validation_analysis, print_validation_summary
from term_recommendation.term_recommender import recommend_new_term

load_dotenv(override=True)

app = FastAPI(title="CDM Mapping API Gateway - MongoDB")

# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

EMBEDDINGS_INSTANCE = None
LLM_INSTANCE = None
MONGODB_CLIENT = None
MONGODB_DB = None
VECTOR_STORES = {}  # Store MongoDB vector search instances

# ==========================================================================
# Web- Interactive:UI
# ==========================================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the interactive web UI from frontend folder"""
    ui_path = Path(__file__).parent.parent / "frontend" / "index.html"
    
    if not ui_path.exists():
        return HTMLResponse(content=f"""
        <html><body>
        <h1>CDM Mapping API</h1>
        <p>UI file not found at: {ui_path}</p>
        <p>Please visit <a href="/docs">Swagger UI</a> for API documentation.</p>
        </body></html>
        """)
    
    return FileResponse(ui_path)

# ============================================================================
# MODELS
# ============================================================================

class EmbeddingsInitRequest(BaseModel):
    model: str = "text-embedding-3-large"
    api_key: Optional[str] = None

class EmbeddingsInitResponse(BaseModel):
    status: str
    model: str
    message: str = "Embeddings initialized successfully"

class DocumentModel(BaseModel):
    page_content: str
    metadata: Dict[str, Any]

class CreateCollectionRequest(BaseModel):
    documents: List[DocumentModel]
    collection_name: str
    mongodb_uri: str
    db_name: str = "cdm_mapping"
    index_name: str = "vector_index"
    drop_old: bool = True

class CreateCollectionResponse(BaseModel):
    status: str
    collection_name: str
    num_entities: int
    db_name: str

class VectorSearchRequest(BaseModel):
    query_text: str
    collection_name: str
    db_name: str = "cdm_mapping"
    index_name: str = "vector_index"
    top_k: int = 25
    return_scores: bool = True

class SearchResultModel(BaseModel):
    page_content: str
    metadata: Dict[str, Any]
    score: Optional[float] = None

class VectorSearchResponse(BaseModel):
    results: List[SearchResultModel]
    num_results: int

class LLMRequest(BaseModel):
    system_prompt: str
    user_prompt: str
    response_format: str = "json"

class LLMResponse(BaseModel):
    content: str

class MappingRecord(BaseModel):
    """Single CDM mapping record"""
    csv_table_name: Optional[str] = None
    csv_table_description: Optional[str] = None
    csv_column_name: Optional[str] = None
    csv_column_description: Optional[str] = None
    cdm_parent_name: Optional[str] = None
    cdm_parent_definition: Optional[str] = None
    parent_candidates: Optional[List[Dict[str, Any]]] = None
    cdm_column_name: Optional[str] = None
    cdm_column_definition: Optional[str] = None
    other_candidates: Optional[List[Dict[str, Any]]] = None
    comprehensive_reason: Optional[str] = None
    llm_reason: Optional[str] = None
    final_decision: Optional[str] = None
    app_query_text: Optional[str] = None

class SaveMappingsRequest(BaseModel):
    """Request to save final mappings to MongoDB"""
    final_mappings: List[Dict[str, Any]]
    mongodb_uri: str
    db_name: str = "cdm_mapping"
    mappings_collection: str = "final_mappings"
    execution_metadata: Optional[Dict[str, Any]] = None

class SaveMappingsResponse(BaseModel):
    """Response from save mappings endpoint"""
    status: str
    mappings_saved: int
    mappings_collection: str
    execution_id: Optional[str] = None
    message: str

class ReviewCombinedRequest(BaseModel):
    session_id: str
    action: Optional[Literal['accept_top','reject','choose_candidate','skip','accept_suggested','reject_suggested']] = None
    candidate_index: Optional[int] = None

class APILatencyModel(BaseModel):
    """API latency metrics"""
    total_duration_seconds: float
    avg_time_per_column_seconds: float
    total_columns_processed: int

class PreReviewKPIsModel(BaseModel):
    """Pre-review KPI metrics"""
    avg_confidence_score: float
    accept_vs_reject_ratio: float
    challenger_rejection_rate: float
    acceptance_rate: float
    api_latency: APILatencyModel

class PreReviewBreakdownModel(BaseModel):
    """Pre-review breakdown counts"""
    total_mappings: int
    accepted_mappings: int
    rejected_mappings: int
    unmapped_columns: int
    auto_rejected: int
    total_candidates_evaluated: int
    challenger_rejected_count: int

class PreReviewKPIsResponse(BaseModel):
    """Complete pre-review KPI response"""
    session_id: str
    kpis: PreReviewKPIsModel
    breakdown: PreReviewBreakdownModel

class PostReviewKPIsResponse(BaseModel):
    """Post-review final results response"""
    session_id: str
    total_mapped: int
    avg_confidence_score: float
    unmapped: int
    user_rejected: int


INTERACTIVE_SESSIONS: Dict[str, Dict[str, Any]] = {}

# ============================================================================
# ENDPOINT 1: Initialize Embeddings
# ============================================================================

@app.post("/api/v1/embeddings/initialize", response_model=EmbeddingsInitResponse)
async def initialize_embeddings(request: EmbeddingsInitRequest):
    """Initialize OpenAI embeddings"""
    global EMBEDDINGS_INSTANCE
    
    try:
        key = request.api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise ValueError("API key required")
        
        EMBEDDINGS_INSTANCE = OpenAIEmbeddings(
            model=request.model,
            api_key=SecretStr(key)
        )
        
        return EmbeddingsInitResponse(status="success", model=request.model)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ENDPOINT 2: Create MongoDB Collection
# ============================================================================

@app.post("/api/v1/mongodb/create-collection", response_model=CreateCollectionResponse)
async def create_mongodb_collection(request: CreateCollectionRequest):
    """
    Create MongoDB Atlas vector collection.
    Main code prepares documents with all logic.
    """
    global EMBEDDINGS_INSTANCE, MONGODB_CLIENT, MONGODB_DB, VECTOR_STORES
    
    if not EMBEDDINGS_INSTANCE:
        raise HTTPException(status_code=400, detail="Embeddings not initialized")
    
    try:
        # Connect to MongoDB if not already connected
        if not MONGODB_CLIENT:
            MONGODB_CLIENT = MongoClient(request.mongodb_uri)
            MONGODB_CLIENT.admin.command('ping')  # Test connection
        
        MONGODB_DB = MONGODB_CLIENT[request.db_name]
        collection = MONGODB_DB[request.collection_name]
        
        # Drop old collection if requested
        if request.drop_old:
            collection.delete_many({})
        
        # Convert to LangChain documents
        documents = [
            Document(page_content=doc.page_content, metadata=doc.metadata)
            for doc in request.documents
        ]
        
        # Create vector store (generates embeddings via OpenAI API)
        vector_store = MongoDBAtlasVectorSearch.from_documents(
            documents=documents,
            embedding=EMBEDDINGS_INSTANCE,
            collection=collection,
            index_name=request.index_name
        )
        
        # Create vector search index
        create_vector_search_index(collection, request.index_name)
        
        # Count documents in collection
        num_docs = collection.count_documents({})
        print(f"✅ Created collection '{request.collection_name}' with {num_docs} documents")
        
        # Store reference with document count
        store_key = f"{request.db_name}.{request.collection_name}"
        VECTOR_STORES[store_key] = {
            "vector_store": vector_store,
            "collection": collection,
            "index_name": request.index_name,
            "num_docs": num_docs
        }
        
        return CreateCollectionResponse(
            status="success",
            collection_name=request.collection_name,
            num_entities=num_docs,
            db_name=request.db_name
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINT 3: Vector Search (MongoDB)
# ============================================================================

@app.post("/api/v1/mongodb/search", response_model=VectorSearchResponse)
async def vector_search_mongodb(request: VectorSearchRequest):
    """
    Perform vector search on MongoDB Atlas.
    Main code handles all result processing.
    """
    global EMBEDDINGS_INSTANCE, VECTOR_STORES
    
    if not EMBEDDINGS_INSTANCE:
        raise HTTPException(status_code=400, detail="Embeddings not initialized")
    
    store_key = f"{request.db_name}.{request.collection_name}"
    if store_key not in VECTOR_STORES:
        raise HTTPException(status_code=404, detail=f"Collection not found: {store_key}")
    
    try:
        vector_store = VECTOR_STORES[store_key]["vector_store"]
        
        # Collection info for debugging if needed
        collection_info = VECTOR_STORES[store_key]
        
        # Perform search (embeds query via OpenAI API)
        if request.return_scores:
            try:
                results = vector_store.similarity_search_with_score(
                    request.query_text,
                    k=request.top_k
                )
                # Retry once if no results (possible index warming issue)
                if not results:
                    import time
                    time.sleep(1)
                    results = vector_store.similarity_search_with_score(
                        request.query_text,
                        k=request.top_k
                    )
            except Exception as search_error:
                print(f"❌ Vector search error: {search_error}")
                results = []
            
            search_results = [
                SearchResultModel(
                    page_content=doc.page_content,
                    metadata=doc.metadata,
                    score=float(score)
                )
                for doc, score in results
            ]
        else:
            results = vector_store.similarity_search(
                request.query_text,
                k=request.top_k
            )
            search_results = [
                SearchResultModel(
                    page_content=doc.page_content,
                    metadata=doc.metadata,
                    score=None
                )
                for doc in results
            ]
        
        return VectorSearchResponse(
            results=search_results,
            num_results=len(search_results)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ENDPOINT 4: Generic LLM Call
# ============================================================================

@app.post("/api/v1/llm/chat", response_model=LLMResponse)
async def llm_chat(request: LLMRequest):
    """
    Generic LLM endpoint - NO prompts, NO formatting, NO business logic.
    Main code sends complete prompts.
    """
    global LLM_INSTANCE

    # Lazy init LLM
    if not LLM_INSTANCE:
        try:
            from langchain_openai import ChatOpenAI
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set")

            # Initialize with JSON mode support if needed
            LLM_INSTANCE = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.0,
                api_key=api_key,
                max_tokens=2000,
                timeout=300  # 5 minutes for complex reasoning
            )
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"LLM init failed: {e}")

    try:
        from langchain_core.messages import SystemMessage, HumanMessage
        from langchain_core.output_parsers import JsonOutputParser
        import json

        # Build messages with JSON format enforcement
        if request.response_format == "json":
            # Add explicit JSON instruction to system prompt
            enhanced_system_prompt = request.system_prompt
            if "OUTPUT FORMAT" in request.system_prompt or "JSON" in request.system_prompt.upper():
                # System prompt already has JSON instructions
                enhanced_system_prompt = request.system_prompt + "\n\nIMPORTANT: You MUST respond with valid JSON only. Do not include any markdown formatting, explanations, or text outside the JSON structure."

            messages = [
                SystemMessage(content=enhanced_system_prompt),
                HumanMessage(content=request.user_prompt)
            ]

            # Use model_kwargs to enforce JSON mode for compatible models
            llm_with_json = LLM_INSTANCE.bind(response_format={"type": "json_object"})
            response = llm_with_json.invoke(messages)

            try:
                # Validate and format JSON
                parsed_json = json.loads(response.content)
                content = json.dumps(parsed_json)
            except json.JSONDecodeError as e:
                # If JSON parsing fails, return error
                raise HTTPException(
                    status_code=500,
                    detail=f"LLM did not return valid JSON. Parse error: {e}. Content: {response.content[:200]}"
                )
        else:
            # Non-JSON response
            messages = [
                SystemMessage(content=request.system_prompt),
                HumanMessage(content=request.user_prompt)
            ]
            response = LLM_INSTANCE.invoke(messages)
            content = response.content

        return LLMResponse(content=content)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ENDPOINT 5: Save Mappings to MongoDB
# ============================================================================

@app.post("/api/v1/mongodb/save-mappings", response_model=SaveMappingsResponse)
async def save_mappings_to_mongodb(request: SaveMappingsRequest):
    """
    Save final mappings to MongoDB collection.
    This endpoint stores the results of the CDM mapping workflow.
    """
    try:
        from datetime import datetime

        # Connect to MongoDB
        client = MongoClient(request.mongodb_uri)
        db = client[request.db_name]

        # Test connection
        client.admin.command('ping')

        # Generate execution ID for tracking
        execution_id = f"exec_{int(datetime.now().timestamp())}"

        # Prepare metadata to add to each document
        base_metadata = {
            "execution_id": execution_id,
            "created_at": datetime.now().isoformat(),
            **(request.execution_metadata or {})
        }

        mappings_saved = 0

        # Save final mappings
        if request.final_mappings:
            mappings_collection = db[request.mappings_collection]

            # Add metadata to each mapping
            mappings_with_metadata = [
                {**mapping, **base_metadata, "record_type": "final_mapping"}
                for mapping in request.final_mappings
            ]

            result = mappings_collection.insert_many(mappings_with_metadata)
            mappings_saved = len(result.inserted_ids)
            print(f"✅ Saved {mappings_saved} final mappings to {request.mappings_collection}")

        # Close connection
        client.close()

        return SaveMappingsResponse(
            status="success",
            mappings_saved=mappings_saved,
            mappings_collection=request.mappings_collection,
            execution_id=execution_id,
            message=f"Successfully saved {mappings_saved} mappings"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving to MongoDB: {str(e)}")


# ==========================================================================
# INTERACTIVE REVIEW HELPERS
# ==========================================================================

def _build_final_mapping_from_choice(
    suggestion: Dict[str, Any],
    chosen_term: Optional[str],
    cdm_glossary_dict: Dict[str, Dict[str, Any]],
    decision_label: str
) -> Dict[str, Any]:
    cdm_row = cdm_glossary_dict.get(chosen_term, {}) if chosen_term else {}

    return {
        'csv_table_name': suggestion.get('csv_table_name'),
        'csv_table_description': suggestion.get('csv_table_description'),
        'csv_column_name': suggestion.get('csv_column_name'),
        'csv_column_description': suggestion.get('csv_column_description'),
        'cdm_parent_name': cdm_row.get(OBJECT_PARENT_COL) if chosen_term else None,
        'cdm_parent_definition': cdm_row.get(CDM_TABLE_DESC_COL, '') if chosen_term else '',
        'parent_candidates': suggestion.get('parent_candidates', []),
        'cdm_column_name': chosen_term,
        'cdm_column_definition': cdm_row.get(GLOSSARY_DEFINITION_COL, '') if chosen_term else '',
        'other_candidates': suggestion.get('other_candidates', []),
        'comprehensive_reason': decision_label,
        'final_decision': decision_label,
        'app_query_text': suggestion.get('app_query_text')
    }


def _normalize_mongodb_uri(mongodb_uri: Optional[str]) -> Optional[str]:
    if not mongodb_uri:
        return None
    candidate = mongodb_uri.strip().strip('"').strip("'")
    lowered = candidate.lower()
    if lowered == "string" or lowered.startswith("mongodb://string") or lowered.startswith("mongodb+srv://string"):
        return None
    return candidate


def _calculate_pre_review_kpis(session: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate pre-review KPI metrics from session data.
    Called before human review starts.
    """
    suggestions = session.get("suggestions", [])
    auto_rejected = session.get("auto_rejected", [])
    unmapped = session.get("unmapped_columns", [])
    
    # Calculate confidence scores from all suggestions with candidates
    all_scores = []
    total_candidates_evaluated = 0
    challenger_rejected_count = 0
    
    for suggestion in suggestions:
        llm_candidates = suggestion.get("llm_candidates", [])
        for candidate in llm_candidates:
            score = candidate.get("score", 0)
            all_scores.append(score)
            total_candidates_evaluated += 1
            
            # Count challenger rejections
            verdict = candidate.get("challenger_verdict", "")
            if verdict == "REJECT":
                challenger_rejected_count += 1
    
    # Also count candidates from auto_rejected
    for suggestion in auto_rejected:
        llm_candidates = suggestion.get("llm_candidates", [])
        for candidate in llm_candidates:
            score = candidate.get("score", 0)
            all_scores.append(score)
            total_candidates_evaluated += 1
            
            verdict = candidate.get("challenger_verdict", "")
            if verdict == "REJECT":
                challenger_rejected_count += 1
    
    # Calculate metrics
    avg_confidence_score = sum(all_scores) / len(all_scores) if all_scores else 0.0
    
    # Suggestions with at least one candidate that passed
    accepted_mappings = len([s for s in suggestions if s.get("llm_candidates")])
    rejected_mappings = len(auto_rejected)
    total_mappings = accepted_mappings + rejected_mappings
    
    accept_vs_reject_ratio = accepted_mappings / rejected_mappings if rejected_mappings > 0 else float(accepted_mappings)
    challenger_rejection_rate = (challenger_rejected_count / total_candidates_evaluated * 100) if total_candidates_evaluated > 0 else 0.0
    acceptance_rate = (accepted_mappings / total_mappings * 100) if total_mappings > 0 else 0.0
    
    # Timing metrics
    workflow_duration = session.get("workflow_duration_seconds", 0.0)
    total_columns = session.get("total_columns_processed", 0)
    avg_time_per_column = workflow_duration / total_columns if total_columns > 0 else 0.0
    
    return {
        "session_id": session.get("session_id", "unknown"),
        "kpis": {
            "avg_confidence_score": round(avg_confidence_score, 2),
            "accept_vs_reject_ratio": round(accept_vs_reject_ratio, 2),
            "challenger_rejection_rate": round(challenger_rejection_rate, 2),
            "acceptance_rate": round(acceptance_rate, 2),
            "api_latency": {
                "total_duration_seconds": round(workflow_duration, 2),
                "avg_time_per_column_seconds": round(avg_time_per_column, 2),
                "total_columns_processed": total_columns
            }
        },
        "breakdown": {
            "total_mappings": total_mappings,
            "accepted_mappings": accepted_mappings,
            "rejected_mappings": rejected_mappings,
            "unmapped_columns": len(unmapped),
            "auto_rejected": len(auto_rejected),
            "total_candidates_evaluated": total_candidates_evaluated,
            "challenger_rejected_count": challenger_rejected_count
        }
    }


def _calculate_post_review_kpis(session: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate post-review KPI metrics from session data.
    Called after human review completes.
    """
    final_mappings = session.get("final_mappings", [])
    unmapped_columns = session.get("unmapped_columns", [])
    
    # Calculate average confidence score from accepted mappings
    mapped_scores = []
    for mapping in final_mappings:
        # Try to extract score from the mapping
        # The score might be in different places depending on how it was saved
        candidates = mapping.get("other_candidates", [])
        if candidates and len(candidates) > 0:
            # First candidate is usually the chosen one
            score = candidates[0].get("score", 0)
            mapped_scores.append(score)
    
    avg_confidence_score = sum(mapped_scores) / len(mapped_scores) if mapped_scores else 0.0
    
    # Count user rejections (vs auto-rejections)
    user_rejected_count = 0
    for unmapped in unmapped_columns:
        decision = unmapped.get("comprehensive_reason", "")
        if "User Rejected" in decision or "User rejected" in decision:
            user_rejected_count += 1
    
    return {
        "session_id": session.get("session_id", "unknown"),
        "total_mapped": len(final_mappings),
        "avg_confidence_score": round(avg_confidence_score, 2),
        "unmapped": len(unmapped_columns),
        "user_rejected": user_rejected_count
    }


def _review_next_response(session: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> JSONResponse:
    idx = session.get("index", 0)
    suggestions = session.get("suggestions", [])

    if idx >= len(suggestions):
        final_mappings = session.get("final_mappings", [])
        unmapped_columns = session.get("unmapped_columns", [])

        # Do NOT append auto_rejected to unmapped_columns here
        # Auto-rejected items are handled during the review process
        # If user skips/rejects them, they get added appropriately

        output_suffix = f"_interactive_{int(time.time())}"
        save_results_to_csv(final_mappings, unmapped_columns, output_suffix)

        mapped_filename = f"Final_CDM_Mappings{output_suffix}.csv"
        unmapped_filename = f"Unmapped_Columns{output_suffix}.csv"

        return JSONResponse(content={
            "done": True,
            "status": "success",
            "message": "Interactive review completed",
            "output_files": {
                "mapped_file": mapped_filename,
                "unmapped_file": unmapped_filename,
            }
        })
        # return JSONResponse(content={"done": True})
    response_payload = {
        "done": False,
        "index": idx,
        "total": len(suggestions),
        "suggestion": suggestions[idx]
    }
    if extra:
        response_payload.update(extra)
    return JSONResponse(content=response_payload)


# CHANGED: pending recommendation response
def _pending_recommendation_response(session: Dict[str, Any]) -> JSONResponse:
    pending_index = session.get("pending_index")
    recommendation = session.get("pending_recommendation")
    suggestions = session.get("suggestions", [])

    if pending_index is None or recommendation is None or pending_index >= len(suggestions):
        return _review_next_response(session)

    return JSONResponse(content={
        "done": False,
        "index": pending_index,
        "total": len(suggestions),
        "suggestion": suggestions[pending_index],
        "awaiting_recommendation": True,
        "recommendation": recommendation
    })


# CHANGED: helper to generate recommendation on skip
def _build_skip_recommendation(
    suggestion: Dict[str, Any],
    cdm_glossary_dict: Dict[str, Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    csv_term_details = {
        "csv_table_name": suggestion.get("csv_table_name"),
        "csv_table_description": suggestion.get("csv_table_description"),
        "csv_column_name": suggestion.get("csv_column_name"),
        "csv_column_description": suggestion.get("csv_column_description")
    }
    existing_cdm_parents = sorted({
        v.get(OBJECT_PARENT_COL)
        for v in cdm_glossary_dict.values()
        if v.get(OBJECT_PARENT_COL)
    })
    return recommend_new_term(
        csv_term_details=csv_term_details,
        existing_cdm_parents=existing_cdm_parents,
        rejection_reason="User Skipped - Needs new term suggestion",
        llm={"type": "fastapi", "available": True}
    )

async def _process_workflow_background(
    session_id: str,
    api_key: Optional[str],
    mongodb_uri: Optional[str],
    cdm_content: bytes,
    cdm_filename: str,
    mapping_content: bytes,
    mapping_filename: str
):
    """
    Background task to process the workflow.
    Updates session status as it progresses.
    """
    global EMBEDDINGS_INSTANCE
    
    try:
        print(f"\n{'='*60}", flush=True)
        print(f"🚀 WORKFLOW STARTED: {session_id}", flush=True)
        print(f"{'='*60}", flush=True)
        
        # Update status
        INTERACTIVE_SESSIONS[session_id]["status"] = "processing"
        INTERACTIVE_SESSIONS[session_id]["message"] = "Validating and preparing files..."
        print("📋 Phase 1: Validating and preparing files...", flush=True)
        
        key = api_key or os.getenv("OPENAI_API_KEY")
        mongo_uri = _normalize_mongodb_uri(mongodb_uri) or os.getenv("MONGODB_URI")

        if not key:
            INTERACTIVE_SESSIONS[session_id]["status"] = "error"
            INTERACTIVE_SESSIONS[session_id]["message"] = "OpenAI API key required"
            return
        if not mongo_uri:
            INTERACTIVE_SESSIONS[session_id]["status"] = "error"
            INTERACTIVE_SESSIONS[session_id]["message"] = "MongoDB URI required"
            return

        if not EMBEDDINGS_INSTANCE:
            EMBEDDINGS_INSTANCE = OpenAIEmbeddings(
                model="text-embedding-3-large",
                api_key=SecretStr(key)
            )

        INTERACTIVE_SESSIONS[session_id]["message"] = "Loading and validating CSV files..."
        print("📂 Phase 2: Loading and validating CSV files...", flush=True)
        
        cdm_path = save_uploaded_file(cdm_content, cdm_filename, file_type='cdm')
        mapping_path = save_uploaded_file(mapping_content, mapping_filename, file_type='mapping')
        print(f"   ✓ CDM file saved: {cdm_filename}", flush=True)
        print(f"   ✓ Mapping file saved: {mapping_filename}", flush=True)

        cdm_df = load_and_clean_csv_file(str(cdm_path))
        mapping_df = load_and_clean_csv_file(str(mapping_path))

        if not validate_required_columns(cdm_df, [], "CDM"):
            INTERACTIVE_SESSIONS[session_id]["status"] = "error"
            INTERACTIVE_SESSIONS[session_id]["message"] = "CDM file missing required columns"
            return

        csv_required_columns = [settings.CSV_TABLE_NAME_COL, settings.CSV_COLUMN_NAME_COL]
        if not validate_required_columns(mapping_df, csv_required_columns, "CSV"):
            INTERACTIVE_SESSIONS[session_id]["status"] = "error"
            INTERACTIVE_SESSIONS[session_id]["message"] = "Mapping file missing required columns"
            return

        INTERACTIVE_SESSIONS[session_id]["message"] = "Building glossary and documents..."
        print("📚 Phase 3: Building glossary and documents...", flush=True)
        
        cdm_glossary_dict = build_cdm_glossary_dict(cdm_df)
        cdm_terms_list = build_cdm_terms_list(cdm_df)
        print(f"   ✓ CDM glossary built: {len(cdm_glossary_dict)} terms", flush=True)
        print(f"   ✓ CDM terms list: {len(cdm_terms_list)} entries", flush=True)

        cdm_documents = []
        for _, row in cdm_df.iterrows():
            content = create_cdm_representation(row.to_dict())
            if content.strip():
                cdm_documents.append(DocumentModel(page_content=content, metadata=row.to_dict()))

        csv_documents = []
        for _, row in mapping_df.iterrows():
            content = create_csv_representation(row.to_dict())
            if content.strip():
                csv_documents.append(DocumentModel(page_content=content, metadata=row.to_dict()))

        if not cdm_documents:
            INTERACTIVE_SESSIONS[session_id]["status"] = "error"
            INTERACTIVE_SESSIONS[session_id]["message"] = "No valid CDM documents found"
            return
        if not csv_documents:
            INTERACTIVE_SESSIONS[session_id]["status"] = "error"
            INTERACTIVE_SESSIONS[session_id]["message"] = "No valid mapping documents found"
            return

        INTERACTIVE_SESSIONS[session_id]["message"] = "Creating vector collections..."
        print("🗄️  Phase 4: Creating vector collections...", flush=True)
        
        cdm_request = CreateCollectionRequest(
            documents=cdm_documents,
            collection_name=settings.CDM_COLLECTION_NAME,
            mongodb_uri=mongo_uri,
            db_name=settings.MONGODB_DB_NAME,
            index_name=settings.VECTOR_INDEX_NAME,
            drop_old=True
        )
        cdm_response = await create_mongodb_collection(cdm_request)
        print(f"   ✓ CDM collection created: {cdm_response.num_entities} entities", flush=True)

        csv_request = CreateCollectionRequest(
            documents=csv_documents,
            collection_name=settings.CSV_COLLECTION_NAME,
            mongodb_uri=mongo_uri,
            db_name=settings.MONGODB_DB_NAME,
            index_name=settings.VECTOR_INDEX_NAME,
            drop_old=True
        )
        csv_response = await create_mongodb_collection(csv_request)
        print(f"   ✓ CSV collection created: {csv_response.num_entities} entities", flush=True)

        cdm_collection_info = {
            "collection_name": cdm_response.collection_name,
            "db_name": cdm_response.db_name,
            "num_entities": cdm_response.num_entities
        }
        csv_collection_info = {
            "collection_name": csv_response.collection_name,
            "db_name": csv_response.db_name,
            "num_entities": csv_response.num_entities
        }

        # Wait for vector indexes and API endpoints to fully initialize
        INTERACTIVE_SESSIONS[session_id]["message"] = "Initializing vector indexes..."
        print("\n⏳ Waiting for vector indexes and API endpoints initialization...", flush=True)
        print("   Allowing 15 seconds for endpoints to warm up and indexes to stabilize...", flush=True)
        
        # Progressive wait with status updates for better UX
        for i in range(15):
            await asyncio.sleep(1)
            INTERACTIVE_SESSIONS[session_id]["message"] = f"Initializing vector indexes... ({i+1}/10s)"
        
        print("✅ Systems ready - starting workflow", flush=True)
        
        # Update status to processing
        INTERACTIVE_SESSIONS[session_id]["status"] = "processing"
        INTERACTIVE_SESSIONS[session_id]["message"] = "Processing data with LLM..."

        # ====================================================================
        # CRITICAL: Register direct search function to avoid HTTP deadlock
        # ====================================================================
        from api.api_client import register_direct_search_function
        
        def direct_vector_search(query_text: str, collection_name: str, 
                                db_name: str, top_k: int = 25, return_scores: bool = True):
            """Direct MongoDB vector search without HTTP calls"""
            global EMBEDDINGS_INSTANCE, VECTOR_STORES
            
            store_key = f"{db_name}.{collection_name}"
            if store_key not in VECTOR_STORES:
                print(f"⚠️ Collection not found: {store_key}, available: {list(VECTOR_STORES.keys())}", flush=True)
                return []
            
            vector_store = VECTOR_STORES[store_key]["vector_store"]
            
            try:
                if return_scores:
                    results = vector_store.similarity_search_with_score(query_text, k=top_k)
                    # Convert to expected format: list of (Document, score) tuples
                    return results
                else:
                    results = vector_store.similarity_search(query_text, k=top_k)
                    return results
            except Exception as e:
                print(f"❌ Direct vector search error: {e}", flush=True)
                import traceback
                traceback.print_exc()
                return []
        
        # Register the direct search function
        register_direct_search_function(direct_vector_search)
        print("✅ Registered direct vector search (no HTTP calls)", flush=True)
        
        # ====================================================================
        # CRITICAL: Register direct LLM function to avoid HTTP deadlock
        # ====================================================================
        from workflow.llm_operations import register_direct_llm_function
        
        def direct_llm_call(system_prompt: str, user_prompt: str, response_format: str = "json"):
            """Direct LLM call without HTTP requests"""
            global LLM_INSTANCE
            
            # Lazy init LLM if needed
            if not LLM_INSTANCE:
                try:
                    from langchain_openai import ChatOpenAI
                    api_key = os.getenv("OPENAI_API_KEY")
                    if not api_key:
                        raise ValueError("OPENAI_API_KEY not set")
                    
                    LLM_INSTANCE = ChatOpenAI(
                        model="gpt-4o-mini",
                        temperature=0.0,
                        api_key=api_key,
                        max_tokens=2000,
                        timeout=300
                    )
                except Exception as e:
                    print(f"❌ LLM init failed: {e}", flush=True)
                    raise
            
            try:
                from langchain_core.messages import SystemMessage, HumanMessage
                import json
                
                if response_format == "json":
                    # Add explicit JSON instruction
                    enhanced_system_prompt = system_prompt
                    if "OUTPUT FORMAT" in system_prompt or "JSON" in system_prompt.upper():
                        enhanced_system_prompt = system_prompt + "\n\nIMPORTANT: You MUST respond with valid JSON only. Do not include any markdown formatting, explanations, or text outside the JSON structure."
                    
                    messages = [
                        SystemMessage(content=enhanced_system_prompt),
                        HumanMessage(content=user_prompt)
                    ]
                    
                    # Use model_kwargs to enforce JSON mode
                    llm_with_json = LLM_INSTANCE.bind(response_format={"type": "json_object"})
                    response = llm_with_json.invoke(messages)
                    
                    try:
                        # Validate and format JSON
                        parsed_json = json.loads(response.content)
                        return json.dumps(parsed_json)
                    except json.JSONDecodeError as e:
                        print(f"❌ LLM JSON parse error: {e}", flush=True)
                        raise ValueError(f"LLM did not return valid JSON: {response.content[:200]}")
                else:
                    # Non-JSON response
                    messages = [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=user_prompt)
                    ]
                    response = LLM_INSTANCE.invoke(messages)
                    return response.content
                    
            except Exception as e:
                print(f"❌ Direct LLM call error: {e}", flush=True)
                import traceback
                traceback.print_exc()
                raise
        
        # Register the direct LLM function
        register_direct_llm_function(direct_llm_call)
        print("✅ Registered direct LLM call (no HTTP calls)", flush=True)
        
        # ====================================================================

        INTERACTIVE_SESSIONS[session_id]["status"] = "processing"
        INTERACTIVE_SESSIONS[session_id]["message"] = "Running CDM mapping workflow..."
        print("\n🧠 Phase 5: Running CDM mapping workflow (LLM processing)...", flush=True)
        
        from workflow.enhanced_workflow import EnhancedInteractiveMappingWorkflow

        workflow = EnhancedInteractiveMappingWorkflow(
            cdm_collection_info=cdm_collection_info,
            csv_collection_info=csv_collection_info,
            cdm_glossary_dict=cdm_glossary_dict,
            cdm_terms_list=cdm_terms_list,
            llm={"type": "fastapi", "available": True}
        )

        csv_data_rows = []
        for _, row in mapping_df.iterrows():
            csv_data_rows.append({
                settings.CSV_TABLE_NAME_COL: row.get(settings.CSV_TABLE_NAME_COL, ''),
                settings.CSV_COLUMN_NAME_COL: row.get(settings.CSV_COLUMN_NAME_COL, ''),
                settings.CSV_COLUMN_DESC_COL: row.get(settings.CSV_COLUMN_DESC_COL, ''),
                settings.CSV_TABLE_DESC_COL: row.get(settings.CSV_TABLE_DESC_COL, '')
            })
        
        # Store total columns for progress tracking
        INTERACTIVE_SESSIONS[session_id]["total_columns_processed"] = len(csv_data_rows)
        INTERACTIVE_SESSIONS[session_id]["current_row_processed"] = 0
        print(f"   Processing {len(csv_data_rows)} columns with proposer/challenger agents...", flush=True)

        # Progress callback to update session
        def update_progress(current: int, total: int):
            try:
                INTERACTIVE_SESSIONS[session_id]["current_row_processed"] = current
                INTERACTIVE_SESSIONS[session_id]["message"] = f"Processing row {current}/{total}..."
                print(f"   ⏳ Progress: {current}/{total} ({current/total*100:.1f}%)", flush=True)
            except Exception as e:
                print(f"⚠️ Progress callback error: {e}", flush=True)

        # Track processing time
        workflow_start_time = time.time()
        print("   ⏳ This may take 20-245 seconds depending on data size...", flush=True)
        
        # Run synchronous workflow in thread executor to avoid blocking async event loop
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            result = await loop.run_in_executor(
                executor, 
                workflow.run_batch_process, 
                csv_data_rows, 
                update_progress
            )
        
        workflow_end_time = time.time()
        workflow_duration = workflow_end_time - workflow_start_time

        suggestions = result.get('suggestions', [])
        unmapped = result.get('unmapped', [])
        auto_rejected = result.get('auto_rejected', [])
        
        print(f"\n   ✅ Workflow completed in {workflow_duration:.2f} seconds", flush=True)
        print(f"   📊 Results: {len(suggestions)} suggestions, {len(unmapped)} unmapped, {len(auto_rejected)} auto-rejected", flush=True)

        # ============================================================
        # NEW: Optional Validation Analysis
        # ============================================================
        INTERACTIVE_SESSIONS[session_id]["message"] = "Running validation analysis..."
        
        validation_report = None
        validation_file = Path("Inputs/Validation/starbucks_validate.csv")
        
        if validation_file.exists():
            try:
                print("\n🔍 Running validation analysis against ground truth...", flush=True)
                validation_report = run_validation_analysis(
                    suggestions=suggestions,
                    validation_file_path=str(validation_file)
                )
                print_validation_summary(validation_report)
            except Exception as e:
                print(f"⚠️ Validation failed: {e}", flush=True)
                import traceback
                traceback.print_exc()
                validation_report = {"error": str(e)}
        else:
            print(f"ℹ️ No validation file found at {validation_file}", flush=True)
        # ===========================================================

        # Update session with results
        INTERACTIVE_SESSIONS[session_id].update({
            "suggestions": suggestions,
            "index": 0,
            "final_mappings": [],
            "unmapped_columns": list(unmapped),
            "auto_rejected": auto_rejected,
            "cdm_glossary_dict": cdm_glossary_dict,
            "validation_report": validation_report,
            "workflow_duration_seconds": workflow_duration,
            "status": "ready_for_review",
            "message": "Workflow completed. Ready for review."
        })
        
        print(f"\n{'='*60}", flush=True)
        print(f"✅ SESSION READY: {session_id}", flush=True)
        print(f"   Status: ready_for_review", flush=True)
        print(f"   Duration: {workflow_duration:.2f}s", flush=True)
        print(f"   Next: Call GET /api/v1/kpis/session/{session_id}", flush=True)
        print(f"{'='*60}\n", flush=True)
        
    except Exception as e:
        print(f"❌ Background processing error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        INTERACTIVE_SESSIONS[session_id]["status"] = "error"
        INTERACTIVE_SESSIONS[session_id]["message"] = f"Processing error: {str(e)}"




# ==========================================================================
# ENDPOINT 6A: Run Mapping (Interactive Review)
# ==========================================================================

@app.post("/api/v1/run-mapping-interactive")
async def run_mapping_interactive(
    background_tasks: BackgroundTasks,
    api_key: str = Form(None),
    mongodb_uri: str = Form(None),
    cdm_file: UploadFile = File(...),
    mapping_file: UploadFile = File(...)
):
    """
    Process uploaded files and return session_id immediately.
    Processing runs in background.
    """
    try:
        # Read file contents
        cdm_content = await cdm_file.read()
        mapping_content = await mapping_file.read()
        
        # Create session immediately
        session_id = f"session_{int(time.time())}"
        INTERACTIVE_SESSIONS[session_id] = {
            "session_id": session_id,
            "status": "initializing",
            "message": "Session created. Starting workflow...",
            "suggestions": [],
            "index": 0,
            "final_mappings": [],
            "unmapped_columns": [],
            "auto_rejected": [],
            "session_start_time": time.time(),
            "workflow_duration_seconds": 0,
            "total_columns_processed": 0
        }
        
        # Start background processing
        background_tasks.add_task(
            _process_workflow_background,
            session_id,
            api_key,
            mongodb_uri,
            cdm_content,
            cdm_file.filename,
            mapping_content,
            mapping_file.filename
        )
        
        # Return session_id immediately
        return JSONResponse(content={
            "status": "processing",
            "message": "Workflow started. Connect to /api/v1/session/{session_id}/progress-stream for real-time updates.",
            "session_id": session_id
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")



# ==========================================================================
# ENDPOINT 6B: Review Combined (Next or Decision)
# ==========================================================================

@app.post("/api/v1/review/combined")
async def review_combined(request: ReviewCombinedRequest):
    session = INTERACTIVE_SESSIONS.get(request.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # If no action provided, behave like review_next
    if not request.action:
        if session.get("pending_recommendation") is not None:
            return _pending_recommendation_response(session)
        return _review_next_response(session)

    idx = session.get("index", 0)
    suggestions = session.get("suggestions", [])
    if idx >= len(suggestions):
        final_mappings = session.get("final_mappings", [])
        unmapped_columns = session.get("unmapped_columns", [])

        for ar in session.get("auto_rejected", []):
            unmapped_columns.append({
                'csv_table_name': ar.get('csv_table_name'),
                'csv_table_description': ar.get('csv_table_description'),
                'csv_column_name': ar.get('csv_column_name'),
                'csv_column_description': ar.get('csv_column_description'),
                'other_candidates': ar.get('other_candidates', []),
                'parent_candidates': ar.get('parent_candidates', []),
                'comprehensive_reason': ar.get('comprehensive_reason', 'Auto-rejected'),
                'error': ar.get('comprehensive_reason', 'Auto-rejected')
            })

        output_suffix = f"_interactive_{int(time.time())}"
        save_results_to_csv(final_mappings, unmapped_columns, output_suffix)

        mapped_filename = f"Final_CDM_Mappings{output_suffix}.csv"
        unmapped_filename = f"Unmapped_Columns{output_suffix}.csv"

        return JSONResponse(content={
            "done": True,
            "status": "success",
            "message": "Interactive review completed",
            "output_files": {
                "mapped_file": mapped_filename,
                "unmapped_file": unmapped_filename,
            }
        })
        # return JSONResponse(content={"done": True})

    # CHANGED: handle pending recommendation decision
    if request.action in {"accept_suggested", "reject_suggested"}:
        if session.get("pending_recommendation") is None:
            raise HTTPException(status_code=400, detail="No pending recommendation")

        pending_index = session.get("pending_index", idx)
        suggestion = suggestions[pending_index]
        recommendation = session.get("pending_recommendation")

        if request.action == "accept_suggested":
            session["final_mappings"].append({
                'csv_table_name': suggestion.get('csv_table_name'),
                'csv_table_description': suggestion.get('csv_table_description'),
                'csv_column_name': suggestion.get('csv_column_name'),
                'csv_column_description': suggestion.get('csv_column_description'),
                'cdm_parent_name': recommendation.get('recommended_parent') if recommendation else None,
                'cdm_parent_definition': '',
                'parent_candidates': suggestion.get('parent_candidates', []),
                'cdm_column_name': recommendation.get('recommended_column_name') if recommendation else None,
                'cdm_column_definition': recommendation.get('definition_suggestion', '') if recommendation else '',
                'other_candidates': suggestion.get('other_candidates', []),
                'comprehensive_reason': 'User Accepted Suggested Term',
                'final_decision': 'User Accepted Suggested Term',
                'app_query_text': suggestion.get('app_query_text'),
                'recommended_new_term': recommendation  # Include the full recommendation
            })
        else:
            session["unmapped_columns"].append({
                'csv_table_name': suggestion.get('csv_table_name'),
                'csv_table_description': suggestion.get('csv_table_description'),
                'csv_column_name': suggestion.get('csv_column_name'),
                'csv_column_description': suggestion.get('csv_column_description'),
                'other_candidates': suggestion.get('other_candidates', []),
                'parent_candidates': suggestion.get('parent_candidates', []),
                'comprehensive_reason': 'User Rejected Suggested Term',
                'error': 'User Rejected Suggested Term',
                'recommended_new_term': recommendation
            })

        session["pending_recommendation"] = None
        session["pending_index"] = None

        session["index"] = pending_index + 1
        if session["index"] >= len(suggestions):
            # 

            final_mappings = session.get("final_mappings", [])
            unmapped_columns = session.get("unmapped_columns", [])

            # Do NOT append auto_rejected to unmapped_columns here
            # Auto-rejected items are handled during the review process

            output_suffix = f"_interactive_{int(time.time())}"
            save_results_to_csv(final_mappings, unmapped_columns, output_suffix)

            mapped_filename = f"Final_CDM_Mappings{output_suffix}.csv"
            unmapped_filename = f"Unmapped_Columns{output_suffix}.csv"

            return JSONResponse(content={
                "done": True,
                "status": "success",
                "message": "Interactive review completed",
                "output_files": {
                    "mapped_file": mapped_filename,
                    "unmapped_file": unmapped_filename,
                }
            })
            # 
            # return JSONResponse(content={"done": True})
        

        return _review_next_response(session)

    suggestion = suggestions[idx]
    cdm_glossary_dict = session.get("cdm_glossary_dict", {})

    llm_candidates = suggestion.get('llm_candidates', [])
    chosen_term = None
    skip_recommendation = None

    if request.action == "accept_top":
        if llm_candidates:
            chosen_term = llm_candidates[0].get('term')
    elif request.action == "choose_candidate":
        if request.candidate_index is None:
            raise HTTPException(status_code=400, detail="candidate_index required")
        if 0 <= request.candidate_index < len(llm_candidates):
            chosen_term = llm_candidates[request.candidate_index].get('term')
    elif request.action == "reject":
        chosen_term = None
    elif request.action == "skip":
        chosen_term = None
        # CHANGED: generate recommendation on skip
        skip_recommendation = await asyncio.to_thread(
            _build_skip_recommendation,
            suggestion,
            cdm_glossary_dict
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid action")

    if request.action == "skip":
        session["pending_recommendation"] = skip_recommendation
        session["pending_index"] = idx
        return _pending_recommendation_response(session)

    if chosen_term:
        final_mapping = _build_final_mapping_from_choice(
            suggestion,
            chosen_term,
            cdm_glossary_dict,
            decision_label="User Accepted"
        )
        session["final_mappings"].append(final_mapping)
    else:
        session["unmapped_columns"].append({
            'csv_table_name': suggestion.get('csv_table_name'),
            'csv_table_description': suggestion.get('csv_table_description'),
            'csv_column_name': suggestion.get('csv_column_name'),
            'csv_column_description': suggestion.get('csv_column_description'),
            'other_candidates': suggestion.get('other_candidates', []),
            'parent_candidates': suggestion.get('parent_candidates', []),
            'comprehensive_reason': 'User Rejected',
            'error': 'User Rejected'
        })

    session["index"] = idx + 1

    return _review_next_response(session)

# ==========================================================================
# ENDPOINT 7: KPI Endpoints
# ==========================================================================

@app.get("/api/v1/kpis/session/{session_id}", response_model=PreReviewKPIsResponse)
async def get_pre_review_kpis(session_id: str):
    """
    Get pre-review KPI metrics for a session.
    Call this before human review starts to see proposer/challenger metrics.
    """
    session = INTERACTIVE_SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    try:
        kpis_data = _calculate_pre_review_kpis(session)
        return PreReviewKPIsResponse(**kpis_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating KPIs: {str(e)}")


@app.get("/api/v1/kpis/session/{session_id}/final", response_model=PostReviewKPIsResponse)
async def get_post_review_kpis(session_id: str):
    """
    Get post-review KPI metrics for a session.
    Call this after human review completes to see final results.
    """
    session = INTERACTIVE_SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check if review has started (index > 0 or final_mappings exist)
    if session.get("index", 0) == 0 and not session.get("final_mappings"):
        raise HTTPException(
            status_code=400, 
            detail="Review has not started yet. Use /api/v1/kpis/session/{session_id} for pre-review metrics."
        )
    
    try:
        kpis_data = _calculate_post_review_kpis(session)
        return PostReviewKPIsResponse(**kpis_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating KPIs: {str(e)}")

# ==========================================================================
# ENDPOINT 8: SSE Endpoint for progress stream
# ==========================================================================

@app.get("/api/v1/session/{session_id}/progress-stream")
async def progress_stream(session_id: str):
    """
    SSE (Server-Sent Events) endpoint for real-time progress streaming.
    Frontend can listen to this stream for automatic updates without polling.
    
    Usage (JavaScript):
        const eventSource = new EventSource(`/api/v1/session/${sessionId}/progress-stream`);
        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            // Update UI with data.status, data.progress, etc.
        };
    """
    session = INTERACTIVE_SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    async def event_generator():
        """
        Generator that yields SSE-formatted progress updates.
        """
        import json
        
        while True:
            session = INTERACTIVE_SESSIONS.get(session_id)
            if not session:
                # Session deleted or expired
                yield f"data: {{\"status\": \"error\", \"message\": \"Session not found\"}}\n\n"
                break
            
            status = session.get("status", "unknown")
            message = session.get("message", "")
            suggestions = session.get("suggestions", [])
            current_index = session.get("index", 0)
            
            # Build progress data
            if status in ["initializing", "processing", "error"]:
                # Workflow running or failed
                total_cols = session.get("total_columns_processed", 0)
                current_row = session.get("current_row_processed", 0)
                remaining = max(0, total_cols - current_row)
                
                # Calculate completion percentage
                if total_cols > 0:
                    completion_pct = (current_row / total_cols * 100)
                else:
                    # During initialization, show 0% but with proper context
                    completion_pct = 0.0
                
                progress_data = {
                    "session_id": session_id,
                    "status": status,
                    "workflow_phase": "workflow_processing",
                    "message": message,
                    "progress": {
                        "total_items": total_cols,
                        "completed_items": current_row,
                        "remaining_items": remaining,
                        "completion_percentage": round(completion_pct, 1)
                    },
                    "timing": {
                        "elapsed_seconds": round(time.time() - session.get("session_start_time", time.time()), 2)
                    }
                }
                
            elif status == "ready_for_review":
                # Workflow complete, waiting for review
                progress_data = {
                    "session_id": session_id,
                    "status": status,
                    "workflow_phase": "human_review",
                    "message": message,
                    "progress": {
                        "total_items": len(suggestions),
                        "completed_items": 0,
                        "remaining_items": len(suggestions),
                        "completion_percentage": 0.0
                    },
                    "timing": {
                        "elapsed_seconds": round(time.time() - session.get("session_start_time", time.time()), 2)
                    }
                }
                
            else:
                # Human review in progress or completed
                total_items = len(suggestions)
                completed_items = current_index
                remaining_items = total_items - completed_items
                completion_percentage = (completed_items / total_items * 100) if total_items > 0 else 0.0
                
                review_status = "completed" if completed_items >= total_items else "in_progress"
                
                progress_data = {
                    "session_id": session_id,
                    "status": review_status,
                    "workflow_phase": "completed" if review_status == "completed" else "human_review",
                    "message": "Review in progress" if review_status == "in_progress" else "Review completed",
                    "progress": {
                        "total_items": total_items,
                        "completed_items": completed_items,
                        "remaining_items": remaining_items,
                        "completion_percentage": round(completion_percentage, 2)
                    },
                    "timing": {
                        "elapsed_seconds": round(time.time() - session.get("session_start_time", time.time()), 2)
                    }
                }
            
            # Send SSE event
            yield f"data: {json.dumps(progress_data)}\n\n"
            
            # Stop streaming if completed or error
            if status in ["completed", "error"] or (status not in ["initializing", "processing", "ready_for_review"] and current_index >= len(suggestions)):
                break
            
            # Wait 1 second before next update
            await asyncio.sleep(1)
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

# ==========================================================================
# ENDPOINT 9: Download Outputs
# ==========================================================================

@app.get("/api/v1/download")
async def download_output(kind: str = "mapped", filename: Optional[str] = None):
    """Download latest or specified output file."""
    if kind not in {"mapped", "unmapped"}:
        raise HTTPException(status_code=400, detail="Invalid kind. Use 'mapped' or 'unmapped'.")

    directory = settings.MAPPED_OUTPUT_DIR if kind == "mapped" else settings.UNMAPPED_OUTPUT_DIR

    if filename:
        safe_name = Path(filename).name
        target = directory / safe_name
    else:
        files = list(directory.glob("*.csv"))
        if not files:
            raise HTTPException(status_code=404, detail="No files available for download")
        target = max(files, key=lambda p: p.stat().st_mtime)

    if not target.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(path=target, filename=target.name, media_type="text/csv")

# ============================================================================
# HEALTH
# ============================================================================

@app.get("/health")
async def health():
    """Health check endpoint with input directory info"""
    from utils.file_operations import list_input_files
    
    input_files = list_input_files()
    
    return {
        "status": "healthy",
        "embeddings": EMBEDDINGS_INSTANCE is not None,
        "llm": LLM_INSTANCE is not None,
        "mongodb_connected": MONGODB_CLIENT is not None,
        "collections": len(VECTOR_STORES),
        "inputs": input_files
    }



# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print("🚀 CDM Mapping API Gateway - MongoDB Atlas")
    print("="*60)
    print("🌐 Web UI: http://localhost:8000")
    print("📊 /api/v1/embeddings/initialize")
    print("🗄️  /api/v1/mongodb/create-collection")
    print("🔍 /api/v1/mongodb/search")
    print("🤖 /api/v1/llm/chat")
    print("💾 /api/v1/mongodb/save-mappings")
    print("🧭 /api/v1/run-mapping-interactive (POST with file upload)")
    print("🧩 /api/v1/review/combined")
    print("📈 /api/v1/kpis/session/{session_id} (GET pre-review KPIs)")
    print("📊 /api/v1/kpis/session/{session_id}/final (GET post-review KPIs)")
    print("🌊 /api/v1/session/{session_id}/progress-stream (SSE real-time stream)")
    print("⬇️  /api/v1/download (GET mapped/unmapped)")
    print("�💚 /health (includes inputs info)")
    print(f"{'='*60}\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)