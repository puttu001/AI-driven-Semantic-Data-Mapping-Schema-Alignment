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

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, SecretStr, Field
from typing import List, Dict, Optional, Any, Annotated, Literal
import uvicorn
from langchain_openai import OpenAIEmbeddings
from langchain_mongodb import MongoDBAtlasVectorSearch
from langchain_core.documents import Document
from pymongo import MongoClient
import os
import asyncio
import pandas as pd
import io
from pathlib import Path
import time
from datetime import datetime
from dotenv import load_dotenv

from core.database import create_vector_search_index
from config import settings
from config.settings import OBJECT_PARENT_COL, CDM_TABLE_DESC_COL, GLOSSARY_DEFINITION_COL,FASTAPI_SERVICE_URL
from utils.data_processing import (
    load_and_clean_csv_file,
    validate_required_columns,
    build_cdm_glossary_dict,
    build_cdm_terms_list,
    create_cdm_representation,
    create_csv_representation
)
from utils.file_operations import save_uploaded_file, save_results_to_csv, save_results_to_mongodb, generate_logical_model_excel
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

@app.get("/")
async def root():
    return {'status: Running'}

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
    new_terms_recommended: int
    executive_summary: str


INTERACTIVE_SESSIONS: Dict[str, Dict[str, Any]] = {}

# Demo files source in MongoDB (used when uploads are not provided)
DEMO_FILES_DB_NAME = "Data_mapping_demo_files"
DEMO_MAPPING_COLLECTION = "input_mapping_file"
DEMO_CDM_COLLECTION = "CDM_file"

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


def _generate_executive_summary(session: Dict[str, Any], kpis: Dict[str, Any]) -> str:
    """
    Generate executive summary using LLM.
    Summarizes the entire mapping session including CDM, mapping file, agents, and results.
    """
    global LLM_INSTANCE
    
    try:
        # Initialize LLM if not already done
        if not LLM_INSTANCE:
            from langchain_openai import ChatOpenAI
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return f"Mapping session completed: {kpis.get('total_mapped', 0)} columns mapped out of {session.get('total_columns_processed', 0)} total columns."
            
            LLM_INSTANCE = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.0,
                api_key=api_key,
                max_tokens=2000,
                timeout=300
            )
        
        # Gather context
        total_columns = session.get("total_columns_processed", 0)
        workflow_duration = session.get("workflow_duration_seconds", 0)
        suggestions = session.get("suggestions", [])
        auto_rejected = session.get("auto_rejected", [])
        final_mappings = session.get("final_mappings", [])
        unmapped = session.get("unmapped_columns", [])
        
        # Pre-review stats
        total_proposed = len(suggestions) + len(auto_rejected)
        challenger_accepted = len(suggestions)
        challenger_rejected = len(auto_rejected)
        
        # Post-review stats
        total_mapped = kpis.get("total_mapped", 0)
        avg_confidence = kpis.get("avg_confidence_score", 0)
        user_rejected = kpis.get("user_rejected", 0)
        new_terms = kpis.get("new_terms_recommended", 0)
        total_unmapped = kpis.get("unmapped", 0)
        
        # Avoid division by zero
        avg_time_per_column = workflow_duration / total_columns if total_columns > 0 else 0
        success_rate = total_mapped / total_columns * 100 if total_columns > 0 else 0
        agent_efficiency = challenger_accepted / total_proposed * 100 if total_proposed > 0 else 0
        challenger_reject_rate = challenger_rejected / total_proposed * 100 if total_proposed > 0 else 0
        human_override_rate = user_rejected / challenger_accepted * 100 if challenger_accepted > 0 else 0
        
        # Build context for LLM
        system_prompt = """You are an executive AI assistant specializing in data mapping and governance.
Generate a concise, professional executive summary of a CDM (Common Data Model) mapping session.
Focus on key metrics, success rates, and actionable insights.
Keep it brief (2-3 paragraphs) and business-focused.

IMPORTANT: 
- Use ONLY plain text without any markdown formatting
- Do NOT use asterisks, hashtags, or any special formatting characters
- Do NOT include a title or header like "Executive Summary"
- Start directly with the content
- Use proper punctuation and paragraph breaks only"""
        
        user_prompt = f"""Generate an executive summary for this CDM mapping session:

**SESSION OVERVIEW:**
- Total Source Columns Processed: {total_columns}
- Processing Duration: {workflow_duration:.1f} seconds
- Average Time per Column: {avg_time_per_column:.2f}s

**AUTOMATED AGENT PERFORMANCE:**
- Proposer Agent Generated: {total_proposed} mapping suggestions
- Challenger Agent Approved: {challenger_accepted} mappings ({agent_efficiency:.1f}%)
- Challenger Agent Rejected: {challenger_rejected} mappings ({challenger_reject_rate:.1f}%)

**HUMAN REVIEW RESULTS:**
- Final Mapped Columns: {total_mapped}
- Average Confidence Score: {avg_confidence:.2f}
- User Rejected: {user_rejected}
- Total Unmapped: {total_unmapped}
- New Terms Recommended: {new_terms}

**MAPPING QUALITY:**
- Success Rate: {success_rate:.1f}% of source columns successfully mapped
- Agent Efficiency: {agent_efficiency:.1f}% of AI suggestions passed quality checks
- Human Override Rate: {human_override_rate:.1f}% of approved suggestions were rejected by users

Generate a professional executive summary highlighting the key outcomes, quality indicators, and any notable patterns.
Use clear, concise language suitable for stakeholders. Keep it to 2-3 paragraphs maximum.

IMPORTANT: Write in plain text only. Do not use markdown formatting, asterisks, hashtags, or bold/italic text. 
Do not include any title or header. Start directly with the summary content."""
        
        # Call LLM directly
        from langchain_core.messages import SystemMessage, HumanMessage
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        response = LLM_INSTANCE.invoke(messages)
        summary = response.content.strip()
        
        # Clean up any markdown formatting that might have slipped through
        import re
        summary = re.sub(r'\*\*([^*]+)\*\*', r'\1', summary)  # Remove bold **text**
        summary = re.sub(r'\*([^*]+)\*', r'\1', summary)      # Remove italic *text*
        summary = re.sub(r'#+\s*', '', summary)               # Remove headers ###
        summary = re.sub(r'^\s*Executive Summary[:\s]*', '', summary, flags=re.IGNORECASE)  # Remove "Executive Summary:" header
        summary = summary.strip()
        
        if summary:
            return summary
        else:
            return f"Mapping session completed: {total_mapped} of {total_columns} columns successfully mapped ({success_rate:.1f}% success rate)."
            
    except Exception as e:
        print(f"⚠️ Failed to generate executive summary: {e}")
        import traceback
        traceback.print_exc()
        return f"Mapping session completed: {kpis.get('total_mapped', 0)} columns mapped out of {session.get('total_columns_processed', 0)} total columns."


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
    
    # Count new terms recommended (when skip was hit)
    new_terms_recommended = 0
    for mapping in final_mappings:
        if mapping.get("recommended_new_term") is not None:
            new_terms_recommended += 1
    for unmapped in unmapped_columns:
        if unmapped.get("recommended_new_term") is not None:
            new_terms_recommended += 1
    
    kpis_data = {
        "session_id": session.get("session_id", "unknown"),
        "total_mapped": len(final_mappings),
        "avg_confidence_score": round(avg_confidence_score, 2),
        "unmapped": len(unmapped_columns),
        "user_rejected": user_rejected_count,
        "new_terms_recommended": new_terms_recommended
    }
    
    # Generate executive summary
    print("📝 Generating executive summary...")
    executive_summary = _generate_executive_summary(session, kpis_data)
    kpis_data["executive_summary"] = executive_summary
    
    return kpis_data


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
        
        # Generate logical model Excel file
        mapped_csv_path = settings.MAPPED_OUTPUT_DIR / mapped_filename
        excel_path = generate_logical_model_excel(mapped_csv_path, output_suffix)
        excel_filename = excel_path.name if excel_path else None

        return JSONResponse(content={
            "done": True,
            "status": "success",
            "message": "Interactive review completed",
            "output_files": {
                "mapped_file": mapped_filename,
                "unmapped_file": unmapped_filename,
                "excel_file": excel_filename
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

def _load_cdm_from_mongodb(mongo_uri: str) -> pd.DataFrame:
    """Load CDM data from MongoDB."""
    print("📥 Loading CDM data from MongoDB...")
    client = MongoClient(mongo_uri)
    db = client["cdm_mapping"]
    collection = db["cdm_master_data"]
    
    # Get all CDM records
    records = list(collection.find({}, {"_id": 0}))
    client.close()
    
    if not records:
        raise HTTPException(status_code=404, detail="No CDM data found in MongoDB. Run cdm_setup.py first.")
    
    df = pd.DataFrame(records)
    print(f"✅ Loaded {len(df)} CDM records from MongoDB")
    return df


def _load_demo_file_from_mongodb(mongo_uri: str, collection_name: str, label: str) -> pd.DataFrame:
    """Load demo CDM/mapping input from dedicated MongoDB demo collections."""
    print(f"📥 Loading demo {label} from MongoDB collection '{collection_name}'...")

    client = MongoClient(mongo_uri)
    db = client[DEMO_FILES_DB_NAME]
    collection = db[collection_name]
    records = list(collection.find({}, {"_id": 0}))
    client.close()

    if not records:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No demo {label} data found in MongoDB "
                f"{DEMO_FILES_DB_NAME}.{collection_name}."
            )
        )

    first_record = records[0]

    # Format A: one doc with CSV text/base64 payload
    csv_text_key = next(
        (k for k in ("csv_text", "csv_content", "content") if isinstance(first_record.get(k), str)),
        None
    )
    if csv_text_key:
        try:
            return pd.read_csv(io.StringIO(first_record[csv_text_key]))
        except Exception:
            pass

    # Format B: one doc with embedded rows list
    rows = first_record.get("rows")
    if isinstance(rows, list) and rows:
        df = pd.DataFrame(rows)
        print(f"✅ Loaded {len(df)} demo {label} rows from embedded list")
        return df

    # Format C: each Mongo document is one CSV row
    df = pd.DataFrame(records)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Demo {label} data is empty")

    print(f"✅ Loaded {len(df)} demo {label} rows from MongoDB")
    return df


def _run_mapping_interactive_sync(
    api_key: Optional[str],
    mongodb_uri: Optional[str],
    cdm_content: Optional[bytes],
    cdm_filename: Optional[str],
    mapping_content: Optional[bytes],
    mapping_filename: Optional[str]
):
    global EMBEDDINGS_INSTANCE

    key = api_key or os.getenv("OPENAI_API_KEY")
    mongo_uri = _normalize_mongodb_uri(mongodb_uri) or os.getenv("MONGODB_URI")

    if not key:
        raise HTTPException(status_code=400, detail="OpenAI API key required (provide in form or .env)")
    if not mongo_uri:
        raise HTTPException(status_code=400, detail="MongoDB URI required (provide in form or .env)")

    if not EMBEDDINGS_INSTANCE:
        EMBEDDINGS_INSTANCE = OpenAIEmbeddings(
            model="text-embedding-3-large",
            api_key=SecretStr(key)
        )

    # Load CDM data - from file if provided, otherwise from MongoDB
    if cdm_content and cdm_filename:
        print("📁 Using uploaded CDM file")
        cdm_path = save_uploaded_file(cdm_content, cdm_filename, file_type='cdm')
        cdm_df = load_and_clean_csv_file(str(cdm_path))
    else:
        print("💾 No CDM upload provided. Using demo CDM file from MongoDB")
        cdm_df = _load_demo_file_from_mongodb(mongo_uri, DEMO_CDM_COLLECTION, "CDM")

    if mapping_content and mapping_filename:
        print("📁 Using uploaded mapping file")
        mapping_path = save_uploaded_file(mapping_content, mapping_filename, file_type='mapping')
        mapping_df = load_and_clean_csv_file(str(mapping_path))
    else:
        print("💾 No mapping upload provided. Using demo mapping file from MongoDB")
        mapping_df = _load_demo_file_from_mongodb(mongo_uri, DEMO_MAPPING_COLLECTION, "mapping")

    if not validate_required_columns(cdm_df, [], "CDM"):
        raise HTTPException(status_code=400, detail="CDM file missing required columns")

    csv_required_columns = [settings.CSV_TABLE_NAME_COL, settings.CSV_COLUMN_NAME_COL]
    if not validate_required_columns(mapping_df, csv_required_columns, "CSV"):
        raise HTTPException(status_code=400, detail="Mapping file missing required columns")

    cdm_glossary_dict = build_cdm_glossary_dict(cdm_df)
    cdm_terms_list = build_cdm_terms_list(cdm_df)

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
        raise HTTPException(status_code=400, detail="No valid CDM documents found")
    if not csv_documents:
        raise HTTPException(status_code=400, detail="No valid mapping documents found")

    cdm_request = CreateCollectionRequest(
        documents=cdm_documents,
        collection_name=settings.CDM_COLLECTION_NAME,
        mongodb_uri=mongo_uri,
        db_name=settings.MONGODB_DB_NAME,
        index_name=settings.VECTOR_INDEX_NAME,
        drop_old=True
    )
    cdm_response = asyncio.run(create_mongodb_collection(cdm_request))

    csv_request = CreateCollectionRequest(
        documents=csv_documents,
        collection_name=settings.CSV_COLLECTION_NAME,
        mongodb_uri=mongo_uri,
        db_name=settings.MONGODB_DB_NAME,
        index_name=settings.VECTOR_INDEX_NAME,
        drop_old=True
    )
    csv_response = asyncio.run(create_mongodb_collection(csv_request))

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
    print("\n⏳ Waiting for vector indexes and API endpoints initialization...")
    print("   Allowing 20 seconds for endpoints to warm up and indexes to stabilize...")
    time.sleep(20)
    print("✅ Systems ready - starting workflow")

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

    # Track processing time
    workflow_start_time = time.time()
    result = workflow.run_batch_process(csv_data_rows)
    workflow_end_time = time.time()
    workflow_duration = workflow_end_time - workflow_start_time

    suggestions = result.get('suggestions', [])
    unmapped = result.get('unmapped', [])
    auto_rejected = result.get('auto_rejected', [])



    # ============================================================
    # NEW: Optional Validation Analysis
    # ============================================================
    validation_report = None
    validation_file = Path("Inputs/Validation/starbucks_validate.csv")
    
    if validation_file.exists():
        try:
            print("\n🔍 Running validation analysis against ground truth...")
            validation_report = run_validation_analysis(
                suggestions=suggestions,
                validation_file_path=str(validation_file)
            )
            print_validation_summary(validation_report)
        except Exception as e:
            print(f"⚠️ Validation failed: {e}")
            import traceback
            traceback.print_exc()
            validation_report = {"error": str(e)}
    else:
        print(f"ℹ️ No validation file found at {validation_file}")
    # ===========================================================



    session_id = f"session_{int(time.time())}"
    INTERACTIVE_SESSIONS[session_id] = {
        "session_id": session_id,  # Store session_id in the session itself
        "suggestions": suggestions,
        "index": 0,
        "final_mappings": [],
        "unmapped_columns": list(unmapped),
        "auto_rejected": auto_rejected,
        "cdm_glossary_dict": cdm_glossary_dict,
        "validation_report": validation_report,  # NEW: Store validation results
        # Timing metadata
        "session_start_time": time.time(),
        "workflow_duration_seconds": workflow_duration,
        "total_columns_processed": len(csv_data_rows)
    }

    #changes here below###

    # return JSONResponse(content={
    #     "status": "success",
    #     "message": "Suggestions generated. Start review.",
    #     "session_id": session_id,
    #     "total_suggestions": len(suggestions),
    #     "auto_rejected_count": len(auto_rejected),
    #     "unmapped_count": len(unmapped)
    # })


    # Build response with validation metrics
    response_data = {
        "status": "success",
        "message": "Suggestions generated. Start review.",
        "session_id": session_id,
        "total_suggestions": len(suggestions),
        "auto_rejected_count": len(auto_rejected),
        "unmapped_count": len(unmapped)
    }
    
    # Add validation summary to response if available
    if validation_report and "error" not in validation_report:
        response_data["validation"] = {
            "combined_accuracy": validation_report['combined_accuracy'],
            "level1_accuracy": validation_report['level1_accuracy'],
            "level2_accuracy": validation_report['level2_accuracy'],
            "validated_rows": validation_report['validated_rows'],
            "total_rows": validation_report['total_rows'],
            "rank_distribution": validation_report['rank_distribution']
        }
    elif validation_report and "error" in validation_report:
        response_data["validation"] = {"error": validation_report["error"]}
    
    return JSONResponse(content=response_data)


# ==========================================================================
# ENDPOINT 6A: Run Mapping (Interactive Review)
# ==========================================================================

@app.post("/api/v1/run-mapping-interactive")
async def run_mapping_interactive(
    api_key: str = Form(None),
    mongodb_uri: str = Form(None),
    cdm_file: UploadFile = File(None),
    mapping_file: UploadFile = File(None)
):
    """
    Process uploaded files and return a review session for user decisions.
    Includes challenger agent via LLM evaluation.
    """
    try:
        cdm_content = await cdm_file.read() if cdm_file else None
        cdm_filename = cdm_file.filename if cdm_file else None
        mapping_content = await mapping_file.read() if mapping_file else None
        mapping_filename = mapping_file.filename if mapping_file else None
        return await asyncio.to_thread(
            _run_mapping_interactive_sync,
            api_key,
            mongodb_uri,
            cdm_content,
            cdm_filename,
            mapping_content,
            mapping_filename
        )
    except HTTPException:
        raise
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
        
        # Generate logical model Excel file
        mapped_csv_path = settings.MAPPED_OUTPUT_DIR / mapped_filename
        excel_path = generate_logical_model_excel(mapped_csv_path, output_suffix)
        excel_filename = excel_path.name if excel_path else None

        return JSONResponse(content={
            "done": True,
            "status": "success",
            "message": "Interactive review completed",
            "output_files": {
                "mapped_file": mapped_filename,
                "unmapped_file": unmapped_filename,
                "excel_file": excel_filename
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
            
            # Generate logical model Excel file
            mapped_csv_path = settings.MAPPED_OUTPUT_DIR / mapped_filename
            excel_path = generate_logical_model_excel(mapped_csv_path, output_suffix)
            excel_filename = excel_path.name if excel_path else None

            return JSONResponse(content={
                "done": True,
                "status": "success",
                "message": "Interactive review completed",
                "output_files": {
                    "mapped_file": mapped_filename,
                    "unmapped_file": unmapped_filename,
                    "excel_file": excel_filename
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
# ENDPOINT 8: Download Outputs
# ==========================================================================

@app.get("/api/v1/download")
async def download_output(kind: str = "mapped", filename: Optional[str] = None):
    """Download latest or specified output file."""
    if kind not in {"mapped", "unmapped", "excel"}:
        raise HTTPException(status_code=400, detail="Invalid kind. Use 'mapped', 'unmapped', or 'excel'.")

    # Determine directory and file extension based on kind
    if kind == "excel":
        directory = settings.MAPPED_OUTPUT_DIR
        file_extension = "*.xlsx"
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        directory = settings.MAPPED_OUTPUT_DIR if kind == "mapped" else settings.UNMAPPED_OUTPUT_DIR
        file_extension = "*.csv"
        media_type = "text/csv"

    if filename:
        safe_name = Path(filename).name
        target = directory / safe_name
    else:
        files = list(directory.glob(file_extension))
        if not files:
            raise HTTPException(status_code=404, detail="No files available for download")
        target = max(files, key=lambda p: p.stat().st_mtime)

    if not target.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(path=target, filename=target.name, media_type=media_type)


@app.get("/api/v1/demo-files/download")
async def download_demo_file(kind: str = "cdm"):
    """Download demo CDM or mapping file sourced from MongoDB demo collections."""
    kind_normalized = kind.lower().strip()
    if kind_normalized not in {"cdm", "mapping"}:
        raise HTTPException(status_code=400, detail="Invalid kind. Use 'cdm' or 'mapping'.")

    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        raise HTTPException(status_code=400, detail="MONGODB_URI missing from environment")

    collection_name = DEMO_CDM_COLLECTION if kind_normalized == "cdm" else DEMO_MAPPING_COLLECTION
    df = _load_demo_file_from_mongodb(
        mongo_uri,
        collection_name,
        "CDM" if kind_normalized == "cdm" else "mapping"
    )

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    output_name = f"demo_{kind_normalized}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{output_name}"'}
    return StreamingResponse(io.BytesIO(csv_bytes), media_type="text/csv", headers=headers)
# ============================================================================
# HEALTH
# ============================================================================

@app.get("/health")
async def health():
    """Health check endpoint with input directory info"""
    from utils.file_operations import list_input_files
    
    input_files = list_input_files()
    
    # Check CDM data in MongoDB
    cdm_status = {"available": False, "count": 0}
    try:
        mongo_uri = os.getenv("MONGODB_URI")
        if mongo_uri:
            client = MongoClient(mongo_uri)
            db = client["cdm_mapping"]
            collection = db["cdm_master_data"]
            count = collection.count_documents({})
            cdm_status = {"available": count > 0, "count": count}
            client.close()
    except Exception:
        pass
    
    return {
        "status": "healthy",
        "embeddings": EMBEDDINGS_INSTANCE is not None,
        "llm": LLM_INSTANCE is not None,
        "mongodb_connected": MONGODB_CLIENT is not None,
        "collections": len(VECTOR_STORES),
        "inputs": input_files,
        "cdm_in_mongodb": cdm_status
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
    print("⬇️  /api/v1/download (GET mapped/unmapped)")
    print("�💚 /health (includes inputs info)")
    print(f"{'='*60}\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)