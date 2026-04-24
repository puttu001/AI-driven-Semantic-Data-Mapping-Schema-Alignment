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
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
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
from utils.file_operations import save_uploaded_file, save_results_to_csv
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
    """Minimal UI for running mapping and downloading results"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>CDM Mapping </title>
        <style>
            body { font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; }
            h1 { color: #222; }
            form { background: #f7f7f7; padding: 20px; border-radius: 8px; }
            label { display: block; margin-top: 12px; font-weight: bold; }
            input[type="text"], input[type="file"] { width: 100%; padding: 8px; margin-top: 5px; border: 1px solid #ddd; border-radius: 4px; }
            button { margin-top: 16px; padding: 10px 18px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; }
            button:disabled { background: #6c757d; cursor: not-allowed; }
            .status { margin-top: 16px; padding: 12px; background: #eef5ff; border-radius: 6px; }
            .downloads { margin-top: 16px; }
            .error { background: #ffe6e6; }
        </style>
    </head>
    <body>
        <h1>CDM Mapping </h1>
        <form id="mappingForm" enctype="multipart/form-data">
            <label>OpenAI API Key (optional, uses .env if not provided):</label>
            <input type="text" name="api_key" placeholder="sk-...">

            <label>MongoDB URI (optional, uses .env if not provided):</label>
            <input type="text" name="mongodb_uri" placeholder="mongodb+srv://...">

            <label>CDM CSV File: *</label>
            <input type="file" name="cdm_file" accept=".csv" required>

            <label>Mapping CSV File: *</label>
            <input type="file" name="mapping_file" accept=".csv" required>

            <label style="margin-top:16px; font-weight: normal;">
                <input type="checkbox" id="interactiveMode" checked>
                Enable interactive review (includes challenger agent)
            </label>

            <button type="submit" id="runBtn">Run Mapping</button>
        </form>

        <div id="status" class="status" style="display:none;"></div>
        <div id="review" class="status" style="display:none;"></div>
        <div id="downloads" class="downloads" style="display:none;"></div>

        <script>
            const form = document.getElementById('mappingForm');
            const statusEl = document.getElementById('status');
            const reviewEl = document.getElementById('review');
            const downloadsEl = document.getElementById('downloads');
            const runBtn = document.getElementById('runBtn');
            const interactiveMode = document.getElementById('interactiveMode');

            let currentSessionId = null;

            function renderSuggestion(suggestion, index, total) {
                if (!suggestion) {
                    reviewEl.innerHTML = '<strong>Review complete.</strong>'; 
                    return;
                }

                const candidates = suggestion.llm_candidates || [];
                let html = `<div><strong>Review ${index + 1} of ${total}</strong></div>`;
                html += `<div style="margin-top:8px;"><strong>Table:</strong> ${suggestion.csv_table_name || ''}</div>`;
                html += `<div><strong>Column:</strong> ${suggestion.csv_column_name || ''}</div>`;
                html += `<div><strong>Description:</strong> ${suggestion.csv_column_description || ''}</div>`;

                if (candidates.length) {
                    html += '<div style="margin-top:10px;"><strong>Candidates:</strong></div>';
                    html += '<ul>';
                    candidates.forEach((c, i) => {
                        const tableName = c.table_name ? ` — ${c.table_name}` : '';
                        html += `<li><label><input type="radio" name="candidate" value="${i}"> ${c.term}${tableName} (Score: ${c.score})</label></li>`;
                    });
                    html += '</ul>';
                } else {
                    html += '<div><em>No candidates available.</em></div>';
                }

                html += `
                    <div style="margin-top:10px;">
                        <button type="button" id="acceptTopBtn">Accept Top</button>
                        <button type="button" id="chooseBtn">Choose Selected</button>
                        <button type="button" id="rejectBtn">Reject</button>
                        <button type="button" id="skipBtn">Skip</button>
                    </div>
                `;

                reviewEl.innerHTML = html;

                document.getElementById('acceptTopBtn').onclick = () => submitDecision('accept_top');
                document.getElementById('chooseBtn').onclick = () => {
                    const selected = document.querySelector('input[name="candidate"]:checked');
                    if (!selected) {
                        alert('Select a candidate first.');
                        return;
                    }
                    submitDecision('choose_candidate', parseInt(selected.value));
                };
                document.getElementById('rejectBtn').onclick = () => submitDecision('reject');
                document.getElementById('skipBtn').onclick = () => submitDecision('skip');
            }

            // CHANGED: render recommendation decision UI
            function renderRecommendation(suggestion, index, total, rec) {
                let html = `<div><strong>Review ${index + 1} of ${total}</strong></div>`;
                html += `<div style="margin-top:8px;"><strong>Table:</strong> ${suggestion.csv_table_name || ''}</div>`;
                html += `<div><strong>Column:</strong> ${suggestion.csv_column_name || ''}</div>`;
                html += `<div><strong>Description:</strong> ${suggestion.csv_column_description || ''}</div>`;

                html += '<div style="margin-top:10px;"><strong>Suggested New Term:</strong></div>';
                html += `<div><strong>${rec.recommended_column_name || 'N/A'}</strong> (Parent: ${rec.recommended_parent || 'N/A'}, Confidence: ${rec.confidence_score || 0})</div>`;
                if (rec.reasoning) {
                    html += `<div style="margin-top:6px;"><em>${rec.reasoning}</em></div>`;
                }

                html += `
                    <div style="margin-top:10px;">
                        <button type="button" id="acceptSuggestedBtn">Accept Suggested</button>
                        <button type="button" id="rejectSuggestedBtn">Reject Suggested</button>
                    </div>
                `;

                reviewEl.innerHTML = html;

                document.getElementById('acceptSuggestedBtn').onclick = () => submitDecision('accept_suggested');
                document.getElementById('rejectSuggestedBtn').onclick = () => submitDecision('reject_suggested');
            }

            async function fetchNextSuggestion() {
                const res = await fetch('/api/v1/review/combined-finalize', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ session_id: currentSessionId })
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.detail || 'Failed to fetch next suggestion');

                if (data.done) {
                    reviewEl.innerHTML = '<strong>Review complete. Finalizing...</strong>';
                    await finalizeSession();
                    return;
                }
                if (data.awaiting_recommendation && data.recommendation) {
                    renderRecommendation(data.suggestion, data.index, data.total, data.recommendation);
                } else {
                    renderSuggestion(data.suggestion, data.index, data.total);
                }
            }

            async function submitDecision(action, candidateIndex = null) {
                const payload = { session_id: currentSessionId, action, candidate_index: candidateIndex };
                const res = await fetch('/api/v1/review/combined-finalize', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.detail || 'Failed to submit decision');

                if (data.done) {
                    reviewEl.innerHTML = '<strong>Review complete. Finalizing...</strong>';
                    await finalizeSession();
                } else {
                    if (data.awaiting_recommendation && data.recommendation) {
                        renderRecommendation(data.suggestion, data.index, data.total, data.recommendation);
                    } else {
                        renderSuggestion(data.suggestion, data.index, data.total);
                    }
                }
            }

            async function finalizeSession() {
                const res = await fetch('/api/v1/review/combined-finalize', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ session_id: currentSessionId, finalize: true })
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.detail || 'Finalize failed');

                statusEl.textContent = data.message || 'Completed.';
                if (data.output_files) {
                    const mapped = data.output_files.mapped_file;
                    const unmapped = data.output_files.unmapped_file;
                    const mappedLink = mapped ? `/api/v1/download?kind=mapped&filename=${encodeURIComponent(mapped)}` : null;
                    const unmappedLink = unmapped ? `/api/v1/download?kind=unmapped&filename=${encodeURIComponent(unmapped)}` : null;

                    let html = '<strong>Downloads:</strong><ul>';
                    if (mappedLink) html += `<li><a href="${mappedLink}">Download Mapped CSV</a></li>`;
                    if (unmappedLink) html += `<li><a href="${unmappedLink}">Download Unmapped CSV</a></li>`;
                    html += '</ul>';

                    downloadsEl.innerHTML = html;
                    downloadsEl.style.display = 'block';
                }
            }

            form.addEventListener('submit', async (e) => {
                e.preventDefault();
                statusEl.style.display = 'block';
                statusEl.classList.remove('error');
                statusEl.textContent = 'Processing... This may take several minutes.';
                downloadsEl.style.display = 'none';
                runBtn.disabled = true;

                const formData = new FormData(form);
                try {
                    const endpoint = interactiveMode.checked ? '/api/v1/run-mapping-interactive' : '/api/v1/run-mapping';
                    const response = await fetch(endpoint, { method: 'POST', body: formData });
                    const data = await response.json();

                    if (!response.ok) {
                        throw new Error(data.detail || data.message || 'Request failed');
                    }

                    statusEl.textContent = data.message || 'Completed.';

                    if (interactiveMode.checked) {
                        currentSessionId = data.session_id;
                        reviewEl.style.display = 'block';
                        await fetchNextSuggestion();
                    } else if (data.output_files) {
                        const mapped = data.output_files.mapped_file;
                        const unmapped = data.output_files.unmapped_file;
                        const mappedLink = mapped ? `/api/v1/download?kind=mapped&filename=${encodeURIComponent(mapped)}` : null;
                        const unmappedLink = unmapped ? `/api/v1/download?kind=unmapped&filename=${encodeURIComponent(unmapped)}` : null;

                        let html = '<strong>Downloads:</strong><ul>';
                        if (mappedLink) html += `<li><a href="${mappedLink}">Download Mapped CSV</a></li>`;
                        if (unmappedLink) html += `<li><a href="${unmappedLink}">Download Unmapped CSV</a></li>`;
                        html += '</ul>';

                        downloadsEl.innerHTML = html;
                        downloadsEl.style.display = 'block';
                    }
                } catch (err) {
                    statusEl.classList.add('error');
                    statusEl.textContent = `Error: ${err.message}`;
                } finally {
                    runBtn.disabled = false;
                }
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

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
    
# class ReviewDecisionRequest(BaseModel):
#     session_id: str
#     action: Literal['accept_top','reject','choose_candidate','skip','accept_suggested','reject_suggested']
#     candidate_index: Optional[int] = None

#     class Config:
#         schema_extra = {
#             "examples": {
#                 "skip": {
#                     "summary": "Skip (request recommendation)",
#                     "value": {
#                         "session_id": "session_1769753209",
#                         "action": "skip"
#                     }
#                 },
#                 "accept_suggested": {
#                     "summary": "Accept suggested term",
#                     "value": {
#                         "session_id": "session_1769753209",
#                         "action": "accept_suggested"
#                     }
#                 },
#                 "reject_suggested": {
#                     "summary": "Reject suggested term",
#                     "value": {
#                         "session_id": "session_1769753209",
#                         "action": "reject_suggested"
#                     }
#                 }
#             }
#         }

class ReviewCombinedRequest(BaseModel):
    session_id: str
    action: Optional[Literal['accept_top','reject','choose_candidate','skip','accept_suggested','reject_suggested']] = None
    candidate_index: Optional[int] = None

class ReviewCombinedFinalizeRequest(BaseModel):
    session_id: str
    action: Optional[Literal['accept_top','reject','choose_candidate','skip','accept_suggested','reject_suggested']] = None
    candidate_index: Optional[int] = None
    finalize: Optional[bool] = False

    class Config:
        schema_extra = {
            "examples": {
                "skip": {
                    "summary": "Skip (request recommendation)",
                    "value": {
                        "session_id": "session_1769753209",
                        "action": "skip",
                        "finalize": False
                    }
                },
                "accept_suggested": {
                    "summary": "Accept suggested term",
                    "value": {
                        "session_id": "session_1769753209",
                        "action": "accept_suggested",
                        "finalize": False
                    }
                },
                "reject_suggested": {
                    "summary": "Reject suggested term",
                    "value": {
                        "session_id": "session_1769753209",
                        "action": "reject_suggested",
                        "finalize": False
                    }
                },
                "finalize": {
                    "summary": "Finalize review",
                    "value": {
                        "session_id": "session_1769753209",
                        "finalize": True
                    }
                }
            }
        }
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
        
        # Store reference
        store_key = f"{request.db_name}.{request.collection_name}"
        VECTOR_STORES[store_key] = {
            "vector_store": vector_store,
            "collection": collection,
            "index_name": request.index_name
        }
        
        num_docs = collection.count_documents({})
        
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
        
        # Perform search (embeds query via OpenAI API)
        if request.return_scores:
            results = vector_store.similarity_search_with_score(
                request.query_text,
                k=request.top_k
            )
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
###########change11

def _review_next_response(session: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> JSONResponse:
    idx = session.get("index", 0)
    suggestions = session.get("suggestions", [])

    if idx >= len(suggestions):
        return JSONResponse(content={"done": True})
##change22
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


def _run_mapping_batch_sync(
    api_key: Optional[str],
    mongodb_uri: Optional[str],
    cdm_content: bytes,
    cdm_filename: str,
    mapping_content: bytes,
    mapping_filename: str
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

    cdm_path = save_uploaded_file(cdm_content, cdm_filename, file_type='cdm')
    mapping_path = save_uploaded_file(mapping_content, mapping_filename, file_type='mapping')

    cdm_df = load_and_clean_csv_file(str(cdm_path))
    mapping_df = load_and_clean_csv_file(str(mapping_path))

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

    result = workflow.run_batch_process(csv_data_rows)

    def _to_final_mapping(suggestion: Dict[str, Any]) -> Dict[str, Any]:
        top_parent = (suggestion.get('parent_candidates') or [None])[0]
        top_column = (suggestion.get('other_candidates') or [None])[0]

        return {
            'csv_table_name': suggestion.get('csv_table_name'),
            'csv_table_description': suggestion.get('csv_table_description'),
            'csv_column_name': suggestion.get('csv_column_name'),
            'csv_column_description': suggestion.get('csv_column_description'),
            'cdm_parent_name': top_parent.get('parent_name') if isinstance(top_parent, dict) else None,
            'cdm_parent_definition': top_parent.get('parent_definition', '') if isinstance(top_parent, dict) else '',
            'parent_candidates': suggestion.get('parent_candidates', []),
            'cdm_column_name': top_column.get('term') if isinstance(top_column, dict) else None,
            'cdm_column_definition': '',
            'other_candidates': suggestion.get('other_candidates', []),
            'comprehensive_reason': 'Batch mode: auto-selected top candidates',
            'final_decision': 'Auto-Accepted (Batch)',
            'app_query_text': suggestion.get('app_query_text')
        }

    suggestions = result.get('suggestions', [])
    auto_rejected = result.get('auto_rejected', [])
    unmapped = result.get('unmapped', [])

    final_mappings = [_to_final_mapping(s) for s in suggestions]

    unmapped_columns = list(unmapped)
    for ar in auto_rejected:
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

    output_suffix = f"_web_{int(time.time())}"
    save_results_to_csv(final_mappings, unmapped_columns, output_suffix)

    mapped_filename = f"Final_CDM_Mappings{output_suffix}.csv"
    unmapped_filename = f"Unmapped_Columns{output_suffix}.csv"

    return JSONResponse(content={
        "status": "success",
        "message": "Batch processing completed",
        "processing_stats": {
            "suggestions_count": len(suggestions),
            "unmapped_count": len(unmapped_columns),
            "auto_rejected_count": len(auto_rejected)
        },
        "output_files": {
            "mapped_file": mapped_filename,
            "unmapped_file": unmapped_filename
        }
    })


def _run_mapping_interactive_sync(
    api_key: Optional[str],
    mongodb_uri: Optional[str],
    cdm_content: bytes,
    cdm_filename: str,
    mapping_content: bytes,
    mapping_filename: str
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

    cdm_path = save_uploaded_file(cdm_content, cdm_filename, file_type='cdm')
    mapping_path = save_uploaded_file(mapping_content, mapping_filename, file_type='mapping')

    cdm_df = load_and_clean_csv_file(str(cdm_path))
    mapping_df = load_and_clean_csv_file(str(mapping_path))

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

    result = workflow.run_batch_process(csv_data_rows)

    suggestions = result.get('suggestions', [])
    unmapped = result.get('unmapped', [])
    auto_rejected = result.get('auto_rejected', [])

    session_id = f"session_{int(time.time())}"
    INTERACTIVE_SESSIONS[session_id] = {
        "suggestions": suggestions,
        "index": 0,
        "final_mappings": [],
        "unmapped_columns": list(unmapped),
        "auto_rejected": auto_rejected,
        "cdm_glossary_dict": cdm_glossary_dict
    }

    return JSONResponse(content={
        "status": "success",
        "message": "Suggestions generated. Start review.",
        "session_id": session_id,
        "total_suggestions": len(suggestions),
        "auto_rejected_count": len(auto_rejected),
        "unmapped_count": len(unmapped)
    })


# ==========================================================================
# ENDPOINT 6: Run Mapping (Batch)
# ==========================================================================

# @app.post("/api/v1/run-mapping")
# async def run_mapping_batch(
#     api_key: str = Form(None),
#     mongodb_uri: str = Form(None),
#     cdm_file: UploadFile = File(...),
#     mapping_file: UploadFile = File(...)
# ):
#     """
#     Process uploaded CDM and mapping CSV files in batch mode.
#     Generates mapped + unmapped CSV outputs for download.
#     """
#     try:
#         cdm_content = await cdm_file.read()
#         mapping_content = await mapping_file.read()
#         return await asyncio.to_thread(
#             _run_mapping_batch_sync,
#             api_key,
#             mongodb_uri,
#             cdm_content,
#             cdm_file.filename,
#             mapping_content,
#             mapping_file.filename
#         )
#     except HTTPException:
#         raise
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")


# ==========================================================================
# ENDPOINT 6B: Run Mapping (Interactive Review)
# ==========================================================================

@app.post("/api/v1/run-mapping-interactive")
async def run_mapping_interactive(
    api_key: str = Form(None),
    mongodb_uri: str = Form(None),
    cdm_file: UploadFile = File(...),
    mapping_file: UploadFile = File(...)
):
    """
    Process uploaded files and return a review session for user decisions.
    Includes challenger agent via LLM evaluation.
    """
    try:
        cdm_content = await cdm_file.read()
        mapping_content = await mapping_file.read()
        return await asyncio.to_thread(
            _run_mapping_interactive_sync,
            api_key,
            mongodb_uri,
            cdm_content,
            cdm_file.filename,
            mapping_content,
            mapping_file.filename
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")


# ==========================================================================
# ENDPOINT 6C: Review Next
# ==========================================================================

# @app.get("/api/v1/review/next")
# async def review_next(session_id: str):
#     session = INTERACTIVE_SESSIONS.get(session_id)
#     if not session:
#         raise HTTPException(status_code=404, detail="Session not found")

#     idx = session.get("index", 0)
#     suggestions = session.get("suggestions", [])

#     if idx >= len(suggestions):
#         return JSONResponse(content={"done": True})

#     return JSONResponse(content={
#         "done": False,
#         "index": idx,
#         "total": len(suggestions),
#         "suggestion": suggestions[idx]
#     })


# ==========================================================================
# ENDPOINT 6D: Review Decision
# ==========================================================================

# @app.post("/api/v1/review/decision")
# async def review_decision(request: ReviewDecisionRequest):
#     session = INTERACTIVE_SESSIONS.get(request.session_id)
#     if not session:
#         raise HTTPException(status_code=404, detail="Session not found")

#     idx = session.get("index", 0)
#     suggestions = session.get("suggestions", [])
#     if idx >= len(suggestions):
#         return JSONResponse(content={"done": True})

#     # CHANGED: handle pending recommendation decision
    # if request.action in {"accept_suggested", "reject_suggested"}:
#         if session.get("pending_recommendation") is None:
#             raise HTTPException(status_code=400, detail="No pending recommendation")

#         pending_index = session.get("pending_index", idx)
#         suggestion = suggestions[pending_index]
#         recommendation = session.get("pending_recommendation")

        # if request.action == "accept_suggested":
        #     session["final_mappings"].append({
        #         'csv_table_name': suggestion.get('csv_table_name'),
        #         'csv_table_description': suggestion.get('csv_table_description'),
        #         'csv_column_name': suggestion.get('csv_column_name'),
        #         'csv_column_description': suggestion.get('csv_column_description'),
        #         'cdm_parent_name': recommendation.get('recommended_parent') if recommendation else None,
        #         'cdm_parent_definition': '',
        #         'parent_candidates': suggestion.get('parent_candidates', []),
        #         'cdm_column_name': recommendation.get('recommended_column_name') if recommendation else None,
        #         'cdm_column_definition': recommendation.get('definition_suggestion', '') if recommendation else '',
        #         'other_candidates': suggestion.get('other_candidates', []),
        #         'comprehensive_reason': 'User Accepted Suggested Term',
        #         'final_decision': 'User Accepted Suggested Term',
        #         'app_query_text': suggestion.get('app_query_text')
        #     })
        # else:
        #     session["unmapped_columns"].append({
        #         'csv_table_name': suggestion.get('csv_table_name'),
        #         'csv_table_description': suggestion.get('csv_table_description'),
        #         'csv_column_name': suggestion.get('csv_column_name'),
        #         'csv_column_description': suggestion.get('csv_column_description'),
        #         'other_candidates': suggestion.get('other_candidates', []),
        #         'parent_candidates': suggestion.get('parent_candidates', []),
        #         'comprehensive_reason': 'User Rejected Suggested Term',
        #         'error': 'User Rejected Suggested Term',
        #         'recommended_new_term': recommendation
        #     })

#         session["pending_recommendation"] = None
#         session["pending_index"] = None

#         session["index"] = pending_index + 1
#         if session["index"] >= len(suggestions):
#             return JSONResponse(content={"done": True})

#         return _review_next_response(session)

#     suggestion = suggestions[idx]
#     cdm_glossary_dict = session.get("cdm_glossary_dict", {})

#     llm_candidates = suggestion.get('llm_candidates', [])
#     chosen_term = None
#     skip_recommendation = None      ##change3

#     if request.action == "accept_top":
#         if llm_candidates:
#             chosen_term = llm_candidates[0].get('term')
#     elif request.action == "choose_candidate":
#         if request.candidate_index is None:
#             raise HTTPException(status_code=400, detail="candidate_index required")
#         if 0 <= request.candidate_index < len(llm_candidates):
#             chosen_term = llm_candidates[request.candidate_index].get('term')
#     elif request.action == "reject":
#         chosen_term = None
#     elif request.action == "skip":
#         chosen_term = None
#         # CHANGED: generate recommendation on skip
#         skip_recommendation = await asyncio.to_thread(
#             _build_skip_recommendation,
#             suggestion,
#             cdm_glossary_dict
#         )
#     else:
#         raise HTTPException(status_code=400, detail="Invalid action")

#     if request.action == "skip":
#         session["pending_recommendation"] = skip_recommendation
#         session["pending_index"] = idx
#         return _pending_recommendation_response(session)

#     if chosen_term:
#         final_mapping = _build_final_mapping_from_choice(
#             suggestion,
#             chosen_term,
#             cdm_glossary_dict,
#             decision_label="User Accepted"
#         )
#         session["final_mappings"].append(final_mapping)
#     else:
#         session["unmapped_columns"].append({
#             'csv_table_name': suggestion.get('csv_table_name'),
#             'csv_table_description': suggestion.get('csv_table_description'),
#             'csv_column_name': suggestion.get('csv_column_name'),
#             'csv_column_description': suggestion.get('csv_column_description'),
#             'other_candidates': suggestion.get('other_candidates', []),
#             'parent_candidates': suggestion.get('parent_candidates', []),
#             'comprehensive_reason': 'User Rejected',
#             'error': 'User Rejected'
#         })

#     session["index"] = idx + 1

#     if session["index"] >= len(suggestions):
#         return JSONResponse(content={"done": True})

#     return _review_next_response(session)


# ==========================================================================
# ENDPOINT 6F: Review Combined (Next or Decision)
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
        return JSONResponse(content={"done": True})

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
                'app_query_text': suggestion.get('app_query_text')
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
            return JSONResponse(content={"done": True})

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
# ENDPOINT 6E: Review Finalize
# ==========================================================================

@app.get("/api/v1/review/finalize")
async def review_finalize(session_id: str):
    session = INTERACTIVE_SESSIONS.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

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
        "status": "success",
        "message": "Interactive review completed",
        "output_files": {
            "mapped_file": mapped_filename,
            "unmapped_file": unmapped_filename
        }
    })


# ==========================================================================
# ENDPOINT 6G: Review Combined + Finalize
# ==========================================================================

@app.post("/api/v1/review/combined-finalize")
async def review_combined_finalize(request: ReviewCombinedFinalizeRequest):
    session = INTERACTIVE_SESSIONS.get(request.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if request.finalize:
        return await review_finalize(request.session_id)

    # Defer to combined behavior (next or decision)
    return await review_combined(ReviewCombinedRequest(
        session_id=request.session_id,
        action=request.action,
        candidate_index=request.candidate_index
    ))


# ==========================================================================
# ENDPOINT 7: Download Outputs
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
    print("📤 /api/v1/run-mapping (POST with file upload)")
    print("🧭 /api/v1/run-mapping-interactive (POST with file upload)")
    print("🧩 /api/v1/review/next")
    print("🧩 /api/v1/review/decision")
    print("🧩 /api/v1/review/finalize")
    print("⬇️  /api/v1/download (GET mapped/unmapped)")
    print("💚 /health (includes inputs info)")
    print(f"{'='*60}\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)