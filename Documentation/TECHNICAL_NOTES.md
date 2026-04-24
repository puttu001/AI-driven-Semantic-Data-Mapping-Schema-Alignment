# Technical Implementation Notes - CDM Mapping API

## Key Technical Innovations & Solutions

### 1. HTTP Deadlock Prevention (Critical Fix)

**Problem:**
Background FastAPI task makes HTTP call back to same FastAPI server → Server busy with background task → Can't handle HTTP request → Deadlock

**Original (Broken) Code:**
```python
# Background task running in FastAPI
async def process_workflow():
    # This HTTP call blocks forever!
    result = requests.post("http://localhost:8000/api/v1/llm/chat", ...)
```

**Solution - Direct Function Registry:**
```python
# Global function registry
_DIRECT_LLM_FUNCTION: Optional[Callable] = None

def register_direct_llm_function(func: Callable):
    global _DIRECT_LLM_FUNCTION
    _DIRECT_LLM_FUNCTION = func

# In background task setup
def direct_llm_call(system_prompt, user_prompt, response_format="json"):
    # Direct access to global LLM instance
    global LLM_INSTANCE
    llm_with_json = LLM_INSTANCE.bind(response_format={"type": "json_object"})
    response = llm_with_json.invoke(messages)
    return response.content

# Register before workflow starts
register_direct_llm_function(direct_llm_call)

# Workflow checks registry first
if _DIRECT_LLM_FUNCTION:
    content = _DIRECT_LLM_FUNCTION(system_prompt, user_prompt)
else:
    # Fallback to HTTP (when running outside FastAPI)
    content = requests.post("/api/v1/llm/chat", ...)
```

**Applied to:**
- LLM calls (`workflow/llm_operations.py`)
- Vector searches (`api/api_client.py`)

---

### 2. Real-Time Progress Tracking (SSE Implementation)

**Challenge:**
LLM processing takes 40-240 seconds, user has no visibility into progress

**Solution - Progress Callback Pattern:**

```python
# Step 1: Workflow accepts progress callback
def run_batch_process(csv_data_rows, progress_callback=None):
    for idx, row in enumerate(csv_data_rows):
        # Process row...
        
        # Update progress after each row
        if progress_callback:
            progress_callback(idx + 1, len(csv_data_rows))

# Step 2: FastAPI background task provides callback
def update_progress(current: int, total: int):
    INTERACTIVE_SESSIONS[session_id]["current_row_processed"] = current
    INTERACTIVE_SESSIONS[session_id]["message"] = f"Processing row {current}/{total}..."

result = workflow.run_batch_process(csv_data_rows, progress_callback=update_progress)

# Step 3: SSE endpoint reads session state
@app.get("/api/v1/session/{session_id}/progress-stream")
async def progress_stream(session_id: str):
    async def event_generator():
        while True:
            session = INTERACTIVE_SESSIONS.get(session_id)
            current = session.get("current_row_processed", 0)
            total = session.get("total_columns_processed", 0)
            percentage = (current / total * 100) if total > 0 else 0
            
            yield f"data: {json.dumps({'progress': {'completion_percentage': percentage}})}\n\n"
            
            if session["status"] == "ready_for_review":
                break
            
            await asyncio.sleep(1)
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")
```

**Why SSE instead of WebSocket:**
- Unidirectional (server → client only)
- Auto-reconnection built-in
- Browser-native EventSource API
- Lower overhead than WebSocket
- Perfect for progress updates

---

### 3. Session-Based State Management

**Architecture:**
```python
INTERACTIVE_SESSIONS = {
    "map_1708387456_abc123": {
        # Workflow metadata
        "status": "processing",
        "session_start_time": 1708387456.123,
        "message": "Processing row 2/3...",
        
        # Progress tracking
        "total_columns_processed": 3,
        "current_row_processed": 2,
        
        # LLM results
        "suggestions": [
            {
                "csv_column_name": "district_cd",
                "llm_candidates": [
                    {"term": "DISTRICT", "score": 90}
                ]
            }
        ],
        
        # Human review state
        "index": 0,
        "final_mappings": [],
        "unmapped_columns": [],
        "auto_rejected": [],
        
        # CDM reference data
        "cdm_glossary_dict": {...},
        "cdm_terms_list": [...]
    }
}
```

**Session Lifecycle:**
1. Created when workflow starts (POST /run-mapping-interactive)
2. Updated during background processing (status, progress)
3. Read/modified during human review (index, final_mappings)
4. Persists until download complete

**Important Limitation:**
Sessions are **in-memory**. If API server restarts, all sessions are lost.

**Production Recommendation:**
```python
import redis

# Replace in-memory dict with Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def set_session(session_id, data):
    redis_client.set(f"session:{session_id}", json.dumps(data))

def get_session(session_id):
    data = redis_client.get(f"session:{session_id}")
    return json.loads(data) if data else None
```

---

### 4. Async Background Tasks without Blocking

**FastAPI BackgroundTasks Pattern:**
```python
from fastapi import BackgroundTasks

@app.post("/api/v1/run-mapping-interactive")
async def run_mapping_interactive(
    file: UploadFile,
    background_tasks: BackgroundTasks,
    cdm_collection_name: str = Form(...),
    cdm_db_name: str = Form(...)
):
    # Generate session ID
    session_id = f"map_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    
    # Initialize session
    INTERACTIVE_SESSIONS[session_id] = {
        "status": "initializing",
        "message": "Starting workflow..."
    }
    
    # Add background task
    background_tasks.add_task(
        process_workflow_background,
        session_id,
        file_content,
        cdm_collection_name,
        cdm_db_name
    )
    
    # Return immediately
    return {
        "session_id": session_id,
        "status": "success",
        "message": "Workflow started"
    }

async def process_workflow_background(session_id, file_content, ...):
    try:
        # Long-running workflow (40-240 seconds)
        result = workflow.run_batch_process(csv_data_rows)
        
        # Update session when done
        INTERACTIVE_SESSIONS[session_id]["status"] = "ready_for_review"
    except Exception as e:
        INTERACTIVE_SESSIONS[session_id]["status"] = "error"
        INTERACTIVE_SESSIONS[session_id]["message"] = str(e)
```

**Why BackgroundTasks:**
- Non-blocking API response
- Runs in same process (shares global state)
- No need for Celery/Redis queue
- Simple for single-server deployment

**Production Scaling Alternative:**
Use Celery with Redis backend for multi-worker support:
```python
from celery import Celery

celery_app = Celery('cdm_mapping', broker='redis://localhost:6379/0')

@celery_app.task
def process_workflow_celery(session_id, file_content, ...):
    # Same workflow logic
    pass

@app.post("/api/v1/run-mapping-interactive")
async def run_mapping_interactive(...):
    task = process_workflow_celery.delay(session_id, file_content, ...)
    return {"session_id": session_id, "task_id": task.id}
```

---

### 5. Terminal Output Buffering Fix

**Problem:**
Print statements in workflow weren't visible in terminal during processing

**Root Cause:**
Python buffers stdout by default. Output only appears when buffer is full or script ends.

**Solution:**
Add `flush=True` to ALL print statements:
```python
# Before
print("Processing row 1/3...")  # Won't appear until much later

# After
print("Processing row 1/3...", flush=True)  # Appears immediately
```

**Alternative (Global Fix):**
```python
# In main.py or at API startup
import sys
sys.stdout = sys.stderr  # stderr is unbuffered

# Or set environment variable
export PYTHONUNBUFFERED=1
```

**Files Modified:**
- `workflow/enhanced_workflow.py` - 50+ print statements
- `api/fastapi_new.py` - 30+ print statements
- `workflow/llm_operations.py` - 10+ print statements

---

### 6. Dual-Agent LLM Architecture (Proposer + Challenger)

**Traditional Approach (Single LLM):**
```
Query → LLM → Answer
```
**Problem:** High false positive rate, overconfident suggestions

**Our Approach (Dual-Agent):**
```
Query → Proposer LLM → 5-10 Candidates
              ↓
        Challenger LLM → Validates Each → Top 2-3 Kept
```

**Proposer Prompt:**
```python
sys_prompt = """You are a proposer agent. Suggest 5-10 CDM terms that could match.
Be generous - include partial matches and related terms.
Output JSON: {"candidates": [{"term": "...", "score": 90, "reason": "..."}]}"""
```

**Challenger Prompt:**
```python
sys_prompt = """You are a critical challenger agent. Review proposer's suggestions.
REJECT terms that don't truly match. Be strict about semantic alignment.
Keep only 2-3 best matches.
Output JSON: {"kept": [...], "rejected": [...]}"""
```

**Benefits:**
- Higher precision (fewer false positives)
- Better explanations (challenger provides detailed rejection reasons)
- More transparent (users see both proposals and validations)

**Implementation:**
- Proposer: `prompts/optimized_suggestions.py`
- Challenger: `challenger_agent.py`
- Orchestration: `workflow/llm_operations.py` - `evaluate_with_reasoning_llm()`

---

### 7. TypedDict State with Optional Fields

**Challenge:**
LangGraph requires TypedDict for state, but we need optional fields (e.g., progress_callback)

**Original (Fails):**
```python
class MappingState(TypedDict):
    csv_data_rows: List[Dict]
    progress_callback: Callable  # ERROR: All fields are required
```

**Solution:**
```python
class MappingState(TypedDict, total=False):  # Allow optional fields
    csv_data_rows: List[Dict]
    cdm_glossary_dict: Dict
    # ... required fields ...
    progress_callback: Optional[Callable[[int, int], None]]  # Optional
```

**Why `total=False`:**
- Makes ALL fields optional by default
- Still type-safe with mypy/pyright
- Allows gradual state building in workflow nodes

---

### 8. Vector Search with MongoDB Atlas

**Collection Structure:**
```javascript
{
  "_id": ObjectId("..."),
  "table_name": "store_info",
  "column_name": "DISTRICT",
  "column_description": "Geographic district identifier",
  "embedding": [0.123, -0.456, ...],  // 3072-dimensional vector
  "metadata": {
    "table_description": "Store master data",
    "parent_table": "LOCATION"
  }
}
```

**Vector Index Configuration:**
```json
{
  "type": "vectorSearch",
  "path": "embedding",
  "numDimensions": 3072,
  "similarity": "cosine"
}
```

**Search Query:**
```python
from langchain_mongodb import MongoDBAtlasVectorSearch

vector_store = MongoDBAtlasVectorSearch(
    collection=collection,
    embedding=EMBEDDINGS_INSTANCE,
    index_name="vector_index"
)

# Semantic search
results = vector_store.similarity_search_with_score(
    query="District code for stores",
    k=25
)

# Returns: [(Document, score), ...]
# score: 0.0-1.0 (higher = more similar)
```

**Why MongoDB Atlas:**
- Native vector search support
- Scales to millions of documents
- Cosine similarity for semantic matching
- Supports metadata filtering

---

### 9. LangGraph Workflow State Machine

**Graph Structure:**
```
[generate_suggestions] → [display_all_suggestions] → [present_for_review]
                                                              ↓
                                                      [process_feedback]
                                                              ↓
                                            ┌─────────────────┴────────────────┐
                                            ↓                                  ↓
                                    [present_for_review]                    [END]
                                    (continue_review_cycle)              (end_cycle)
```

**State Flow:**
1. **generate_suggestions**: Vector search + LLM evaluation → suggestions list
2. **display_all_suggestions**: Log results to terminal
3. **present_for_review**: Present suggestion to user (INTERRUPT HERE)
4. **process_feedback**: Handle user decision (accept/reject)
5. **Decision**: More items? → Loop back to present_for_review : End

**Checkpointer with Interrupts:**
```python
from langgraph.checkpoint.memory import MemorySaver

workflow = workflow.compile(
    checkpointer=MemorySaver(),
    interrupt_after=["present_for_review"]  # Pause here for human input
)

# Run workflow
config = {"configurable": {"thread_id": "unique_id"}}
for event in workflow.stream(initial_state, config):
    if workflow.get_state(config).next == ("present_for_review",):
        # Workflow paused, waiting for user input
        user_input = get_user_decision()
        
        # Resume with feedback
        workflow.update_state(config, {"user_feedback": user_input})
```

**Why LangGraph:**
- Built-in state management
- Interrupt/resume for human-in-the-loop
- Checkpointing for crash recovery
- Visual workflow debugging

---

### 10. Pydantic Models for API Validation

**Request/Response Models:**
```python
from pydantic import BaseModel, Field
from typing import Optional, Literal

class ReviewCombinedRequest(BaseModel):
    session_id: str
    action: Optional[Literal[
        'accept_top',
        'reject',
        'choose_candidate',
        'skip',
        'accept_suggested',
        'reject_suggested'
    ]] = None
    candidate_index: Optional[int] = None

class PreReviewKPIsResponse(BaseModel):
    session_id: str
    timestamp: str
    kpis: dict
    breakdown: dict
```

**Benefits:**
- Automatic validation
- Auto-generated OpenAPI docs
- Type hints for IDE autocomplete
- Clear API contracts

---

## Performance Optimizations Implemented

### 1. Lazy Initialization
```python
# LLM and embeddings initialized only when first request comes
LLM_INSTANCE = None
EMBEDDINGS_INSTANCE = None

def get_llm():
    global LLM_INSTANCE
    if not LLM_INSTANCE:
        LLM_INSTANCE = ChatOpenAI(...)
    return LLM_INSTANCE
```

### 2. Vector Store Caching
```python
# Collections cached globally, reused across sessions
VECTOR_STORES = {
    "cdm_mapping_db.starbucks_cdm": {
        "vector_store": vector_store_instance,
        "collection": collection_instance
    }
}
```

### 3. Batch Processing
- Process all rows in single workflow run
- Reuse LLM instance across rows
- Single MongoDB connection for all searches

### 4. Top-K Filtering
```python
# Retrieve 25 candidates from vector search
cdm_matches = vector_search(query, top_k=25)

# LLM evaluates only top 10
llm_candidates = evaluate_with_llm(candidates[:10])

# Challenger keeps only top 2-3
final_candidates = challenger(llm_candidates)  # Returns 2-3
```

**Why Progressive Filtering:**
- Vector search is fast (25 docs in ~2s)
- LLM evaluation is slow (10 docs in ~15s)
- Only evaluate most promising candidates

---

## Testing Recommendations

### 1. Integration Tests
```python
import pytest
from fastapi.testclient import TestClient

client = TestClient(app)

def test_full_workflow():
    # Upload CSV
    with open("test_mapping.csv", "rb") as f:
        response = client.post(
            "/api/v1/run-mapping-interactive",
            files={"file": f},
            data={"cdm_collection_name": "test_cdm", "cdm_db_name": "test_db"}
        )
    
    assert response.status_code == 200
    session_id = response.json()["session_id"]
    
    # Monitor progress (manual check for now - SSE testing is tricky)
    
    # Get KPIs
    kpis_response = client.get(f"/api/v1/kpis/session/{session_id}")
    assert kpis_response.status_code == 200
    
    # Review loop
    while True:
        review_response = client.post(
            "/api/v1/review/combined",
            json={"session_id": session_id}
        )
        data = review_response.json()
        
        if data.get("done"):
            break
        
        # Auto-accept for testing
        client.post(
            "/api/v1/review/combined",
            json={"session_id": session_id, "action": "accept_top"}
        )
```

### 2. Load Testing
```python
# Use locust for load testing
from locust import HttpUser, task, between

class CDMUser(HttpUser):
    wait_time = between(1, 3)
    
    @task
    def upload_and_process(self):
        # Simulate concurrent users
        with open("test.csv", "rb") as f:
            self.client.post(
                "/api/v1/run-mapping-interactive",
                files={"file": f},
                data={"cdm_collection_name": "test", "cdm_db_name": "test"}
            )
```

---

## Deployment Considerations

### 1. Environment Variables
```bash
# Required
OPENAI_API_KEY=sk-proj-...
MONGODB_URI=mongodb+srv://...

# Optional
PYTHONUNBUFFERED=1  # For real-time terminal output
LOG_LEVEL=info
FASTAPI_ENV=production
```

### 2. Resource Requirements
- **CPU**: 2-4 cores (LLM calls are I/O bound, not CPU bound)
- **RAM**: 2-4 GB (for in-memory sessions and vector caches)
- **Disk**: 1 GB (mostly for output CSVs)

### 3. Scaling Strategy
- **Horizontal**: Deploy multiple API servers behind load balancer
- **Session Persistence**: Use Redis for shared session storage
- **Background Tasks**: Use Celery with Redis queue
- **Vector Search**: MongoDB Atlas auto-scales

### 4. Monitoring
```python
# Add Prometheus metrics
from prometheus_fastapi_instrumentator import Instrumentator

Instrumentator().instrument(app).expose(app)

# Key metrics to track:
# - workflow_duration_seconds (histogram)
# - llm_requests_total (counter)
# - vector_search_latency (histogram)
# - active_sessions (gauge)
```

---

## Security Considerations

### 1. File Upload Validation
```python
# Add in fastapi_new.py
ALLOWED_EXTENSIONS = {'.csv'}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

async def validate_upload(file: UploadFile):
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "Only CSV files allowed")
    
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(400, "File too large")
    
    return content
```

### 2. API Key Management
```python
# Use environment variables, NEVER hardcode
import os
from dotenv import load_dotenv

load_dotenv()  # Load from .env file
api_key = os.getenv("OPENAI_API_KEY")
```

### 3. Rate Limiting
```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.post("/api/v1/run-mapping-interactive")
@limiter.limit("5/minute")  # Max 5 workflows per minute per IP
async def run_mapping(...):
    pass
```

---

## Future Enhancements

### 1. WebSocket for Bidirectional Communication
Replace SSE with WebSocket for more interactive features:
```python
from fastapi import WebSocket

@app.websocket("/ws/session/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    
    while True:
        # Send progress
        await websocket.send_json({"progress": get_progress(session_id)})
        
        # Receive commands
        data = await websocket.receive_json()
        if data.get("command") == "pause":
            pause_workflow(session_id)
        
        await asyncio.sleep(1)
```

### 2. Redis Session Persistence
```python
import redis
import json

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

def save_session(session_id: str, data: dict):
    redis_client.setex(
        f"session:{session_id}",
        3600,  # 1 hour TTL
        json.dumps(data)
    )

def get_session(session_id: str) -> dict:
    data = redis_client.get(f"session:{session_id}")
    return json.loads(data) if data else None
```

### 3. Batch Review Endpoint
Allow reviewing multiple suggestions in one request:
```python
@app.post("/api/v1/review/batch")
async def review_batch(request: BatchReviewRequest):
    decisions = request.decisions  # List of {index, action, candidate_index}
    
    for decision in decisions:
        # Process each decision
        pass
    
    return {"processed": len(decisions)}
```

---

**Document Version:** 1.0  
**Last Updated:** February 19, 2026
