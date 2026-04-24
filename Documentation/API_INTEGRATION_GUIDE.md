# CDM Mapping API - Integration Guide

**Version:** 1.0  
**Last Updated:** February 19, 2026  
**Base URL:** `http://localhost:8000` (or your deployment URL)

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Complete Workflow Sequence](#complete-workflow-sequence)
4. [API Endpoints Reference](#api-endpoints-reference)
5. [Real-Time Progress Tracking (SSE)](#real-time-progress-tracking-sse)
6. [Integration Examples](#integration-examples)
7. [Error Handling](#error-handling)
8. [Performance Considerations](#performance-considerations)

---

## System Overview

### What This System Does

The CDM (Common Data Model) Mapping API is an AI-powered service that automatically maps source CSV columns to a standardized Common Data Model using:

- **Vector Search**: MongoDB Atlas with OpenAI embeddings for semantic similarity
- **LLM Evaluation**: Proposer/Challenger dual-agent system using GPT-4o-mini
- **Human-in-the-Loop**: Interactive review workflow for final approval
- **Real-Time Streaming**: Server-Sent Events (SSE) for progress updates

### Key Features

✅ **Automatic Mapping**: AI suggests best CDM mappings with confidence scores  
✅ **Dual-Agent Validation**: Proposer suggests, Challenger validates  
✅ **Real-Time Progress**: SSE streaming shows row-by-row processing  
✅ **Human Review**: Interactive approval/rejection of AI suggestions  
✅ **KPI Analytics**: Pre-review and post-review metrics  
✅ **Session Management**: Stateful workflow with unique session IDs  

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Your Frontend/Platform                  │
│                  (React, Angular, Vue, etc.)                │
└────────────────────┬────────────────────────────────────────┘
                     │
                     │ HTTP/SSE
                     │
┌────────────────────▼────────────────────────────────────────┐
│                  FastAPI Service                            │
│              (api/fastapi_new.py)                           │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Workflow   │  │  Vector      │  │   LLM        │     │
│  │   Engine     │◄─┤  Search      │◄─┤   Service    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│         │                  │                  │             │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
          │                  │                  │
          ▼                  ▼                  ▼
  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
  │  LangGraph   │  │  MongoDB     │  │   OpenAI     │
  │  Workflow    │  │  Atlas       │  │   API        │
  └──────────────┘  └──────────────┘  └──────────────┘
```

### Technology Stack

- **Framework**: FastAPI (async Python web framework)
- **Workflow**: LangGraph (state machine for complex workflows)
- **Vector DB**: MongoDB Atlas with vector search
- **Embeddings**: OpenAI text-embedding-3-large
- **LLM**: OpenAI GPT-4o-mini
- **Streaming**: Server-Sent Events (SSE)

---

## Complete Workflow Sequence

### Phase 1: Initialization (One-Time Setup)

```
POST /api/v1/embeddings/initialize
  → Initialize OpenAI embeddings model

POST /api/v1/mongodb/create-collection (for CDM data)
  → Create vector collection with embeddings

POST /api/v1/mongodb/create-collection (for CSV data)
  → Create vector collection for source data
```

### Phase 2: Start Mapping Workflow

```
POST /api/v1/run-mapping-interactive
  ↓
  Returns: { session_id, status: "initializing", message: "..." }
```

### Phase 3: Monitor Progress (Real-Time)

```
EventSource → GET /api/v1/session/{session_id}/progress-stream
  ↓
  Streams: { status, progress: { completion_percentage, ... }, message }
  ↓
  Updates every 1 second until status = "ready_for_review"
```

### Phase 4: Review Pre-Processing KPIs

```
GET /api/v1/kpis/session/{session_id}
  ↓
  Returns: { total_mappings, avg_confidence, acceptance_rate, ... }
```

### Phase 5: Human Review Loop

```
POST /api/v1/review/combined (with session_id, no action)
  ↓
  Returns: { index, total, suggestion: { csv_column, llm_candidates } }
  ↓
POST /api/v1/review/combined (with action: "accept_top" | "reject" | "choose_candidate" | "skip")
  ↓
  Repeat until done: true
```

### Phase 6: Final KPIs and Download

```
GET /api/v1/kpis/session/{session_id}/final
  → { total_mapped, avg_confidence, unmapped, user_rejected }

GET /api/v1/download?kind=mapped
  → Download mapped results CSV

GET /api/v1/download?kind=unmapped
  → Download unmapped columns CSV
```

---

## API Endpoints Reference

### 1. Embeddings Initialization

**Endpoint:** `POST /api/v1/embeddings/initialize`

**Purpose:** Initialize OpenAI embeddings model (call once on startup)

**Request Body:**
```json
{
  "model_name": "text-embedding-3-large",
  "dimensions": 3072
}
```

**Response:**
```json
{
  "status": "success",
  "model": "text-embedding-3-large",
  "dimensions": 3072
}
```

---

### 2. Create Vector Collection

**Endpoint:** `POST /api/v1/mongodb/create-collection`

**Purpose:** Create MongoDB collection with vector embeddings

**Request Body:**
```json
{
  "csv_file_path": "Inputs/CDM_data/starbucks_CDM_UPDATED.csv",
  "mongodb_uri": "mongodb+srv://...",
  "db_name": "cdm_mapping_db",
  "collection_name": "starbucks_cdm",
  "index_name": "vector_index",
  "overwrite": false
}
```

**Response:**
```json
{
  "status": "success",
  "collection": "starbucks_cdm",
  "documents_inserted": 150,
  "vector_store_created": true
}
```

---

### 3. Start Interactive Mapping Workflow ⭐

**Endpoint:** `POST /api/v1/run-mapping-interactive`

**Purpose:** Start the CDM mapping workflow for a CSV file

**Request:** `multipart/form-data`
- `file`: CSV file upload (required)
- `cdm_collection_name`: Collection name (required)
- `cdm_db_name`: Database name (required)
- `csv_collection_name`: CSV collection name (optional)
- `csv_db_name`: CSV database name (optional)

**Response:**
```json
{
  "status": "success",
  "session_id": "map_1708387456_abc123",
  "workflow_status": "initializing",
  "message": "Workflow started. Connect to /api/v1/session/{session_id}/progress-stream for real-time updates.",
  "next_steps": {
    "1_monitor_progress": "GET /api/v1/session/{session_id}/progress-stream (SSE)",
    "2_view_kpis": "GET /api/v1/kpis/session/{session_id}",
    "3_review": "POST /api/v1/review/combined"
  }
}
```

**Processing Flow:**
1. Accepts CSV file upload
2. Creates session with unique ID
3. Starts background workflow (20-245 seconds)
4. Returns immediately with session ID
5. Workflow proceeds: Indexing → Vector Search → LLM Evaluation → Ready for Review

---

### 4. Real-Time Progress Stream (SSE) ⭐⭐⭐

**Endpoint:** `GET /api/v1/session/{session_id}/progress-stream`

**Purpose:** Stream real-time progress updates during workflow processing

**Protocol:** Server-Sent Events (SSE)

**JavaScript Example:**
```javascript
const eventSource = new EventSource(`/api/v1/session/${sessionId}/progress-stream`);

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  console.log(`Status: ${data.status}`);
  console.log(`Progress: ${data.progress.completion_percentage}%`);
  console.log(`Message: ${data.message}`);
  
  // Update progress bar
  progressBar.style.width = `${data.progress.completion_percentage}%`;
  
  // Check if ready for review
  if (data.status === 'ready_for_review') {
    eventSource.close();
    loadKPIs();
  }
};

eventSource.onerror = (error) => {
  console.error('SSE Error:', error);
  eventSource.close();
};
```

**SSE Event Data Format:**
```json
{
  "session_id": "map_1708387456_abc123",
  "status": "processing",
  "workflow_phase": "workflow_processing",
  "message": "Processing row 2/3...",
  "progress": {
    "total_items": 3,
    "completed_items": 2,
    "remaining_items": 1,
    "completion_percentage": 66.7
  },
  "timing": {
    "elapsed_seconds": 45.2
  }
}
```

**Status Values:**
- `initializing`: Vector indexes being created
- `processing`: LLM evaluating rows (progress bar increments here)
- `ready_for_review`: Workflow complete, waiting for human review
- `error`: Processing failed

---

### 5. Pre-Review KPIs

**Endpoint:** `GET /api/v1/kpis/session/{session_id}`

**Purpose:** Get metrics after LLM processing, before human review

**Response:**
```json
{
  "session_id": "map_1708387456_abc123",
  "timestamp": "2026-02-19T14:30:00Z",
  "kpis": {
    "avg_confidence_score": 0.85,
    "acceptance_rate": 75.5,
    "challenger_rejection_rate": 15.2,
    "api_latency": {
      "total_duration_seconds": 67.3,
      "avg_time_per_column_seconds": 22.4,
      "total_columns_processed": 3
    }
  },
  "breakdown": {
    "total_mappings": 3,
    "proposer_accepted": 2,
    "challenger_rejected": 1,
    "unmapped_columns": 0
  }
}
```

---

### 6. Human Review - Combined Endpoint ⭐⭐⭐

**Endpoint:** `POST /api/v1/review/combined`

**Purpose:** Get next suggestion OR submit decision (dual-purpose endpoint)

#### 6a. Get Next Suggestion

**Request:**
```json
{
  "session_id": "map_1708387456_abc123"
}
```

**Response:**
```json
{
  "done": false,
  "index": 0,
  "total": 3,
  "suggestion": {
    "csv_table_name": "store_master",
    "csv_column_name": "district_cd",
    "csv_column_description": "District code for the store",
    "csv_table_description": "Master table for store information",
    "llm_candidates": [
      {
        "term": "DISTRICT",
        "table_name": "store_info",
        "score": 90,
        "reason": "High semantic similarity - both represent district identifiers"
      },
      {
        "term": "REGION",
        "table_name": "store_info",
        "score": 50,
        "reason": "Related but less specific - region is broader than district"
      }
    ],
    "parent_candidates": [],
    "other_candidates": []
  }
}
```

#### 6b. Submit Decision

**Request:**
```json
{
  "session_id": "map_1708387456_abc123",
  "action": "accept_top",
  "candidate_index": null
}
```

**Action Types:**
- `accept_top`: Accept the highest-scored candidate
- `reject`: Reject all candidates (add to unmapped)
- `choose_candidate`: Select specific candidate by index
- `skip`: Skip to next (may trigger term recommendation)
- `accept_suggested`: Accept AI's new term suggestion (after skip)
- `reject_suggested`: Reject AI's new term suggestion

**Request with Candidate Selection:**
```json
{
  "session_id": "map_1708387456_abc123",
  "action": "choose_candidate",
  "candidate_index": 1
}
```

**Response (More Items):**
```json
{
  "done": false,
  "index": 1,
  "total": 3,
  "suggestion": { /* next suggestion */ }
}
```

**Response (All Reviewed):**
```json
{
  "done": true,
  "status": "success",
  "message": "Interactive review completed",
  "output_files": {
    "mapped_file": "Final_CDM_Mappings_interactive_1708387456.csv",
    "unmapped_file": "Unmapped_Columns_interactive_1708387456.csv"
  }
}
```

---

### 7. Post-Review Final KPIs

**Endpoint:** `GET /api/v1/kpis/session/{session_id}/final`

**Purpose:** Get final metrics after human review completion

**Response:**
```json
{
  "session_id": "map_1708387456_abc123",
  "timestamp": "2026-02-19T14:35:00Z",
  "total_mapped": 2,
  "total_unmapped": 1,
  "avg_confidence_score": 0.90,
  "user_accepted": 2,
  "user_rejected": 1,
  "user_modified": 0,
  "output_files": {
    "mapped_csv": "Final_CDM_Mappings_interactive_1708387456.csv",
    "unmapped_csv": "Unmapped_Columns_interactive_1708387456.csv"
  }
}
```

---

### 8. Download Results

**Endpoint:** `GET /api/v1/download`

**Purpose:** Download final mapping results

**Query Parameters:**
- `kind`: `mapped` or `unmapped` (required)
- `filename`: Specific filename (optional, defaults to latest)

**Examples:**
```
GET /api/v1/download?kind=mapped
GET /api/v1/download?kind=unmapped
GET /api/v1/download?kind=mapped&filename=Final_CDM_Mappings_interactive_1708387456.csv
```

**Response:** CSV file download

**Mapped CSV Columns:**
- `csv_table_name`
- `csv_column_name`
- `csv_column_description`
- `cdm_parent_name`
- `cdm_column_name`
- `cdm_column_definition`
- `final_decision` (e.g., "User Accepted", "LLM Proposer Accepted")
- `comprehensive_reason`

**Unmapped CSV Columns:**
- `csv_table_name`
- `csv_column_name`
- `csv_column_description`
- `error` (reason for no mapping)

---

### 9. Health Check

**Endpoint:** `GET /health`

**Purpose:** Check service health and available inputs

**Response:**
```json
{
  "status": "healthy",
  "embeddings": true,
  "llm": true,
  "mongodb_connected": true,
  "collections": 2,
  "inputs": {
    "CDM_data": [
      "starbucks_CDM_UPDATED_20260212_150801.csv",
      "nat_CDM_20260212_155534.csv"
    ],
    "mapping_data": []
  }
}
```

---

## Real-Time Progress Tracking (SSE)

### Why SSE Instead of Polling?

**Traditional Polling (❌ Not Used):**
```javascript
// Old approach - inefficient
setInterval(async () => {
  const response = await fetch(`/api/v1/progress/${sessionId}`);
  const data = await response.json();
  updateUI(data);
}, 1000); // Polls every second
```

**Problems with Polling:**
- Server gets bombarded with requests
- Delayed updates (1-second intervals)
- Wasted bandwidth
- Increased server load

**Server-Sent Events (✅ Our Approach):**
```javascript
// Efficient real-time streaming
const eventSource = new EventSource(`/api/v1/session/${sessionId}/progress-stream`);
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  updateUI(data); // Instant updates
};
```

**Benefits of SSE:**
✅ Automatic reconnection  
✅ Single persistent connection  
✅ Instant updates (no polling delay)  
✅ Browser-native support  
✅ Lower server load  

### Implementation Details

**Backend (FastAPI):**
```python
from fastapi.responses import StreamingResponse

@app.get("/api/v1/session/{session_id}/progress-stream")
async def progress_stream(session_id: str):
    async def event_generator():
        while True:
            session = INTERACTIVE_SESSIONS.get(session_id)
            progress_data = {
                "status": session["status"],
                "progress": {
                    "completed_items": session["current_row_processed"],
                    "total_items": session["total_columns_processed"],
                    "completion_percentage": calculate_percentage()
                }
            }
            yield f"data: {json.dumps(progress_data)}\n\n"
            
            if session["status"] in ["ready_for_review", "error"]:
                break
            await asyncio.sleep(1)
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")
```

**How Progress Updates Work:**

1. **Workflow Callback:**
   ```python
   def update_progress(current: int, total: int):
       INTERACTIVE_SESSIONS[session_id]["current_row_processed"] = current
       INTERACTIVE_SESSIONS[session_id]["message"] = f"Processing row {current}/{total}..."
   
   workflow.run_batch_process(csv_data_rows, progress_callback=update_progress)
   ```

2. **Row-by-Row Updates:**
   - Row 1 processed → Callback → Session updated → SSE sends 33.3%
   - Row 2 processed → Callback → Session updated → SSE sends 66.7%
   - Row 3 processed → Callback → Session updated → SSE sends 100%

3. **SSE Reads Session:**
   - Every 1 second, SSE endpoint reads session state
   - Sends current progress to connected clients
   - Clients update UI instantly

---

## Integration Examples

### Example 1: React Integration

```typescript
import { useState, useEffect } from 'react';

interface ProgressData {
  status: string;
  progress: {
    completion_percentage: number;
    completed_items: number;
    total_items: number;
  };
  message: string;
}

function CDMMappingWorkflow() {
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [progress, setProgress] = useState<ProgressData | null>(null);
  const [eventSource, setEventSource] = useState<EventSource | null>(null);

  // Step 1: Upload CSV and start workflow
  const startMapping = async (file: File, cdmCollection: string) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('cdm_collection_name', cdmCollection);
    formData.append('cdm_db_name', 'cdm_mapping_db');

    const response = await fetch('/api/v1/run-mapping-interactive', {
      method: 'POST',
      body: formData
    });

    const data = await response.json();
    setSessionId(data.session_id);
    connectSSE(data.session_id);
  };

  // Step 2: Connect to SSE stream
  const connectSSE = (sessionId: string) => {
    const es = new EventSource(`/api/v1/session/${sessionId}/progress-stream`);
    
    es.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setProgress(data);
      
      if (data.status === 'ready_for_review') {
        es.close();
        loadKPIs(sessionId);
      }
    };

    es.onerror = () => {
      es.close();
      console.error('SSE connection failed');
    };

    setEventSource(es);
  };

  // Step 3: Load pre-review KPIs
  const loadKPIs = async (sessionId: string) => {
    const response = await fetch(`/api/v1/kpis/session/${sessionId}`);
    const kpis = await response.json();
    console.log('Pre-review KPIs:', kpis);
  };

  // Step 4: Get first suggestion
  const startReview = async () => {
    if (!sessionId) return;
    
    const response = await fetch('/api/v1/review/combined', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId })
    });

    const suggestion = await response.json();
    return suggestion;
  };

  // Step 5: Submit decision
  const submitDecision = async (action: string, candidateIndex?: number) => {
    const response = await fetch('/api/v1/review/combined', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        action: action,
        candidate_index: candidateIndex
      })
    });

    const result = await response.json();
    
    if (result.done) {
      loadFinalKPIs(sessionId!);
    }
    
    return result;
  };

  // Step 6: Load final KPIs
  const loadFinalKPIs = async (sessionId: string) => {
    const response = await fetch(`/api/v1/kpis/session/${sessionId}/final`);
    const finalKpis = await response.json();
    console.log('Final KPIs:', finalKpis);
  };

  // Cleanup
  useEffect(() => {
    return () => {
      if (eventSource) {
        eventSource.close();
      }
    };
  }, [eventSource]);

  return (
    <div>
      {/* UI components here */}
      {progress && (
        <div className="progress-bar">
          <div style={{ width: `${progress.progress.completion_percentage}%` }}>
            {progress.progress.completion_percentage.toFixed(1)}%
          </div>
          <p>{progress.message}</p>
        </div>
      )}
    </div>
  );
}
```

---

### Example 2: Python Client Integration

```python
import requests
import json
import sseclient  # pip install sseclient-py

class CDMMappingClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session_id = None
    
    def start_mapping(self, csv_file_path: str, cdm_collection: str, cdm_db: str):
        """Upload CSV and start mapping workflow"""
        with open(csv_file_path, 'rb') as f:
            files = {'file': f}
            data = {
                'cdm_collection_name': cdm_collection,
                'cdm_db_name': cdm_db
            }
            
            response = requests.post(
                f"{self.base_url}/api/v1/run-mapping-interactive",
                files=files,
                data=data
            )
            
            result = response.json()
            self.session_id = result['session_id']
            return result
    
    def monitor_progress(self, callback=None):
        """Monitor progress via SSE stream"""
        url = f"{self.base_url}/api/v1/session/{self.session_id}/progress-stream"
        response = requests.get(url, stream=True)
        client = sseclient.SSEClient(response)
        
        for event in client.events():
            data = json.loads(event.data)
            
            if callback:
                callback(data)
            
            print(f"Progress: {data['progress']['completion_percentage']}%")
            print(f"Message: {data['message']}")
            
            if data['status'] in ['ready_for_review', 'error']:
                break
    
    def get_pre_review_kpis(self):
        """Get KPIs after LLM processing"""
        response = requests.get(
            f"{self.base_url}/api/v1/kpis/session/{self.session_id}"
        )
        return response.json()
    
    def get_next_suggestion(self):
        """Get next suggestion for review"""
        response = requests.post(
            f"{self.base_url}/api/v1/review/combined",
            json={"session_id": self.session_id}
        )
        return response.json()
    
    def submit_decision(self, action: str, candidate_index: int = None):
        """Submit review decision"""
        payload = {
            "session_id": self.session_id,
            "action": action
        }
        if candidate_index is not None:
            payload["candidate_index"] = candidate_index
        
        response = requests.post(
            f"{self.base_url}/api/v1/review/combined",
            json=payload
        )
        return response.json()
    
    def download_results(self, kind: str = "mapped", output_path: str = None):
        """Download mapped or unmapped results"""
        response = requests.get(
            f"{self.base_url}/api/v1/download",
            params={"kind": kind}
        )
        
        if output_path:
            with open(output_path, 'wb') as f:
                f.write(response.content)
        
        return response.content

# Usage Example
if __name__ == "__main__":
    client = CDMMappingClient()
    
    # Start workflow
    result = client.start_mapping(
        csv_file_path="data/my_mapping.csv",
        cdm_collection="starbucks_cdm",
        cdm_db="cdm_mapping_db"
    )
    print(f"Session ID: {result['session_id']}")
    
    # Monitor progress
    def progress_callback(data):
        print(f"Status: {data['status']}, Progress: {data['progress']['completion_percentage']}%")
    
    client.monitor_progress(callback=progress_callback)
    
    # Get KPIs
    kpis = client.get_pre_review_kpis()
    print(f"Average Confidence: {kpis['kpis']['avg_confidence_score']}")
    
    # Review loop
    while True:
        suggestion = client.get_next_suggestion()
        
        if suggestion.get('done'):
            print("Review complete!")
            break
        
        print(f"\nReviewing {suggestion['index'] + 1}/{suggestion['total']}")
        print(f"Column: {suggestion['suggestion']['csv_column_name']}")
        
        # Auto-accept top candidate for demo
        result = client.submit_decision(action="accept_top")
    
    # Download results
    client.download_results(kind="mapped", output_path="final_mappings.csv")
```

---

## Error Handling

### Common Error Responses

#### 404 - Session Not Found
```json
{
  "detail": "Session not found"
}
```
**Cause:** Invalid session ID or session expired  
**Solution:** Start new workflow with POST /api/v1/run-mapping-interactive

#### 400 - Invalid Action
```json
{
  "detail": "Invalid action"
}
```
**Cause:** Unsupported action type in review endpoint  
**Solution:** Use valid actions: accept_top, reject, choose_candidate, skip

#### 503 - Service Unavailable
```json
{
  "detail": "LLM init failed: OPENAI_API_KEY not set"
}
```
**Cause:** Missing environment variables or service dependencies  
**Solution:** Set OPENAI_API_KEY and MONGODB_URI environment variables

#### SSE Connection Errors

**Browser EventSource:**
```javascript
eventSource.onerror = (error) => {
  console.error('SSE Error:', error);
  
  // Retry logic
  setTimeout(() => {
    const newEventSource = new EventSource(url);
    // ... set up listeners
  }, 5000);
};
```

---

## Performance Considerations

### Processing Time Estimates

| Rows | Vector Search | LLM Evaluation | Total Time |
|------|---------------|----------------|------------|
| 1    | ~2s          | ~10-15s        | ~15-20s    |
| 3    | ~5s          | ~30-45s        | ~40-60s    |
| 10   | ~15s         | ~100-180s      | ~120-200s  |
| 50   | ~60s         | ~500-900s      | ~10-15min  |

### Optimization Tips

1. **Batch Processing**: Process multiple CSVs sequentially with same session
2. **Caching**: CDM embeddings are cached in MongoDB (reused across sessions)
3. **Concurrent Sessions**: API supports multiple simultaneous sessions
4. **Background Tasks**: Workflow runs in background, doesn't block API

### Rate Limits

- **OpenAI API**: 500 requests/min (default tier)
- **MongoDB Atlas**: 500 connections (M10 cluster)
- **SSE Connections**: 100 concurrent connections recommended

---

## Key Differences in This Implementation

### 1. Dual-Agent Architecture (Proposer + Challenger)

Unlike typical LLM systems that give a single answer:
- **Proposer**: Suggests 5-10 candidates with scores
- **Challenger**: Validates each, keeps top 2-3
- **Result**: Higher accuracy, lower false positives

### 2. Session-Based State Management

All workflow state is stored in `INTERACTIVE_SESSIONS[session_id]`:
```python
INTERACTIVE_SESSIONS = {
  "map_1708387456_abc123": {
    "status": "processing",
    "suggestions": [...],
    "index": 0,
    "final_mappings": [],
    "current_row_processed": 2,
    "total_columns_processed": 3
  }
}
```

**Important:** Sessions are in-memory. If API restarts, sessions are lost.  
**Recommendation:** For production, persist sessions to Redis/MongoDB.

### 3. Direct Function Calls (No HTTP Deadlock)

Background workflow calls LLM/Vector Search **directly** instead of making HTTP requests back to itself:

```python
# Registering direct functions
register_direct_search_function(direct_vector_search)
register_direct_llm_function(direct_llm_call)

# Workflow uses direct calls
if _DIRECT_SEARCH_FUNCTION:
    results = _DIRECT_SEARCH_FUNCTION(query, collection, db_name)
else:
    # Fallback to HTTP (when running outside FastAPI)
    results = requests.post("/api/v1/mongodb/search", ...)
```

**Why:** Prevents deadlock when background task tries to call API that's busy with background task.

---

## Environment Variables Required

```bash
# OpenAI API
OPENAI_API_KEY=sk-...

# MongoDB Atlas
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/

# Optional
FASTAPI_ENV=production
LOG_LEVEL=info
```

---

## Quick Start Checklist for Integration

- [ ] Set up environment variables (OPENAI_API_KEY, MONGODB_URI)
- [ ] Initialize embeddings: POST /api/v1/embeddings/initialize
- [ ] Create CDM collection: POST /api/v1/mongodb/create-collection
- [ ] Implement file upload UI
- [ ] Integrate SSE progress stream (EventSource)
- [ ] Build review interface with accept/reject buttons
- [ ] Add KPI dashboard displays
- [ ] Implement download functionality
- [ ] Add error handling and retry logic
- [ ] Test with sample CSV files

---

## Support & Contact

**API Documentation:** http://localhost:8000/docs (Swagger UI)  
**ReDoc:** http://localhost:8000/redoc  
**Health Check:** http://localhost:8000/health

For integration questions, refer to:
- `frontend/index.html` - Reference implementation
- `frontend/README.md` - Frontend guide
- `ARCHITECTURE.md` - System architecture details

---

**Last Updated:** February 19, 2026  
**Version:** 1.0.0
