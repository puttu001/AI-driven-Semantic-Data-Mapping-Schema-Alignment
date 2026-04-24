# CDM Mapping API - Quick Reference

## 🚀 Quick Start (5 Steps)

### 1. Initialize (One-Time Setup)
```bash
POST /api/v1/embeddings/initialize
POST /api/v1/mongodb/create-collection  # For CDM
POST /api/v1/mongodb/create-collection  # For CSV data
```

### 2. Start Workflow
```javascript
const formData = new FormData();
formData.append('file', csvFile);
formData.append('cdm_collection_name', 'starbucks_cdm');
formData.append('cdm_db_name', 'cdm_mapping_db');

const response = await fetch('/api/v1/run-mapping-interactive', {
  method: 'POST',
  body: formData
});

const { session_id } = await response.json();
```

### 3. Monitor Progress (SSE)
```javascript
const eventSource = new EventSource(`/api/v1/session/${session_id}/progress-stream`);

eventSource.onmessage = (event) => {
  const { status, progress, message } = JSON.parse(event.data);
  
  // Update UI
  progressBar.style.width = `${progress.completion_percentage}%`;
  statusText.textContent = message;
  
  if (status === 'ready_for_review') {
    eventSource.close();
    loadKPIs();
  }
};
```

### 4. Review Suggestions
```javascript
// Get first suggestion
let result = await fetch('/api/v1/review/combined', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ session_id })
}).then(r => r.json());

// Review loop
while (!result.done) {
  const { suggestion, index, total } = result;
  
  // User decides: accept, reject, or choose specific candidate
  const action = getUserDecision(); // 'accept_top', 'reject', 'choose_candidate'
  
  result = await fetch('/api/v1/review/combined', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      session_id,
      action,
      candidate_index: selectedIndex // Only for 'choose_candidate'
    })
  }).then(r => r.json());
}
```

### 5. Download Results
```javascript
// Mapped results
window.location.href = `/api/v1/download?kind=mapped`;

// Unmapped results
window.location.href = `/api/v1/download?kind=unmapped`;
```

---

## 📋 Essential Endpoints

| Endpoint | Method | Purpose | Returns |
|----------|--------|---------|---------|
| `/api/v1/run-mapping-interactive` | POST | Start workflow | `session_id` |
| `/api/v1/session/{id}/progress-stream` | GET (SSE) | Real-time progress | Progress events |
| `/api/v1/kpis/session/{id}` | GET | Pre-review metrics | KPIs |
| `/api/v1/review/combined` | POST | Get/submit review | Suggestion or done |
| `/api/v1/kpis/session/{id}/final` | GET | Post-review metrics | Final KPIs |
| `/api/v1/download` | GET | Download CSVs | File download |

---

## 🔄 Workflow Sequence

```
┌─────────────────────────────────────────────────────────────┐
│ 1. POST /run-mapping-interactive (file upload)             │
│    → Returns: { session_id, status: "initializing" }       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. SSE /session/{id}/progress-stream                        │
│    → Streams: 0% → 33% → 66% → 100%                         │
│    → Status: "processing" → "ready_for_review"              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. GET /kpis/session/{id}                                   │
│    → Returns: avg_confidence, acceptance_rate, etc.         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. POST /review/combined (loop until done)                  │
│    → Get suggestion → User decision → Submit action →       │
│    → Repeat for each suggestion                             │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. GET /kpis/session/{id}/final                             │
│    → Returns: total_mapped, user_accepted, etc.             │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. GET /download?kind=mapped                                │
│    → Downloads: Final_CDM_Mappings_{timestamp}.csv          │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Review Actions

| Action | Description | When to Use |
|--------|-------------|-------------|
| `accept_top` | Accept highest-scored candidate | AI's top choice looks good |
| `reject` | Reject all candidates | No good matches |
| `choose_candidate` | Select specific candidate by index | User prefers different option |
| `skip` | Skip this suggestion | Need to think/come back later |
| `accept_suggested` | Accept AI's new term recommendation | After skipping, AI suggests new term |
| `reject_suggested` | Reject AI's new term recommendation | After skipping, decline suggestion |

---

## 📊 SSE Progress Data Format

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
- `initializing` - Creating vector indexes (20s wait)
- `processing` - LLM evaluating rows (progress bar updates here)
- `ready_for_review` - Workflow done, ready for human review
- `error` - Processing failed

---

## 🎨 Frontend Integration Checklist

```jsx
// 1. State Management
const [sessionId, setSessionId] = useState(null);
const [progress, setProgress] = useState(0);
const [suggestions, setSuggestions] = useState([]);
const [currentIndex, setCurrentIndex] = useState(0);

// 2. File Upload Handler
const handleUpload = async (file) => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('cdm_collection_name', 'your_collection');
  formData.append('cdm_db_name', 'your_db');
  
  const res = await fetch('/api/v1/run-mapping-interactive', {
    method: 'POST',
    body: formData
  });
  
  const { session_id } = await res.json();
  setSessionId(session_id);
  connectSSE(session_id);
};

// 3. SSE Connection
const connectSSE = (sessionId) => {
  const es = new EventSource(`/api/v1/session/${sessionId}/progress-stream`);
  
  es.onmessage = (event) => {
    const data = JSON.parse(event.data);
    setProgress(data.progress.completion_percentage);
    
    if (data.status === 'ready_for_review') {
      es.close();
      startReview();
    }
  };
};

// 4. Review Loop
const startReview = async () => {
  const res = await fetch('/api/v1/review/combined', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId })
  });
  
  const data = await res.json();
  if (!data.done) {
    setSuggestions([...suggestions, data.suggestion]);
  }
};

// 5. Submit Decision
const submitDecision = async (action, candidateIndex = null) => {
  const res = await fetch('/api/v1/review/combined', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      session_id: sessionId,
      action,
      candidate_index: candidateIndex
    })
  });
  
  const data = await res.json();
  
  if (data.done) {
    loadFinalResults();
  } else {
    setSuggestions([...suggestions, data.suggestion]);
    setCurrentIndex(data.index);
  }
};
```

---

## 🛠️ Common Issues & Solutions

### Issue: Progress bar stuck at 0%
**Solution:** Status must be `"processing"` (not `"initializing"`) for progress updates

### Issue: SSE connection drops
**Solution:** Implement reconnection logic:
```javascript
eventSource.onerror = () => {
  eventSource.close();
  setTimeout(() => connectSSE(sessionId), 2000);
};
```

### Issue: Session not found
**Solution:** Sessions are in-memory. If API restarts, start new workflow

### Issue: Review endpoint returns 400
**Solution:** Check `action` field matches one of: `accept_top`, `reject`, `choose_candidate`, `skip`

### Issue: Download endpoint 404
**Solution:** Ensure review is complete (`done: true`) before downloading

---

## 🔐 Required Environment Variables

```bash
OPENAI_API_KEY=sk-proj-...        # Required
MONGODB_URI=mongodb+srv://...     # Required
```

---

## 📈 Performance & Timing

| Operation | Average Time |
|----------|--------------|
| Initialization | 20s (one-time) |
| Processing 1 row | 15-20s |
| Processing 3 rows | 40-60s |
| Processing 10 rows | 2-3 min |
| Review (per suggestion) | Instant (human decision time) |

**Bottlenecks:**
1. OpenAI API calls (LLM evaluation) - ~10-15s per row
2. Vector search - ~2-5s per row
3. 20s initialization wait (vector index stabilization)

---

## 📦 Response Examples

### Start Workflow Response
```json
{
  "status": "success",
  "session_id": "map_1708387456_abc123",
  "workflow_status": "initializing",
  "message": "Workflow started. Connect to /api/v1/session/{session_id}/progress-stream for real-time updates."
}
```

### Suggestion Response
```json
{
  "done": false,
  "index": 0,
  "total": 3,
  "suggestion": {
    "csv_table_name": "store_master",
    "csv_column_name": "district_cd",
    "csv_column_description": "District code",
    "llm_candidates": [
      {
        "term": "DISTRICT",
        "table_name": "store_info",
        "score": 90,
        "reason": "High semantic similarity"
      }
    ]
  }
}
```

### Completion Response
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

## 🔗 Additional Resources

- **Full API Documentation:** `API_INTEGRATION_GUIDE.md`
- **Architecture Details:** `ARCHITECTURE.md`
- **Frontend Reference:** `frontend/index.html`
- **Swagger UI:** http://localhost:8000/docs
- **Health Check:** http://localhost:8000/health

---

**Version:** 1.0.0  
**Last Updated:** February 19, 2026
