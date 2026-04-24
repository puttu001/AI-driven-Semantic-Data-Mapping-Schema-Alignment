# CDM Mapping Web Interface

Modern, interactive web UI for the CDM (Common Data Model) Mapping Application.

## Features

✨ **5-Step Workflow**
1. **Upload Files** - Upload CDM and Mapping CSV files
2. **Real-Time Progress** - SSE-powered live progress monitoring
3. **Pre-Review KPIs** - View proposer/challenger metrics
4. **Human Review** - Interactive suggestion review interface
5. **Results & Download** - Final KPIs and CSV downloads

🎨 **Modern Design**
- Beautiful gradient purple theme
- Fully responsive layout
- Smooth animations and transitions
- Professional KPI dashboards

⚡ **Real-Time Features**
- Server-Sent Events (SSE) for live progress updates
- Automatic phase transitions
- Live progress bar with percentage
- Status message updates every second

## How to Use

### 1. Start the FastAPI Server

```bash
# Activate your conda environment
conda activate venv11

# Start the server
python -m uvicorn api.fastapi_new:app --reload
```

### 2. Open the Web UI

Navigate to: **http://localhost:8000**

### 3. Follow the Workflow

**Step 1: Upload Files**
- (Optional) Enter OpenAI API Key
- (Optional) Enter MongoDB URI
- Upload CDM CSV file
- Upload Mapping CSV file
- Click "🚀 Start Mapping Workflow"

**Step 2: Watch Real-Time Progress**
- Automatically displays after upload
- Shows phase indicators (Upload → AI Processing → Ready)
- Live progress bar updates
- Status messages from backend workflow

**Step 3: View Pre-Review Metrics**
- Total Suggestions
- Average Confidence Score
- Acceptance Rate
- Challenger Rejection Rate
- Processing Time
- Unmapped Columns

**Step 4: Review Suggestions**
- Review each mapping suggestion
- View CDM candidates with scores and reasoning
- Make decisions:
  - ✓ Accept Top - Accept highest-scored candidate
  - ✎ Choose Selected - Pick different candidate from list
  - ✗ Reject - Reject all candidates
  - ⏭ Skip - Skip for now

**Step 5: Download Results**
- View final KPIs
- Download Mapped CSV
- Download Unmapped CSV

## Technical Details

### Frontend Stack
- Pure HTML5
- Vanilla JavaScript (no frameworks)
- CSS3 with Flexbox/Grid
- Server-Sent Events (SSE)

### API Integration
- `POST /api/v1/run-mapping-interactive` - Start workflow
- `GET /api/v1/session/{session_id}/progress-stream` - SSE progress
- `GET /api/v1/kpis/session/{session_id}` - Pre-review KPIs
- `POST /api/v1/review/combined` - Submit review decisions
- `GET /api/v1/kpis/session/{session_id}/final` - Final KPIs
- `GET /api/v1/download` - Download results

### Browser Compatibility
- Chrome/Edge 88+
- Firefox 85+
- Safari 14+

## File Structure

```
frontend/
├── index.html          # Main UI file
└── README.md          # This file
```

## Development

The UI is a single HTML file with embedded CSS and JavaScript for easy deployment. All API endpoints are called using the Fetch API.

### SSE Connection
The UI connects to Server-Sent Events to receive real-time progress updates during workflow processing:

```javascript
eventSource = new EventSource(`/api/v1/session/${sessionId}/progress-stream`);
eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    // Update UI with progress
};
```

### Session Management
Each workflow creates a unique session ID that is used throughout the entire process to track progress and maintain state.

## Troubleshooting

**UI not loading?**
- Ensure FastAPI server is running on port 8000
- Check browser console for errors
- Verify `frontend/index.html` exists

**SSE connection issues?**
- Check firewall/proxy settings
- Verify session ID is correct
- Check browser console for connection errors

**File upload fails?**
- Ensure files are valid CSV format
- Check file size limits
- Verify API credentials in .env

## Support

For issues or questions, check the terminal output where FastAPI is running for detailed logs.
