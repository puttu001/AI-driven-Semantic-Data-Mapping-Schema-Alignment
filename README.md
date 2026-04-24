# CDM Mapping Project - Automated Data Model Mapping System

## 📋 Table of Contents
- [Overview](#overview)
- [Quick Start - Web UI](#quick-start---web-ui)
- [Project Purpose](#project-purpose)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Core Components](#core-components)
- [Workflow](#workflow)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Data Files](#data-files)
- [Output Files](#output-files)
- [Technology Stack](#technology-stack)

---

## 📖 Overview

This is an **AI-powered Common Data Model (CDM) Mapping System** designed for the Banking, Financial Services, and Insurance (BFSI) domain. It automates the complex process of mapping application database columns to standardized CDM terms using advanced semantic search, vector embeddings, and LLM-based reasoning.

The system supports **two modes**:
1. **One-Click Web UI**: Upload any CDM and mapping CSV files via browser and process automatically
2. **Interactive CLI**: Review and approve mappings one-by-one with human-in-the-loop validation

---

## 🚀 Quick Start - Web UI

### Upload and Process in One Click

1. **Start the FastAPI service:**
   ```powershell
   python api\fastapi_service.py
   ```

2. **Open your browser:**
   ```
   http://localhost:8000
   ```

3. **Upload your files:**
   - CDM glossary CSV (any format)
   - Mapping/application CSV (any format)
   - Optionally provide API key and MongoDB URI (or use .env)

4. **Click "Run Mapping Process"**
   - Files are saved to `Inputs/CDM_data/` and `Inputs/mapping_data/`
   - Vector stores are created automatically
   - Batch processing runs without user interaction
   - Results are saved to MongoDB

5. **View results:**
   - Check the JSON response for statistics
   - Query MongoDB `batch_mappings` collection
   - Or run interactive review later using saved Inputs


## 🎯 Project Purpose

### Business Problem
Organizations often have:
- Multiple application databases with inconsistent naming conventions
- Need to standardize data definitions across systems
- Requirement to map application schemas to Common Data Models (CDM)
- Manual mapping processes that are time-consuming and error-prone

### Solution
This system provides:
1. **Automated Semantic Matching**: Uses vector embeddings to find similar CDM terms for application columns
2. **AI-Powered Evaluation**: LLM-based reasoning to score and rank mapping candidates
3. **Dual-Agent Validation**: Proposer-Challenger agent pattern for robust mapping validation
4. **Interactive Review**: Human-in-the-loop workflow for final mapping decisions
5. **Comprehensive Tracking**: MongoDB-based persistence and CSV exports for audit trails

---

## ✨ Key Features

### 1. **Vector-Based Semantic Search**
- Uses OpenAI's `text-embedding-3-large` model for semantic embeddings
- MongoDB Atlas vector search for efficient similarity matching
- Context-aware matching considering both column and table descriptions

### 2. **Dual-Agent LLM Architecture**
- **Proposer Agent**: Analyzes candidates and proposes top 3 mappings with detailed reasoning
- **Challenger Agent**: Validates each proposed mapping with rigorous scrutiny
- Only candidates accepted by both agents are presented to users

### 3. **Interactive Workflow**
- LangGraph-based state management
- Human-in-the-loop decision making
- Accept/reject mappings with candidate selection
- Auto-rejection with new term recommendations

### 4. **Multi-Dimensional Scoring**
The LLM evaluates candidates across:
- **Semantic Alignment** (35 points): Column definition matching
- **Table Context Alignment** (35 points): Business domain compatibility
- **Business Logic Fitness** (30 points): Functional type and regulatory compliance
- **Minimum Threshold**: 40 points required for consideration

### 5. **FastAPI Service Layer**
- Decoupled API layer for embeddings, vector search, and LLM operations
- Scalable architecture supporting multiple concurrent requests
- MongoDB Atlas integration for vector storage

### 6. **Comprehensive Persistence**
- Results saved to CSV files (traditional format)
- MongoDB storage for advanced querying and tracking
- Execution metadata for reproducibility

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Main Application                          │
│                        (main.py)                                 │
└───────────────────┬─────────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
┌──────────────┐        ┌─────────────────┐
│  FastAPI     │        │  LangGraph      │
│  Service     │◄───────┤  Workflow       │
│  Layer       │        │  Engine         │
└──────┬───────┘        └────────┬────────┘
       │                         │
       │                         │
       ▼                         ▼
┌──────────────┐        ┌─────────────────┐
│  MongoDB     │        │  Proposer       │
│  Atlas       │        │  Agent (LLM)    │
│  Vector      │        └────────┬────────┘
│  Search      │                 │
└──────────────┘                 ▼
                         ┌─────────────────┐
                         │  Challenger     │
                         │  Agent (LLM)    │
                         └─────────────────┘
```

### Data Flow
1. **Input**: CSV files containing application database schemas
2. **Embedding**: Convert to vectors using OpenAI embeddings
3. **Storage**: Store in MongoDB Atlas with vector indexes
4. **Search**: Vector similarity search for candidate matching
5. **Evaluation**: Proposer LLM scores candidates (top 10 → top 3)
6. **Validation**: Challenger LLM validates each candidate
7. **Review**: Interactive human review and decision
8. **Output**: Final mappings saved to CSV and MongoDB

---

## 📁 Project Structure

```
Data-Mapping/
│
├── main.py                          # Main entry point and orchestration
├── challenger_agent.py              # Challenger agent validation logic
│
├── api/                             # API layer for external services
│   ├── api_client.py               # Client functions for FastAPI calls
│   └── fastapi_service.py          # FastAPI service implementation
│
├── config/                          # Configuration management
│   └── settings.py                 # All configuration constants
│
├── core/                            # Core business logic
│   └── database.py                 # MongoDB vector index management
│
├── prompts/                         # LLM prompt templates
│   └── first_suggestion.py         # Proposer agent prompts
│
├── term_recommendation/             # New term recommendation module
│   ├── __init__.py
│   ├── prompts.py                  # New term recommendation prompts
│   └── term_recommender.py         # Core recommendation logic
│
├── utils/                           # Utility functions
│   ├── data_processing.py          # CSV loading and data cleaning
│   ├── file_operations.py          # File I/O operations
│   └── json_utils.py               # JSON parsing utilities
│
├── workflow/                        # LangGraph workflow components
│   ├── __init__.py
│   ├── enhanced_workflow.py        # Main workflow orchestration
│   ├── llm_operations.py           # LLM interaction logic
│   ├── candidate_processing.py     # Candidate filtering logic
│   ├── display_helpers.py          # UI/display functions
│   └── state_types.py              # TypedDict state definitions
│
└── Data Files/
    ├── Updated_CDM (1).csv         # CDM glossary (1228 terms)
    ├── Mapping - Sheet11.csv       # Application schema file 1
    └── Mapping - Sheet16_.csv      # Application schema file 2
```

---

## 🔧 Core Components

### 1. **main.py** - Application Orchestrator
- Loads environment variables and validates API keys
- Initializes embeddings and LLM services via FastAPI
- Loads and processes CDM and CSV data files
- Creates MongoDB vector stores
- Runs the interactive mapping workflow
- Saves results to CSV and MongoDB
- Optionally runs validation against ground truth

### 2. **api/fastapi_service.py** - FastAPI Gateway
Provides REST endpoints for:
- `/api/v1/embeddings/initialize` - Initialize OpenAI embeddings
- `/api/v1/mongodb/create-collection` - Create vector collections
- `/api/v1/mongodb/search` - Vector similarity search
- `/api/v1/llm/chat` - Generic LLM chat endpoint
- `/api/v1/mappings/save` - Save final mappings to MongoDB

### 3. **workflow/enhanced_workflow.py** - LangGraph Workflow
Implements state machine with nodes:
- **generate_suggestions**: Vector search + LLM evaluation + Challenger validation
- **display_all_suggestions**: Show all candidates to user
- **present_for_review**: Present current term for decision
- **process_feedback**: Handle user accept/reject/recommendation

### 4. **challenger_agent.py** - Validation Agent
Performs rigorous validation using:
- Semantic alignment checks
- Functional type validation
- Business domain rules
- Pattern-based compliance
- Context and qualification matching
- Returns ACCEPT/REJECT verdict with confidence scores

### 5. **term_recommendation/** - New Term Recommender
For rejected/unmapped terms:
- Analyzes application column context
- Evaluates existing CDM structure
- Recommends new CDM term names
- Suggests parent entity (existing or new)
- Provides confidence scores and definitions

### 6. **utils/** - Utility Modules

#### data_processing.py
- Load and clean CSV files
- Build CDM glossary dictionaries
- Create text representations for embedding
- Validate data structures
- Run validation against ground truth

#### file_operations.py
- Save results to CSV with specific formatting
- Save results to MongoDB with metadata
- Format candidate lists for display
- Handle new term recommendations

#### json_utils.py
- Clean JSON from LLM responses
- Remove markdown code fences
- Parse JSON with error handling

---

## 🔄 Workflow

### End-to-End Process

```
1. INITIALIZATION
   ├── Load environment variables (.env)
   ├── Initialize OpenAI embeddings
   ├── Load CDM glossary (1228 terms)
   └── Load application CSV files

2. VECTOR STORE CREATION
   ├── Create CDM vector collection in MongoDB
   ├── Create CSV vector collection in MongoDB
   └── Wait for MongoDB Atlas index creation (30s)

3. MAPPING GENERATION (For each CSV row)
   ├── Create application column representation
   ├── Vector search against CDM (top 25 candidates)
   ├── Proposer LLM evaluates candidates
   │   ├── Scores on semantic alignment (35 pts)
   │   ├── Scores on table context (35 pts)
   │   ├── Scores on business logic (30 pts)
   │   └── Returns top 3 candidates (score >= 40)
   ├── Challenger LLM validates each candidate
   │   ├── Semantic alignment check
   │   ├── Functional type validation
   │   ├── Business domain rules check
   │   ├── Pattern compliance check
   │   └── Returns ACCEPT/REJECT verdict
   └── Only ACCEPTED candidates shown to user

4. INTERACTIVE REVIEW
   ├── Display all suggestions grouped
   ├── For each term:
   │   ├── Show top 3 validated candidates
   │   ├── User chooses: Accept (1/2/3) or Reject
   │   ├── If reject → Optional new term recommendation
   │   └── If auto-rejected → Optional new term recommendation
   └── Build final mappings list

5. OUTPUT GENERATION
   ├── Save to CSV: Final_CDM_Mappings_{timestamp}.csv
   ├── Save to CSV: Unmapped_Columns_{timestamp}.csv
   ├── Save to MongoDB: final_mappings collection
   └── Optional: Validate against ground truth
```

### State Management (LangGraph)

```python
MappingState:
    - csv_data_rows: List of application rows to process
    - initial_suggestions: Generated mapping suggestions
    - current_review_index: Current position in review
    - current_suggestion: Active suggestion being reviewed
    - user_feedback: User's accept/reject decision
    - final_mappings: Accepted mappings
    - rejected_suggestions: Rejected mappings
    - auto_rejected_mappings: Auto-rejected (no candidates)
    - unmapped_columns: Columns without mappings
```

---

## 🚀 Installation & Setup

### Prerequisites
- Python 3.8+
- MongoDB Atlas account (with vector search enabled)
- OpenAI API key

### Step 1: Clone Repository
```bash
cd Natwest-modularise1
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

Required packages:
- `langchain` - LLM orchestration framework
- `langchain-openai` - OpenAI integration
- `langchain-mongodb` - MongoDB vector store
- `langgraph` - Graph-based workflow engine
- `fastapi` - API framework
- `uvicorn` - ASGI server
- `pymongo` - MongoDB driver
- `pandas` - Data manipulation
- `python-dotenv` - Environment variable management
- `requests` - HTTP client

### Step 3: Configure Environment Variables

Create a `.env` file in the project root:

```env
# OpenAI Configuration
OPENAI_API_KEY=sk-your-openai-api-key-here

# MongoDB Atlas Configuration
MONGODB_URI=mongodb+srv://username:password@cluster.xxxxx.mongodb.net/

# Optional: Model Configuration
OPENAI_REASONING_MODEL=gpt-4o-mini
OPENAI_FALLBACK_MODEL=gpt-4o-mini
```

### Step 4: Start FastAPI Service

In a separate terminal:
```bash
cd api
python fastapi_service.py
```

The service will start on `http://localhost:8000`

---

## 💻 Usage

### Running the Main Application

```bash
python main.py
```

### Interactive Review Process

During execution, you'll see:

```
===========================================
ALL SUGGESTIONS - 150 term(s)
===========================================

--- Term 1 of 150 ---
CSV Table: CUSTOMER_ACCOUNTS
CSV Column: cust_id
CSV Column Description: Unique identifier for customer

Candidates (3):
  1. Party Identification Number (Score: 85.0) ✅ VALIDATED
     Definition: A unique reference assigned to a Party...
     Proposer Reasoning: Strong semantic match...
     🛡️ Challenger Validation (Confidence: 0.90)
         Semantic alignment confirmed...

  2. Customer Reference Number (Score: 72.0) ✅ VALIDATED
     ...

  3. Account Holder Identifier (Score: 65.0) ✅ VALIDATED
     ...

Action ([a]ccept or [r]eject): a
Select candidate (1, 2, or 3): 1
✅ Accepted: Party Identification Number
```

### Command Options During Review

- **Accept**: `a` → Then select candidate number (1, 2, or 3)
- **Reject**: `r` → Optionally request new term recommendation
- **Skip**: `Enter` (for auto-rejected terms)

---

## ⚙️ Configuration

### config/settings.py

#### File Paths
```python
CDM_GLOSSARY_FILE = "Updated_CDM (1).csv"
CSV_FILE_1 = "Mapping - Sheet11.csv"
CSV_FILE_2 = "Mapping - Sheet16_.csv"
```

#### Model Configuration
```python
OPENAI_REASONING_MODEL = "gpt-4o-mini"      # Proposer model
OPENAI_FALLBACK_MODEL = "gpt-4o-mini"       # Fallback model
REASONING_MODEL_TEMPERATURE = 0.0
```

#### Search Settings
```python
SIMILARITY_THRESHOLD = 0.2        # Minimum vector similarity
TOP_K_SEARCH_DISPLAY = 25         # Candidates to retrieve
TOP_K_RETRIEVAL_FOR_LLM = 25      # Candidates for LLM evaluation
LLM_MODIFICATION_ENABLED = True   # Enable LLM evaluation
```

#### MongoDB Settings
```python
MONGODB_DB_NAME = "cdm_mapping"
CDM_COLLECTION_NAME = "cdm_glossary"
CSV_COLLECTION_NAME = "csv_data"
VECTOR_INDEX_NAME = "vector_index"
```

#### Column Mappings

**CDM Columns:**
```python
OBJECT_PARENT_COL = 'Table Name'
CDM_TABLE_DESC_COL = 'Table Description'
ENTITY_CONCEPT_COL = 'Parent Name (from Object Parent)'
OBJECT_NAME_COL = 'Column Name'
GLOSSARY_DEFINITION_COL = 'Column Definition'
```

**CSV Columns:**
```python
CSV_TABLE_NAME_COL = 'Table name'
CSV_TABLE_DESC_COL = 'Table Description'
CSV_COLUMN_NAME_COL = 'Column Name'
CSV_COLUMN_DESC_COL = 'Column Description'
```

---

## 📊 Data Files

### Input Files

#### 1. Updated_CDM (1).csv
**Purpose**: Common Data Model glossary (1,228 terms)

**Columns**:
- `S.No`: Serial number
- `Table Name`: CDM parent table (e.g., "Arrangement", "Party")
- `Table Description`: Business definition of the table
- `Parent Name (from Object Parent)`: Entity/concept hierarchy
- `Column Name`: CDM attribute name
- `Column Definition`: Detailed business definition
- `Complete Hierarchy Path`: CDM → Table → Column
- `Comprehensive Description`: Extended explanation

**Sample**:
```csv
S.No,Table Name,Column Name,Column Definition
2,Arrangement,Accrued Interest,"The amount of Interest accrued but not yet applied..."
3,Arrangement,Agreement Identifier,"A code that uniquely identifies an Agreement..."
```

#### 2. Mapping - Sheet11.csv
**Purpose**: Application database schema 1

**Columns**:
- `Table name`: Application table name
- `Table Description`: Purpose of the table
- `Column Name`: Application column name
- `Column Description`: Detailed column description

#### 3. Mapping - Sheet16_.csv
**Purpose**: Application database schema 2 (same structure as Sheet11)

---

### Output Files

#### 1. Final_CDM_Mappings_{timestamp}.csv

**Purpose**: Accepted mappings with full details

**Columns**:
- `Sr. No.`: Serial number
- `App Table name`: Source application table
- `App Table Description`: Source table description
- `App Column Name`: Source column name
- `App Column Description`: Source column description
- `CDM Parent Mapped`: Mapped CDM table
- `CDM Parent Definition`: CDM table definition
- `Top 3 Candidates for Parent Mapping`: Alternative parent options
- `CDM Column Mapped`: Mapped CDM column
- `CDM Column Definition`: CDM column definition
- `Top 3 Candidates`: Alternative column options
- `Reason`: Comprehensive reasoning (Proposer + Challenger)
- `New Recommended Column`: New term suggestions (if applicable)

#### 2. Unmapped_Columns_{timestamp}.csv

**Purpose**: Rejected or unmapped columns

**Columns**:
- `Sr. No.`: Serial number
- `Table name`: Application table
- `Table Description`: Table description
- `Column Name`: Unmapped column
- `Column Description`: Column description
- `Top 3 CDM Column Candidates`: Suggested alternatives
- `Top 3 CDM Parent Candidates`: Suggested parent alternatives
- `Reason`: Why unmapped (user rejected, auto-rejected, etc.)

#### 3. MongoDB Collections

**Collection: final_mappings**
- Stores complete mapping data with execution metadata
- Queryable for analytics and auditing
- Includes candidate scores, reasoning, and timestamps

**Collection: cdm_glossary**
- Vector-embedded CDM terms
- Enables semantic search

**Collection: csv_data**
- Vector-embedded application columns
- Source data for mapping

---

## 🛠️ Technology Stack

### AI & Machine Learning
- **OpenAI Embeddings**: `text-embedding-3-large` (3072 dimensions)
- **OpenAI LLMs**: `gpt-4o-mini` for proposer and challenger agents
- **Vector Search**: MongoDB Atlas vector search (cosine similarity)

### Frameworks & Libraries
- **LangChain**: LLM orchestration and prompt management
- **LangGraph**: State machine workflow engine
- **FastAPI**: High-performance API framework
- **Pydantic**: Data validation and settings management

### Data & Storage
- **MongoDB Atlas**: Vector database and document storage
- **Pandas**: Data manipulation and CSV processing
- **PyMongo**: MongoDB driver

### Development
- **Python 3.8+**: Core programming language
- **python-dotenv**: Environment variable management
- **Uvicorn**: ASGI server for FastAPI

---

## 🎯 Key Algorithms

### 1. Semantic Similarity Scoring
```
Vector Similarity = Cosine Similarity(App Column Embedding, CDM Term Embedding)
Threshold = 0.2 (configurable)
```

### 2. Multi-Dimensional LLM Scoring
```
Total Score = Semantic Alignment (35 pts)
            + Table Context Alignment (35 pts)
            + Business Logic Fitness (30 pts)

Minimum Threshold = 40 points
```

### 3. Dual-Agent Validation
```
Proposer → Generates Top 3 candidates (score >= 40)
         ↓
Challenger → Validates each candidate
           → ACCEPT or REJECT
         ↓
Final Candidates = Only ACCEPTED mappings
```

---

## 📈 Performance Characteristics

- **Embedding Generation**: ~1-2 seconds per 100 terms
- **Vector Search**: ~100-200ms per query (MongoDB Atlas)
- **LLM Evaluation**: ~3-5 seconds per term (Proposer + Challenger)
- **Throughput**: ~100-150 terms/hour (interactive mode)
- **Accuracy**: Dual-agent validation significantly reduces false positives

---

## 🔍 Troubleshooting

### Common Issues

**1. MongoDB Connection Errors**
```
Solution: Verify MONGODB_URI in .env
         Ensure IP whitelist in MongoDB Atlas
         Check network connectivity
```

**2. OpenAI API Errors**
```
Solution: Verify OPENAI_API_KEY in .env
         Check API quota and billing
         Monitor rate limits
```

**3. FastAPI Service Not Running**
```
Solution: Start service: python api/fastapi_service.py
         Check port 8000 availability
         Verify FASTAPI_SERVICE_URL in settings
```

**4. LLM Returns No Candidates**
```
Possible Reasons:
- All candidates scored below 40
- Challenger rejected all candidates
- No semantic matches found (vector search)

Solution: Review similarity threshold
         Check input data quality
         Examine LLM prompts
```

---

## 🚀 Future Enhancements

- **Batch Processing Mode**: Non-interactive bulk mapping
- **Confidence-Based Auto-Acceptance**: Auto-accept high-confidence matches
- **Pattern Learning**: Learn from user decisions to improve suggestions
- **Multi-Model Support**: Support for different LLM providers
- **Advanced Analytics**: Mapping quality metrics and dashboards
- **API Endpoints**: REST API for external integrations

---

## 📝 License

This project is proprietary and confidential. Unauthorized copying or distribution is prohibited.

---


## 📧 Support

For issues or questions, please contact the data architecture team.

---

**Last Updated**: January 22, 2026
**Version**: 1.0
**Status**: Production-Ready
