# System Architecture - Generalized CDM Mapping

## Overall Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         USER OPTIONS                             │
├─────────────────────────────────────────────────────────────────┤
│  1. Web UI Upload    2. Manual File Drop    3. Interactive CLI  │
│  (Browser Button)    (artifacts folder)     (python main.py)    │
└──────────┬───────────────────┬────────────────────┬─────────────┘
           │                   │                    │
           ▼                   ▼                    ▼
     ┌─────────┐         ┌─────────┐         ┌──────────┐
     │ FastAPI │         │  Batch  │         │  Main    │
     │ Service │         │ Script  │         │  Script  │
     └────┬────┘         └────┬────┘         └────┬─────┘
          │                   │                    │
          └───────────────────┴────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │   artifacts/       │
                    │   ├── CDM_data/    │
                    │   └── mapping_data/│
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │ File Operations    │
                    │ - save_uploaded    │
                    │ - get_latest       │
                    │ - cleanup_old      │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │ Data Processing    │
                    │ - Load CSVs        │
                    │ - Build Glossary   │
                    │ - Create Vectors   │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │ MongoDB Atlas      │
                    │ Vector Collections │
                    │ - cdm_glossary     │
                    │ - csv_data         │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │ Workflow Engine    │
                    │ (LangGraph)        │
                    └─────────┬──────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
     ┌────────────────┐            ┌──────────────────┐
     │ Batch Mode     │            │ Interactive Mode │
     │ - Auto Process │            │ - Human Review   │
     │ - No Prompts   │            │ - Accept/Reject  │
     └────────┬───────┘            └────────┬─────────┘
              │                              │
              └──────────────┬───────────────┘
                             │
                   ┌─────────▼──────────┐
                   │ LLM Processing     │
                   │ - Proposer Agent   │
                   │ - Challenger Agent │
                   │ - Scoring (0-100)  │
                   └─────────┬──────────┘
                             │
                   ┌─────────▼──────────┐
                   │ Results Storage    │
                   │ - MongoDB          │
                   │ - CSV Files        │
                   └────────────────────┘
```

## Web UI Workflow (New)

```
User Browser                FastAPI Service              MongoDB Atlas
     │                            │                            │
     │  1. Upload CDM CSV         │                            │
     │  2. Upload Mapping CSV     │                            │
     │──────────────────────────>│                            │
     │                            │                            │
     │                            │  3. Save to artifacts/     │
     │                            │     - CDM_data/            │
     │                            │     - mapping_data/        │
     │                            │                            │
     │                            │  4. Create embeddings      │
     │                            │     (OpenAI API)           │
     │                            │                            │
     │                            │  5. Create collections     │
     │                            │────────────────────────────>
     │                            │                            │
     │                            │  6. Vector search          │
     │                            │<───────────────────────────│
     │                            │                            │
     │                            │  7. LLM evaluation         │
     │                            │     (Proposer + Challenger)│
     │                            │                            │
     │                            │  8. Save results           │
     │                            │────────────────────────────>
     │                            │                            │
     │  9. JSON Response          │                            │
     │<──────────────────────────│                            │
     │  {                         │                            │
     │    "status": "success",    │                            │
     │    "suggestions": 150,     │                            │
     │    "saved": 150            │                            │
     │  }                         │                            │
```

## Artifacts Folder Structure

```
artifacts/
│
├── CDM_data/
│   ├── MyGlossary_20260127_143052.csv    (Latest upload)
│   ├── MyGlossary_20260126_091523.csv    (Previous)
│   ├── OldGlossary_20260125_154820.csv   (Old - will be cleaned)
│   └── ...                                (Keeps last 5)
│
├── mapping_data/
│   ├── AppSchema_20260127_143055.csv     (Latest upload)
│   ├── AppSchema_20260126_091530.csv     (Previous)
│   └── ...                                (Keeps last 5)
│
├── .gitignore                             (Excludes CSV from git)
└── README.md                              (Documentation)
```

## Processing Modes Comparison

```
┌───────────────────┬──────────────┬───────────────┬─────────────┐
│     Feature       │   Web UI     │  Batch Script │ Interactive │
├───────────────────┼──────────────┼───────────────┼─────────────┤
│ User Input        │ File Upload  │  File Drop    │   CLI       │
│ Interaction       │    None      │     None      │   Per Term  │
│ Processing Time   │   2-5 min    │    2-5 min    │  Variable   │
│ User Skill Needed │    Low       │    Medium     │    High     │
│ Automation        │    Yes       │     Yes       │     No      │
│ Output Format     │    JSON      │   MongoDB     │  CSV+Mongo  │
│ Best For          │  Self-Service│   Scheduled   │   Review    │
└───────────────────┴──────────────┴───────────────┴─────────────┘
```

## Data Flow - Batch Processing

```
1. FILE UPLOAD
   ├── Web Form Submission
   ├── save_uploaded_file()
   └── Timestamped filename

2. FILE LOADING
   ├── Read from artifacts/
   ├── pandas.read_csv()
   └── DataFrame validation

3. PREPROCESSING
   ├── build_cdm_glossary_dict()
   ├── build_cdm_terms_list()
   └── create_representations()

4. VECTOR STORE CREATION
   ├── OpenAI Embeddings (3072 dim)
   ├── MongoDB Atlas Collections
   └── Vector Indexes (30s wait)

5. BATCH WORKFLOW
   ├── For each mapping row:
   │   ├── Vector search (top 25)
   │   ├── LLM Proposer (top 10 → top 3)
   │   ├── Challenger validation
   │   └── Filter accepted candidates
   └── No user prompts

6. RESULTS SAVING
   ├── MongoDB (batch_mappings)
   ├── Execution metadata
   └── Timestamped for tracking

7. RESPONSE
   ├── JSON statistics
   ├── File paths
   └── MongoDB execution_id
```

## Column Mapping Configuration

```
config/settings.py
├── CDM Columns (Input)
│   ├── OBJECT_PARENT_COL       → 'Table Name'
│   ├── CDM_TABLE_DESC_COL      → 'Table Description'
│   ├── OBJECT_NAME_COL         → 'Column Name'
│   └── GLOSSARY_DEFINITION_COL → 'Column Definition'
│
└── Mapping Columns (Input)
    ├── CSV_TABLE_NAME_COL      → 'Table name'
    ├── CSV_TABLE_DESC_COL      → 'Table Description'
    ├── CSV_COLUMN_NAME_COL     → 'Column Name'
    └── CSV_COLUMN_DESC_COL     → 'Column Description'

⚙️  To support different CSV formats:
    → Just change these column name mappings
    → No code changes needed
    → Upload any structure
```

## MongoDB Collections

```
cdm_mapping (Database)
│
├── cdm_glossary (Collection)
│   ├── Embedded vectors (3072 dim)
│   ├── Vector index: vector_index
│   └── Metadata: all CDM columns
│
├── csv_data (Collection)
│   ├── Embedded vectors (3072 dim)
│   ├── Vector index: vector_index
│   └── Metadata: all mapping columns
│
├── batch_mappings (Collection)
│   ├── Created by: Web UI uploads
│   ├── Fields: suggestions + metadata
│   └── Indexed by: execution_id
│
└── final_mappings (Collection)
    ├── Created by: Interactive mode
    ├── Fields: accepted mappings
    └── Top 3 candidates only
```

## Deployment Options

```
DEVELOPMENT
┌────────────────────────────────────┐
│  python api\fastapi_service.py     │
│  http://localhost:8000             │
└────────────────────────────────────┘

PRODUCTION
┌────────────────────────────────────┐
│  uvicorn api.fastapi_service:app   │
│    --host 0.0.0.0                  │
│    --port 8000                     │
│    --workers 4                     │
└────────────────────────────────────┘

DOCKER (Future)
┌────────────────────────────────────┐
│  docker-compose up                 │
│  - FastAPI service                 │
│  - Scheduled batch jobs            │
└────────────────────────────────────┘

SCHEDULED BATCH
┌────────────────────────────────────┐
│  Windows Task Scheduler            │
│  Run: python run_batch.py          │
│  Schedule: Daily at 2 AM           │
└────────────────────────────────────┘
```
