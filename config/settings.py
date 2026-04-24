"""
Configuration Settings for CDM Mapping Project
All constants, file paths, and column mappings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=True)

# ============================================================================
# INPUTS DIRECTORY PATHS
# ============================================================================

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# INPUTS directories
INPUTS_DIR = PROJECT_ROOT / "Inputs"
CDM_DATA_DIR = INPUTS_DIR / "CDM_data"
MAPPING_DATA_DIR = INPUTS_DIR / "mapping_data"

# Ensure directories exist
CDM_DATA_DIR.mkdir(parents=True, exist_ok=True)
MAPPING_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# OUTPUTS/ARTIFACTS DIRECTORY PATHS
# ============================================================================

# Artifacts directory for all outputs
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
MAPPED_OUTPUT_DIR = ARTIFACTS_DIR / "mapped"
UNMAPPED_OUTPUT_DIR = ARTIFACTS_DIR / "unmapped"
VALIDATION_OUTPUT_DIR = ARTIFACTS_DIR / "validation"

# Ensure artifacts directories exist
MAPPED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
UNMAPPED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
VALIDATION_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# FILE SELECTION MODE
# ============================================================================

# Set to True to use files from Inputs folder (latest uploaded)
USE_INPUTS_FOLDER = True

# ============================================================================
# MODEL CONFIGURATION
# ============================================================================

OPENAI_REASONING_MODEL = "gpt-4o-mini"  # Primary reasoning model
OPENAI_FALLBACK_MODEL = "gpt-4o-mini"   # Fallback model
REASONING_MODEL_TEMPERATURE = 0.0       # o1 models work best with temperature=1
FALLBACK_MODEL_TEMPERATURE = 0.0        # Standard models work best with temperature=0

# ============================================================================
# SEARCH & LLM SETTINGS
# ============================================================================

SIMILARITY_THRESHOLD = 0.2
TOP_K_SEARCH_DISPLAY = 25
LLM_MODIFICATION_ENABLED = True
TOP_K_RETRIEVAL_FOR_LLM = 25

# ============================================================================
# MONGODB SETTINGS
# ============================================================================

MONGODB_DB_NAME = "cdm_mapping"
CDM_COLLECTION_NAME = "cdm_glossary"
CSV_COLLECTION_NAME = "csv_data"
VECTOR_INDEX_NAME = "vector_index"

# ============================================================================
# FASTAPI SETTINGS
# ============================================================================

FASTAPI_SERVICE_URL = "http://localhost:8000"

# ============================================================================
# CDM COLUMN MAPPINGS
# ============================================================================

OBJECT_PARENT_COL = 'Table Name'
CDM_TABLE_DESC_COL = 'Table Description'
ENTITY_CONCEPT_COL = 'Parent Name (from Object Parent)'
OBJECT_NAME_COL = 'Column Name'
GLOSSARY_DEFINITION_COL = 'Column Definition'

# ============================================================================
# CSV COLUMN MAPPINGS
# ============================================================================

CSV_TABLE_NAME_COL = 'Table name'
CSV_TABLE_DESC_COL = 'Table Description'
CSV_COLUMN_NAME_COL = 'Column Name'
CSV_COLUMN_DESC_COL = 'Column Description'

# ============================================================================
# ENVIRONMENT VARIABLES
# ============================================================================

def get_mongodb_uri():
    """Get MongoDB URI from environment"""
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise ValueError("MONGODB_URI not found in environment variables")
    return uri

def get_openai_api_key():
    """Get OpenAI API key from environment"""
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    return key
