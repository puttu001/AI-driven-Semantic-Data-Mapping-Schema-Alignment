# Inputs Directory

This directory stores uploaded CDM and mapping CSV files for processing.

## Structure

```
Inputs/
├── CDM_data/          # CDM glossary files uploaded via web UI
└── mapping_data/      # Mapping/application CSV files uploaded via web UI
```

## File Naming

Uploaded files are automatically timestamped to prevent conflicts:
- Format: `{original_name}_{YYYYMMDD_HHMMSS}.csv`
- Example: `Updated_CDM_20260127_143052.csv`

## Automatic Cleanup

The system automatically keeps only the 5 most recent files in each directory (configurable).

## Usage

Files in these directories are automatically used by the batch processing workflow when you:
1. Upload files via the web UI at `http://localhost:8000`
2. Click "Run Mapping Process"
3. The system processes the uploaded files and saves results to MongoDB

## Accessing Files

You can retrieve the latest uploaded files programmatically:

```python
from utils.file_operations import get_latest_cdm_file, get_latest_mapping_file

cdm_file = get_latest_cdm_file()
mapping_file = get_latest_mapping_file()
```

## Manual File Placement

You can also manually place CSV files in these directories:
- `CDM_data/` - for CDM glossary files
- `mapping_data/` - for application/mapping files

The system will process the most recently modified files.
