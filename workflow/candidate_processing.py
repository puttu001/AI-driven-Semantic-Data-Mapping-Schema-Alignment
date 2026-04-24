"""
Candidate Processing Module
Handles CDM candidate processing and selection logic
"""

from typing import Dict, List, Tuple, Optional

from config.settings import (
    OBJECT_NAME_COL, GLOSSARY_DEFINITION_COL,
    ENTITY_CONCEPT_COL, OBJECT_PARENT_COL,
    CDM_TABLE_DESC_COL
)


def get_cdm_table_definition(table_name: str, cdm_glossary_dict: Dict, cache: Optional[Dict[str, str]] = None) -> str:
    """
    Get CDM table definition from the glossary dictionary.
    Uses cache for O(1) lookup performance when available.

    Args:
        table_name: Name of the CDM table
        cdm_glossary_dict: CDM glossary dictionary
        cache: Pre-built table definition cache for performance (optional)

    Returns:
        Table definition string
    """
    if not table_name:
        return "No table definition available."
    
    # Use cache for O(1) lookup if available
    if cache and table_name in cache:
        return cache[table_name]

    # Fallback to original logic if no cache (backward compatibility)
    # Look for table definition in CDM glossary
    # First, try to find a table-level entry (if exists)
    table_entry = cdm_glossary_dict.get(table_name, {})
    if table_entry:
        # Try to get table description from various possible columns
        table_desc = (
            table_entry.get(CDM_TABLE_DESC_COL, '') or
            table_entry.get(GLOSSARY_DEFINITION_COL, '')
        )
        if table_desc and table_desc.lower() not in ['y', 'n', 'yes', 'no', '']:
            return table_desc

    # Alternative: Look through all entries to find table-level definitions
    for term_name, term_data in cdm_glossary_dict.items():
        if (term_data.get(OBJECT_PARENT_COL) == table_name or
            term_name == table_name):
            # Check if this entry has table description
            table_desc = term_data.get(CDM_TABLE_DESC_COL, '')
            if table_desc and table_desc.lower() not in ['y', 'n', 'yes', 'no', '']:
                return table_desc

    return f"No definition available for table: {table_name}"


def process_candidates(cdm_matches: List[Tuple], cdm_glossary_dict: Dict, table_definition_cache: Optional[Dict[str, str]] = None) -> List[Dict]:
    """
    Process CDM candidates from vector search with table definitions.
    Deduplicates candidates based on (term, table) combination, keeping highest score.

    Args:
        cdm_matches: List of (document, score) tuples from vector search
        cdm_glossary_dict: CDM glossary dictionary
        table_definition_cache: Pre-built table definition cache for performance (optional)

    Returns:
        List of processed candidate dictionaries (deduplicated)
    """
    candidates = []
    seen_candidates = {}  # Track (term, table) -> candidate with highest score
    
    for cdm_doc, score in cdm_matches:
        meta = cdm_doc.metadata

        # Get column definition
        column_definition = meta.get(GLOSSARY_DEFINITION_COL, '')
        if not column_definition or column_definition.lower() in ['y', 'n', 'yes', 'no', '']:
            column_definition = "No textual definition provided."

        # Get table information
        cdm_table_name = meta.get(OBJECT_PARENT_COL, '')
        cdm_table_definition = get_cdm_table_definition(cdm_table_name, cdm_glossary_dict, table_definition_cache)

        term_name = meta.get(OBJECT_NAME_COL)
        candidate_key = (term_name, cdm_table_name)  # Unique key: column + table
        
        candidate = {
            "term": term_name,
            "definition": column_definition,
            "entity": meta.get(ENTITY_CONCEPT_COL),
            "parent": meta.get(OBJECT_PARENT_COL),
            "table": cdm_table_name,
            "table_definition": cdm_table_definition,
            "original_score": float(score),
            "full_metadata": meta
        }
        
        # Keep only the highest scoring duplicate
        if candidate_key not in seen_candidates:
            seen_candidates[candidate_key] = candidate
        elif float(score) > seen_candidates[candidate_key]["original_score"]:
            seen_candidates[candidate_key] = candidate
    
    # Convert back to list, sorted by score descending
    candidates = sorted(seen_candidates.values(), 
                       key=lambda x: x["original_score"], 
                       reverse=True)

    return candidates
