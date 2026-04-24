"""
State Types Module
Defines TypedDict classes for LangGraph state management
"""

from typing import TypedDict, List, Dict, Optional, Any, Callable


class MappingState(TypedDict, total=False):
    """State definition for the CDM mapping workflow"""
    csv_data_rows: List[Dict[str, Any]]
    cdm_glossary_dict: Dict[str, Dict[str, Any]]
    cdm_terms_list: List[str]
    similarity_threshold: float
    initial_suggestions: List[Dict]
    unmapped_columns: List[Dict]
    current_review_index: int
    current_suggestion: Optional[Dict]
    user_feedback: Optional[str]
    final_mappings: List[Dict]
    rejected_suggestions: List[Dict]
    auto_rejected_mappings: List[Dict]  # Store auto-rejected mappings separately
    # Vector stores (not serializable, passed separately)
    vector_store_info: Dict[str, Any]
    # Progress tracking callback
    progress_callback: Optional[Callable[[int, int], None]]
