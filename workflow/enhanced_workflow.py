"""
LangGraph Workflow for CDM Mapping
Complete workflow including state management, nodes, and interactive handling
"""

import json
import time
import traceback
from typing import Dict, List, Optional, Tuple, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.documents import Document

from config.settings import (
    CSV_TABLE_NAME_COL, CSV_COLUMN_NAME_COL, CSV_COLUMN_DESC_COL, CSV_TABLE_DESC_COL,
    OBJECT_NAME_COL, ENTITY_CONCEPT_COL,
    GLOSSARY_DEFINITION_COL, OBJECT_PARENT_COL,
    TOP_K_RETRIEVAL_FOR_LLM, LLM_MODIFICATION_ENABLED,
    TOP_K_SEARCH_DISPLAY, MONGODB_DB_NAME, SIMILARITY_THRESHOLD
)
from api.api_client import vector_search_via_api
from utils.data_processing import create_csv_representation

# Import helper modules
from .llm_operations import evaluate_with_reasoning_llm
from .display_helpers import display_all_suggestions, display_single_suggestion, display_review_prompt
from .candidate_processing import process_candidates, get_cdm_table_definition
from .state_types import MappingState
from term_recommendation import recommend_new_term
from term_recommendation.term_recommender import format_recommendation_for_display


class EnhancedInteractiveMappingWorkflow:
    """Enhanced interactive workflow with LangGraph state management and reasoning model"""

    def __init__(self, cdm_collection_info: Dict, csv_collection_info: Dict,
             cdm_glossary_dict: Dict, cdm_terms_list: List[str], llm: Optional[Dict] = None):
        """
        Initialize workflow with FastAPI collection references.
        """
        self.cdm_collection_name = cdm_collection_info['collection_name']
        self.csv_collection_name = csv_collection_info['collection_name']
        self.cdm_db_name = cdm_collection_info.get('db_name', MONGODB_DB_NAME)
        self.csv_db_name = csv_collection_info.get('db_name', MONGODB_DB_NAME)
        self.cdm_glossary_dict = cdm_glossary_dict
        self.cdm_terms_list = cdm_terms_list
        self.llm = llm

        # Check if using FastAPI LLM
        self.is_reasoning_model = llm and isinstance(llm, dict) and llm.get("type") == "fastapi"

        if self.is_reasoning_model:
            print(f"Using FastAPI LLM Service")

        # Extract unique CDM parents for new term recommendations
        self.cdm_parents_list = self._get_unique_cdm_parents()
        
        # Build table definition cache for performance optimization
        self.table_definition_cache = self._build_table_definition_cache()

        # Build the LangGraph workflow
        self.graph_app = self._build_graph()

    def _get_unique_cdm_parents(self) -> List[str]:
        """Extract unique parent entities from CDM glossary"""
        parents = set()
        for term_data in self.cdm_glossary_dict.values():
            parent = term_data.get(OBJECT_PARENT_COL)
            if parent and parent.strip():
                parents.add(parent.strip())
        return sorted(list(parents))
    
    def _build_table_definition_cache(self) -> Dict[str, str]:
        """Build table definition cache for O(1) lookup performance"""
        from config.settings import CDM_TABLE_DESC_COL
        
        cache = {}
        # First pass: direct table entries
        for term_name, term_data in self.cdm_glossary_dict.items():
            table_desc = (
                term_data.get(CDM_TABLE_DESC_COL, '') or
                term_data.get(GLOSSARY_DEFINITION_COL, '')
            )
            if table_desc and table_desc.lower() not in ['y', 'n', 'yes', 'no', '']:
                cache[term_name] = table_desc
        
        # Second pass: find table definitions for parent tables
        for term_name, term_data in self.cdm_glossary_dict.items():
            parent_table = term_data.get(OBJECT_PARENT_COL)
            if parent_table and parent_table not in cache:
                table_desc = term_data.get(CDM_TABLE_DESC_COL, '')
                if table_desc and table_desc.lower() not in ['y', 'n', 'yes', 'no', '']:
                    cache[parent_table] = table_desc
        
        return cache

    def _build_graph(self) -> Any:
        """Build the LangGraph workflow"""
        workflow = StateGraph(MappingState)

        # Add nodes
        workflow.add_node("generate_suggestions", self._generate_suggestions_node)
        workflow.add_node("display_all_suggestions", self._display_all_suggestions_node)
        workflow.add_node("present_for_review", self._present_for_review_node)
        workflow.add_node("process_feedback", self._process_feedback_node)

        # Set entry point
        workflow.set_entry_point("generate_suggestions")

        # Add edges
        workflow.add_edge("generate_suggestions", "display_all_suggestions")
        workflow.add_edge("display_all_suggestions", "present_for_review")

        # After presenting for review, always go to process_feedback to handle user input
        workflow.add_edge("present_for_review", "process_feedback")

        # After processing feedback, check if there are more items to review
        workflow.add_conditional_edges(
            "process_feedback",
            self._decide_after_feedback,
            {"continue_review_cycle": "present_for_review", "end_cycle": END}
        )

        return workflow.compile(
            checkpointer=MemorySaver(),
            interrupt_after=["present_for_review"]
        )

    def _generate_suggestions_node(self, state: MappingState) -> MappingState:
        """Node: Generate suggestions with top 3 LLM-scored candidates"""
        print("\n--- Node: generate_suggestions ---")
        updated_state = state.copy()

        # Skip if suggestions already exist
        if updated_state.get("initial_suggestions") or updated_state.get("unmapped_columns"):
            print("Suggestions already exist. Skipping generation.")
            return updated_state

        csv_rows = updated_state["csv_data_rows"]
        suggestions_list = []
        unmapped_data = []
        auto_rejected_mappings = []  # Track auto-rejections for final_mappings

        total_rows = len(csv_rows)
        print(f"Processing {total_rows} CSV rows for suggestions...", flush=True)
        
        # Get progress callback if provided
        progress_callback = updated_state.get("progress_callback")

        for idx, csv_row in enumerate(csv_rows):
            # Update progress if callback provided
            if progress_callback:
                progress_callback(idx + 1, total_rows)
            # CSV columns only (specified format)
            base_info = {
                'csv_table_name': csv_row.get(CSV_TABLE_NAME_COL),
                'csv_table_description': csv_row.get(CSV_TABLE_DESC_COL),
                'csv_column_name': csv_row.get(CSV_COLUMN_NAME_COL),
                'csv_column_description': csv_row.get(CSV_COLUMN_DESC_COL),
                'app_query_text': ''  # Internal use only
            }
            
            print(f"\n{'='*80}", flush=True)
            print(f"📋 Row {idx+1}/{total_rows} | Table: {base_info['csv_table_name']} | Column: {base_info['csv_column_name']}", flush=True)
            print(f"{'='*80}", flush=True)

            # Create representation for search
            csv_repr = create_csv_representation(csv_row)
            base_info['app_query_text'] = csv_repr
            if not csv_repr.strip():
                print(f"⚠️  Skipping: empty representation", flush=True)
                continue

            try:
                # Search against CDM
                cdm_matches = vector_search_via_api(
                    query_text=csv_repr,
                    collection_name=self.cdm_collection_name,
                    db_name=self.cdm_db_name,
                    top_k=TOP_K_RETRIEVAL_FOR_LLM,
                    return_scores=True
                )
                
                if not cdm_matches:
                    print(f"❌ No CDM candidates found", flush=True)
                    # No CDM candidates found at all
                    error_msg = 'No CDM candidates found'
                    comprehensive_reason = f"Auto-rejected: {error_msg}"

                    # Do NOT add to unmapped_data - will be presented for review
                    # Only gets added to unmapped if user rejects

                    # Add to auto_rejected_mappings for review
                    csv_details = {
                        'csv_table_name': base_info.get('csv_table_name'),
                        'csv_table_description': base_info.get('csv_table_description'),
                        'csv_column_name': base_info.get('csv_column_name'),
                        'csv_column_description': base_info.get('csv_column_description')
                    }

                    auto_rejected_mapping = {
                        **csv_details,
                        'cdm_table_name': None,
                        'cdm_parent_name': None,
                        'cdm_parent_definition': '',
                        'cdm_column_name': None,
                        'cdm_column_definition': None,
                        'final_decision': 'Auto-Rejected (No CDM Matches)',
                        'all_candidates': [],
                        'other_candidates': [],
                        'parent_candidates': [],
                        'comprehensive_reason': comprehensive_reason,
                        'recommended_new_term': None,  # Placeholder for potential recommendation
                        'app_query_text': base_info.get('app_query_text', '')
                    }
                    auto_rejected_mappings.append(auto_rejected_mapping)
                    continue

                # Process candidates using imported function
                candidates = process_candidates(cdm_matches, self.cdm_glossary_dict, self.table_definition_cache)
                print(f"🔍 Found {len(candidates)} CDM candidates from vector search", flush=True)

                # Use LLM to evaluate and get top 3 candidates if enabled
                llm_candidates = []
                if self.llm and LLM_MODIFICATION_ENABLED and candidates:
                    print(f"🤖 Evaluating with AI...", flush=True)
                    try:
                        llm_candidates = evaluate_with_reasoning_llm(
                            self.llm,
                            base_info,
                            candidates,
                            cdm_glossary_dict=self.cdm_glossary_dict
                        )
                    except Exception as e:
                        print(f"❌ LLM evaluation failed: {e}", flush=True)
                        import traceback
                        traceback.print_exc()
                        llm_candidates = []

                if llm_candidates:
                    # Build other_candidates (top 3 column candidates) from llm_candidates
                    other_candidates = [
                        {
                            'term': c['term'],
                            'score': c['score'],
                            'reason': c.get('reason', '')[:70] + "..." if len(c.get('reason', '')) > 70 else c.get('reason', ''),
                            'table_name': c.get('table_name', '')  # Include table name for validation
                        }
                        for c in llm_candidates[:3]
                    ]

                    # Build parent_candidates (top 3 unique parent candidates) from original candidates
                    parent_candidates = []
                    seen_parents = set()
                    for c in candidates:
                        parent = c.get('parent')
                        if parent and parent not in seen_parents:
                            seen_parents.add(parent)
                            parent_candidates.append({
                                'parent_name': parent,
                                'parent_definition': c.get('table_definition', ''),
                                'score': c['original_score']
                            })
                            if len(parent_candidates) >= 3:
                                break

                    # Add suggestion with LLM-scored candidates
                    suggestions_list.append({
                        **base_info,
                        'llm_candidates': llm_candidates,  # List of {term, reason, score}
                        'other_candidates': other_candidates,  # Top 3 for CSV/MongoDB
                        'parent_candidates': parent_candidates  # Top 3 unique parents
                    })
                else:
                    # No candidates met threshold OR all rejected by challenger
                    # Determine if it was score-based or challenger-based rejection
                    if candidates:
                        # Had candidates but none passed (could be score < 30 or challenger rejection)
                        comprehensive_reason = "Auto-rejected: All candidates rejected by Challenger Agent or scored below threshold (< 30)"
                        error_msg = 'All candidates rejected by Challenger Agent or scored below threshold (>= 30)'
                        decision_label = 'Auto-Rejected (Challenger/Score)'
                    else:
                        # No candidates at all
                        comprehensive_reason = "Auto-rejected: All candidates scored below threshold (< 30)"
                        error_msg = 'No candidates met score threshold (>= 30)'
                        decision_label = 'Auto-Rejected (Score < 30)'

                    # Build other_candidates and parent_candidates even for auto-rejected (for display)
                    other_candidates = [
                        {
                            'term': c['term'],
                            'score': c['original_score'],
                            'reason': c.get('definition', '')[:70] + "..." if len(c.get('definition', '')) > 70 else c.get('definition', ''),
                            'table_name': c.get('table', '')  # Include table name for validation
                        }
                        for c in candidates[:3]
                    ]

                    parent_candidates = []
                    seen_parents = set()
                    for c in candidates:
                        parent = c.get('parent')
                        if parent and parent not in seen_parents:
                            seen_parents.add(parent)
                            parent_candidates.append({
                                'parent_name': parent,
                                'parent_definition': c.get('table_definition', ''),
                                'score': c['original_score']
                            })
                            if len(parent_candidates) >= 3:
                                break

                    # Do NOT add to unmapped_data - will be presented for review
                    # Only gets added to unmapped if user rejects

                    # Add to auto_rejected_mappings for review
                    csv_details = {
                        'csv_table_name': base_info.get('csv_table_name'),
                        'csv_table_description': base_info.get('csv_table_description'),
                        'csv_column_name': base_info.get('csv_column_name'),
                        'csv_column_description': base_info.get('csv_column_description')
                    }

                    auto_rejected_mapping = {
                        **csv_details,
                        'cdm_table_name': None,
                        'cdm_parent_name': None,
                        'cdm_parent_definition': '',
                        'cdm_column_name': None,
                        'cdm_column_definition': None,
                        'final_decision': decision_label,
                        'all_candidates': [],
                        'other_candidates': other_candidates,
                        'parent_candidates': parent_candidates,
                        'comprehensive_reason': comprehensive_reason,
                        'recommended_new_term': None,  # Placeholder for potential recommendation
                        'app_query_text': base_info.get('app_query_text', '')
                    }
                    auto_rejected_mappings.append(auto_rejected_mapping)

            except Exception as e:
                print(f"Error processing row {idx}: {e}", flush=True)
                error_msg = str(e)
                comprehensive_reason = f"Auto-rejected: Processing error - {error_msg}"

                # Do NOT add to unmapped_data - will be presented for review
                # Only gets added to unmapped if user rejects

                # Add to auto_rejected_mappings for review
                csv_details = {
                    'csv_table_name': base_info.get('csv_table_name'),
                    'csv_table_description': base_info.get('csv_table_description'),
                    'csv_column_name': base_info.get('csv_column_name'),
                    'csv_column_description': base_info.get('csv_column_description')
                }

                auto_rejected_mapping = {
                    **csv_details,
                    'cdm_table_name': None,
                    'cdm_parent_name': None,
                    'cdm_parent_definition': '',
                    'cdm_column_name': None,
                    'cdm_column_definition': None,
                    'final_decision': 'Auto-Rejected (Processing Error)',
                    'all_candidates': [],
                    'other_candidates': [],
                    'parent_candidates': [],
                    'comprehensive_reason': comprehensive_reason,
                    'recommended_new_term': None,  # Placeholder for potential recommendation
                    'app_query_text': base_info.get('app_query_text', '')
                }
                auto_rejected_mappings.append(auto_rejected_mapping)

        print(f"Generated {len(suggestions_list)} suggestions")
        print(f"Stored {len(unmapped_data)} unmapped columns")
        print(f"Auto-rejected {len(auto_rejected_mappings)} terms (will be reviewed for new term recommendations)")

        # Combine suggestions with auto-rejected for review
        # Auto-rejected terms will be shown during review to allow new term recommendations
        all_suggestions_for_review = suggestions_list + [
            {
                'csv_table_name': ar.get('csv_table_name'),
                'csv_table_description': ar.get('csv_table_description'),
                'csv_column_name': ar.get('csv_column_name'),
                'csv_column_description': ar.get('csv_column_description'),
                'llm_candidates': [],
                'other_candidates': ar.get('other_candidates', []),
                'parent_candidates': ar.get('parent_candidates', []),
                'app_query_text': ar.get('app_query_text', ''),
                'is_auto_rejected': True,
                'auto_reject_reason': ar.get('comprehensive_reason', 'Auto-rejected')
            }
            for ar in auto_rejected_mappings
        ]

        updated_state.update({
            "initial_suggestions": all_suggestions_for_review,  # Include auto-rejected for review
            "unmapped_columns": unmapped_data,
            "current_review_index": 0,
            "final_mappings": [],  # Start with empty, will add during review
            "auto_rejected_mappings": auto_rejected_mappings,  # Store separately for reference
            "rejected_suggestions": [],
            "current_suggestion": None,
            "user_feedback": None
        })

        return updated_state

    def _display_all_suggestions_node(self, state: MappingState) -> MappingState:
        """Node: Display all suggestions without confidence grouping"""
        print("\n--- Node: display_all_suggestions ---")
        updated_state = state.copy()
        suggestions = updated_state["initial_suggestions"]

        if not suggestions:
            print("No suggestions to display.")
            return updated_state

        # Display all suggestions in a simple list
        display_all_suggestions(suggestions, self.cdm_glossary_dict)

        print(f"\n\nReady to review {len(suggestions)} term(s) interactively.")
        print("You will be prompted to accept or reject each term, and select a candidate if accepting.\n")

        return updated_state

    def _present_for_review_node(self, state: MappingState) -> MappingState:
        """Node: Present current suggestion for user decision"""
        print("\n--- Node: present_for_review ---")
        updated_state = state.copy()
        suggestions = updated_state["initial_suggestions"]
        index = updated_state["current_review_index"]

        if not suggestions or index >= len(suggestions):
            print("No more suggestions to review.")
            updated_state["current_suggestion"] = None
            return updated_state

        current_sugg = suggestions[index]

        # Display simplified review header (details already shown in grouped display)
        print("\n" + "="*80)
        print(f"REVIEW FOR TERM: {current_sugg['csv_column_name']}")
        print(f"(Term {index + 1} of {len(suggestions)})")
        print("="*80)

        updated_state["current_suggestion"] = current_sugg
        return updated_state

    def _process_feedback_node(self, state: MappingState) -> MappingState:
        """Node: Process user feedback (accept/reject and candidate selection)"""
        print("\n--- Node: process_feedback ---")
        updated_state = state.copy()
        feedback = updated_state.get("user_feedback") or ""
        print(f"DEBUG: Raw feedback from state: '{feedback[:200] if feedback else 'EMPTY'}'...")
        index = updated_state["current_review_index"]
        sugg = updated_state["current_suggestion"]

        final_mappings = list(updated_state.get("final_mappings", []))
        rejected_suggestions = list(updated_state.get("rejected_suggestions", []))

        if not sugg:
            print("Error: No current suggestion for feedback")
            updated_state.update({
                "current_review_index": index + 1,
                "user_feedback": None
            })
            return updated_state

        # Check if feedback is empty
        if not feedback:
            print("Error: No feedback provided")
            updated_state.update({
                "current_review_index": index + 1,
                "user_feedback": None
            })
            return updated_state

        # CSV columns only (specified format)
        csv_details = {
            'csv_table_name': sugg.get('csv_table_name'),
            'csv_table_description': sugg.get('csv_table_description'),
            'csv_column_name': sugg.get('csv_column_name'),
            'csv_column_description': sugg.get('csv_column_description')
        }

        llm_candidates = sugg.get('llm_candidates', [])

        # Parse feedback format: "a:1", "a:2", "a:3", "r", "r:new_rec:{json}", or "auto_reject"
        # Use maxsplit=2 to split only on first 2 colons (handles JSON with colons inside)
        parts = feedback.split(':', 2)
        action = parts[0].strip().lower()

        # Handle auto-rejected terms
        if action == 'auto_reject':
            print(f"Processing auto-rejected term: '{sugg.get('csv_column_name', 'N/A')}'")

            # Check if new term recommendation was provided
            new_term_recommendation = None
            if len(parts) == 3 and parts[1] == 'new_rec':
                try:
                    new_term_recommendation = json.loads(parts[2])
                    print(f"✅ New term recommendation included for auto-rejected term")
                    print(f"DEBUG: Recommendation data: {new_term_recommendation}")
                except json.JSONDecodeError as e:
                    print(f"⚠️  Failed to parse new term recommendation: {e}")
                    print(f"DEBUG: parts[2] = {parts[2][:100]}...")

            # Get the corresponding auto-rejected mapping from state
            auto_rejected_mappings = updated_state.get('auto_rejected_mappings', [])
            auto_reject_reason = sugg.get('auto_reject_reason', 'Auto-rejected')

            # Find matching auto-rejected mapping by column name
            matching_mapping = None
            csv_column_name = csv_details.get('csv_column_name')
            for mapping in auto_rejected_mappings:
                if mapping.get('csv_column_name') == csv_column_name:
                    matching_mapping = mapping.copy()
                    break

            if matching_mapping:
                # Update with recommendation if available
                matching_mapping['recommended_new_term'] = new_term_recommendation
                final_mappings.append(matching_mapping)
            else:
                # Create new auto-rejected mapping if not found
                final_mapping = {
                    **csv_details,
                    'cdm_table_name': None,
                    'cdm_parent_name': None,
                    'cdm_parent_definition': '',
                    'cdm_column_name': None,
                    'cdm_column_definition': None,
                    'final_decision': 'Auto-Rejected',
                    'all_candidates': [],
                    'comprehensive_reason': auto_reject_reason,
                    'recommended_new_term': new_term_recommendation
                }
                final_mappings.append(final_mapping)

            updated_state.update({
                "final_mappings": final_mappings,
                "current_review_index": index + 1,
                "user_feedback": None
            })

        elif action == 'a' and len(parts) == 2:
            # Accept with candidate selection
            try:
                candidate_num = int(parts[1].strip())
                if 1 <= candidate_num <= len(llm_candidates):
                    chosen_candidate = llm_candidates[candidate_num - 1]
                    chosen_term = chosen_candidate['term']
                    chosen_score = chosen_candidate['score']
                    chosen_reason = chosen_candidate['reason']

                    print(f"Feedback: Accepted candidate #{candidate_num}: '{chosen_term}' (Score: {chosen_score:.1f})")

                    # Get CDM details from glossary
                    cdm_data = self.cdm_glossary_dict.get(chosen_term, {})
                    parent_name = cdm_data.get(OBJECT_PARENT_COL)
                    parent_definition = get_cdm_table_definition(parent_name, self.cdm_glossary_dict, self.table_definition_cache) if parent_name else ''

                    final_mapping = {
                        **csv_details,
                        'cdm_table_name': cdm_data.get(ENTITY_CONCEPT_COL),
                        'cdm_parent_name': parent_name,
                        'cdm_parent_definition': parent_definition,
                        'cdm_column_name': chosen_term,
                        'cdm_column_definition': cdm_data.get(GLOSSARY_DEFINITION_COL),
                        'llm_score': chosen_score,
                        'llm_reason': chosen_reason,
                        'comprehensive_reason': f"User accepted candidate #{candidate_num}: {chosen_term}. Reason: {chosen_reason}",
                        'final_decision': 'Accepted',
                        'all_candidates': llm_candidates,
                        'other_candidates': sugg.get('other_candidates', []),
                        'parent_candidates': sugg.get('parent_candidates', []),
                        'chosen_candidate_number': candidate_num
                    }
                    final_mappings.append(final_mapping)
                    updated_state.update({
                        "final_mappings": final_mappings,
                        "current_review_index": index + 1,
                        "user_feedback": None
                    })
                else:
                    print(f"Invalid candidate number: {candidate_num}. Skipping.")
                    updated_state.update({
                        "current_review_index": index + 1,
                        "user_feedback": None
                    })
            except ValueError:
                print(f"Invalid feedback format: {feedback}. Skipping.")
                updated_state.update({
                    "current_review_index": index + 1,
                    "user_feedback": None
                })

        elif action == 'r':
            # Reject - check if new term recommendation was requested
            print(f"Feedback: Rejected all candidates for '{sugg['csv_column_name']}'")
            print(f"DEBUG: Full feedback string: {feedback[:200]}...")
            print(f"DEBUG: Number of parts after split: {len(parts)}")
            if len(parts) >= 2:
                print(f"DEBUG: parts[1] = '{parts[1]}'")

            # Build comprehensive reason for rejected mapping
            reason_parts = ["User rejected all candidates"]
            if llm_candidates:
                candidate_list = [f"{c.get('term')} (Score: {c.get('score', 0):.1f})" for c in llm_candidates]
                reason_parts.append(f"Available candidates were: {', '.join(candidate_list)}")

            comprehensive_reason = ". ".join(reason_parts)

            # Check if feedback includes new term recommendation
            new_term_recommendation = None
            if len(parts) == 3 and parts[1] == 'new_rec':
                # Parse the recommendation from feedback
                try:
                    new_term_recommendation = json.loads(parts[2])
                    print(f"✅ New term recommendation included in rejection")
                    print(f"DEBUG: Recommendation data: {new_term_recommendation}")
                except json.JSONDecodeError as e:
                    print(f"⚠️  Failed to parse new term recommendation: {e}")
                    print(f"DEBUG: parts[2] = {parts[2][:100]}...")

            # Store the final mapping structure
            final_mapping = {
                **csv_details,
                'cdm_table_name': None,
                'cdm_parent_name': None,
                'cdm_parent_definition': '',
                'cdm_column_name': None,
                'cdm_column_definition': None,
                'final_decision': 'Rejected',
                'all_candidates': llm_candidates,
                'other_candidates': sugg.get('other_candidates', []),
                'parent_candidates': sugg.get('parent_candidates', []),
                'comprehensive_reason': comprehensive_reason,
                'recommended_new_term': new_term_recommendation  # Include recommendation if available
            }

            print(f"DEBUG: Storing final_mapping with recommended_new_term = {new_term_recommendation is not None}")

            final_mappings.append(final_mapping)
            rejected_suggestions.append(sugg)
            updated_state.update({
                "final_mappings": final_mappings,
                "rejected_suggestions": rejected_suggestions,
                "current_review_index": index + 1,
                "user_feedback": None
            })

        else:
            print(f"Unknown feedback '{feedback}'. Treating as reject.")
            final_mapping = {
                **csv_details,
                'cdm_table_name': None,
                'cdm_parent_name': None,
                'cdm_parent_definition': '',
                'cdm_column_name': None,
                'cdm_column_definition': None,
                'final_decision': 'Rejected',
                'all_candidates': llm_candidates,
                'other_candidates': sugg.get('other_candidates', []),
                'parent_candidates': sugg.get('parent_candidates', [])
            }
            final_mappings.append(final_mapping)
            updated_state.update({
                "final_mappings": final_mappings,
                "current_review_index": index + 1,
                "user_feedback": None
            })

        return updated_state

    def _decide_after_feedback(self, state: MappingState) -> str:
        """Router: Decide next step after processing feedback"""
        index = state.get("current_review_index", 0)
        total_suggestions = len(state.get("initial_suggestions", []))

        if index >= total_suggestions:
            print(f"All {total_suggestions} suggestions reviewed.")
            return "end_cycle"
        else:
            print(f"More suggestions to review ({index}/{total_suggestions}).")
            return "continue_review_cycle"

    def run_interactive_workflow(self) -> Tuple[List[Dict], List[Dict]]:
        """Run the complete interactive workflow using LangGraph with reasoning model"""
        model_type = "-- Reasoning Model Enhanced --" if self.is_reasoning_model else " Standard LLM"
        print(f"\n --- Starting {model_type} Interactive CDM Mapping Workflow ---")

        # Prepare initial state
        initial_state: MappingState = {
            "csv_data_rows": [],
            "cdm_glossary_dict": self.cdm_glossary_dict,
            "cdm_terms_list": self.cdm_terms_list,
            "similarity_threshold": SIMILARITY_THRESHOLD,
            "initial_suggestions": [],
            "unmapped_columns": [],
            "current_review_index": 0,
            "current_suggestion": None,
            "user_feedback": None,
            "final_mappings": [],
            "rejected_suggestions": [],
            "auto_rejected_mappings": [],
            "vector_store_info": {}
        }

        # Load CSV data
        try:
            all_csv_results = vector_search_via_api(
            query_text="data table column",
            collection_name=self.csv_collection_name,
            db_name=self.csv_db_name,
            top_k=1000,
            return_scores=False
        )

            csv_data_rows = []
            for csv_doc in all_csv_results:
                csv_metadata = csv_doc.metadata
                csv_data_rows.append({
                    CSV_TABLE_NAME_COL: csv_metadata.get(CSV_TABLE_NAME_COL, 'N/A'),
                    CSV_COLUMN_NAME_COL: csv_metadata.get(CSV_COLUMN_NAME_COL, 'N/A'),
                    CSV_COLUMN_DESC_COL: csv_metadata.get(CSV_COLUMN_DESC_COL, ''),
                    CSV_TABLE_DESC_COL: csv_metadata.get(CSV_TABLE_DESC_COL, '')
                   })

            initial_state["csv_data_rows"] = csv_data_rows
            print(f"Loaded {len(csv_data_rows)} CSV rows for processing")

        except Exception as e:
            print(f"Error loading CSV data: {e}")
            return [], []

        # Run the LangGraph workflow
        thread_id = f"mapping_thread_{int(time.time())}"
        thread_config = {"configurable": {"thread_id": thread_id}}

        try:
            current_state = None
            loop_count = 0

            # Initialize the stream
            graph_stream = self.graph_app.stream(initial_state, config=thread_config, stream_mode="values")

            while True:
                loop_count += 1
                print(f"\n>>> Workflow Loop {loop_count} <<<")

                # Consume stream events FIRST to advance the workflow
                events_consumed = 0
                try:
                    for event_value in graph_stream:
                        current_state = event_value
                        events_consumed += 1

                    if events_consumed > 0:
                        print(f"Consumed {events_consumed} event(s)")

                except StopIteration:
                    print("Stream segment completed")

                # NOW get the snapshot to check next nodes
                snapshot = self.graph_app.get_state(config=thread_config)
                if snapshot:
                    current_state = snapshot.values
                    next_nodes = snapshot.next or []
                else:
                    print("Could not get snapshot")
                    break

                print(f"Next nodes: {next_nodes}")

                # Check if workflow is complete
                if not next_nodes:
                    print("Workflow completed - no more nodes to execute")
                    break

                # Handle interrupts - check if we need user input
                if "process_feedback" in next_nodes:
                    print("\n--- INTERRUPT: Waiting for user decision ---")
                    current_suggestion = current_state.get("current_suggestion")

                    if not current_suggestion:
                        print("Warning: No current suggestion available, auto-rejecting")
                        user_feedback = 'r'
                    else:
                        is_auto_rejected = current_suggestion.get('is_auto_rejected', False)
                        llm_candidates = current_suggestion.get('llm_candidates', [])

                        if is_auto_rejected:
                            # Auto-rejected term - offer new term recommendation directly
                            prompt_msg = display_review_prompt(current_suggestion)
                            while True:
                                response = input(prompt_msg).strip().lower()

                                if response == 'w' and self.llm:
                                    # User wants new term recommendation for auto-rejected
                                    csv_details = {
                                        'csv_table_name': current_suggestion.get('csv_table_name'),
                                        'csv_table_description': current_suggestion.get('csv_table_description'),
                                        'csv_column_name': current_suggestion.get('csv_column_name'),
                                        'csv_column_description': current_suggestion.get('csv_column_description')
                                    }

                                    rejection_reason = current_suggestion.get('auto_reject_reason', 'Auto-rejected')

                                    recommendation = recommend_new_term(
                                        csv_term_details=csv_details,
                                        existing_cdm_parents=self.cdm_parents_list,
                                        rejection_reason=rejection_reason,
                                        llm=self.llm
                                    )

                                    if recommendation:
                                        # Display recommendation
                                        print(format_recommendation_for_display(recommendation))
                                        # Mark as auto-rejected with recommendation
                                        user_feedback = f'auto_reject:new_rec:{json.dumps(recommendation)}'
                                    else:
                                        print("⚠️  Failed to generate recommendation, proceeding with standard auto-rejection")
                                        user_feedback = 'auto_reject'
                                    break
                                elif response == '' or response in ['skip', 's']:
                                    # User skipped - keep as auto-rejected without recommendation
                                    user_feedback = 'auto_reject'
                                    break
                                else:
                                    print("Invalid input. Enter 'w' for recommendation or press Enter to skip.")

                        elif not llm_candidates:
                            # No candidates, can only reject
                            while True:
                                response = input("Action ([r]eject only): ").strip().lower()
                                if response in ['r', 'reject']:
                                    user_feedback = 'r'
                                    break
                                else:
                                    print("Invalid input. No candidates available, you can only reject.")
                        else:
                            # Has candidates - accept or reject
                            prompt_msg = display_review_prompt(current_suggestion)
                            while True:
                                response = input(prompt_msg).strip().lower()

                                if response in ['r', 'reject']:
                                    user_feedback = 'r'

                                    # After rejection, ask if user wants new term recommendation
                                    print("\n💡 Would you like an AI-generated new term recommendation for this column?")
                                    rec_response = input("Enter 'w' for new term recommendation, or press Enter to skip: ").strip().lower()

                                    if rec_response == 'w' and self.llm:
                                        # Request new term recommendation
                                        csv_details = {
                                            'csv_table_name': current_suggestion.get('csv_table_name'),
                                            'csv_table_description': current_suggestion.get('csv_table_description'),
                                            'csv_column_name': current_suggestion.get('csv_column_name'),
                                            'csv_column_description': current_suggestion.get('csv_column_description')
                                        }

                                        rejection_reason = "User rejected all candidates"
                                        if llm_candidates:
                                            candidate_list = [f"{c.get('term')} (Score: {c.get('score', 0):.1f})" for c in llm_candidates]
                                            rejection_reason += f". Available candidates were: {', '.join(candidate_list)}"

                                        recommendation = recommend_new_term(
                                            csv_term_details=csv_details,
                                            existing_cdm_parents=self.cdm_parents_list,
                                            rejection_reason=rejection_reason,
                                            llm=self.llm
                                        )

                                        if recommendation:
                                            # Display recommendation
                                            print(format_recommendation_for_display(recommendation))

                                            # Store recommendation for later use (will be added to final_mapping)
                                            user_feedback = f'r:new_rec:{json.dumps(recommendation)}'
                                        else:
                                            print("⚠️  Failed to generate recommendation, proceeding with standard rejection")

                                    break
                                elif response in ['a', 'accept']:
                                    # Ask which candidate
                                    while True:
                                        candidate_choice = input(f"Which candidate? (1-{len(llm_candidates)}): ").strip()
                                        try:
                                            candidate_num = int(candidate_choice)
                                            if 1 <= candidate_num <= len(llm_candidates):
                                                user_feedback = f'a:{candidate_num}'
                                                break
                                            else:
                                                print(f"Please enter a number between 1 and {len(llm_candidates)}")
                                        except ValueError:
                                            print("Please enter a valid number")
                                    break
                                else:
                                    print("Invalid input. Please enter 'a' or 'r'.")

                    print(f"Updating state with feedback: {user_feedback}")
                    # Update state and continue from the interrupt
                    self.graph_app.update_state(thread_config, {"user_feedback": user_feedback})

                    # Create new stream for next iteration after updating state
                    graph_stream = self.graph_app.stream(None, config=thread_config, stream_mode="values")
                else:
                    # No interrupt handling needed, create new stream for next iteration
                    graph_stream = self.graph_app.stream(None, config=thread_config, stream_mode="values")

        except KeyboardInterrupt:
            print(f"\n --- User interrupted the {model_type} workflow ---")
        except Exception as e:
            print(f"Error during {model_type} workflow: {e}")
            traceback.print_exc()

        # Extract final results
        if current_state:
            final_mappings = current_state.get("final_mappings", [])
            unmapped_columns = current_state.get("unmapped_columns", [])
        else:
            final_mappings = []
            unmapped_columns = []

        print(f"\n {model_type} Workflow completed!")
        print(f"Final mappings: {len(final_mappings)}")
        print(f"Unmapped columns: {len(unmapped_columns)}")

        return final_mappings, unmapped_columns

    def run_batch_process(self, csv_data_rows: List[Dict], progress_callback=None) -> Dict:
        """
        Run batch processing (non-interactive) for uploaded CSV data.
        
        This generates suggestions for all CSV rows without user interaction.
        Used by API endpoints to process uploaded files in one click.
        
        Args:
            csv_data_rows: List of CSV row dictionaries with columns defined in settings
            progress_callback: Optional callback function(current, total) to report progress
        
        Returns:
            Dict with 'suggestions', 'unmapped', and 'auto_rejected' lists
        """
        print("\n--- Running batch processing (non-interactive) ---", flush=True)
        
        # Prepare initial state
        initial_state: MappingState = {
            "csv_data_rows": csv_data_rows,
            "cdm_glossary_dict": self.cdm_glossary_dict,
            "cdm_terms_list": self.cdm_terms_list,
            "similarity_threshold": SIMILARITY_THRESHOLD,
            "initial_suggestions": [],
            "progress_callback": progress_callback,
            "unmapped_columns": [],
            "current_review_index": 0,
            "current_suggestion": None,
            "user_feedback": None,
            "final_mappings": [],
            "rejected_suggestions": [],
            "auto_rejected_mappings": [],
            "vector_store_info": {}
        }
        
        try:
            # Run the generation node directly (no user interaction)
            updated_state = self._generate_suggestions_node(initial_state)
            
            suggestions = updated_state.get("initial_suggestions", [])
            unmapped = updated_state.get("unmapped_columns", [])
            auto_rejected = updated_state.get("auto_rejected_mappings", [])
            
            print(f"✅ Batch processing completed:", flush=True)
            print(f"   - Suggestions: {len(suggestions)}", flush=True)
            print(f"   - Unmapped: {len(unmapped)}", flush=True)
            print(f"   - Auto-rejected: {len(auto_rejected)}", flush=True)
            
            return {
                "suggestions": suggestions,
                "unmapped": unmapped,
                "auto_rejected": auto_rejected
            }
            
        except Exception as e:
            print(f"❌ Error during batch processing: {e}", flush=True)
            traceback.print_exc()
            return {
                "suggestions": [],
                "unmapped": [],
                "auto_rejected": [],
                "error": str(e)
            }
