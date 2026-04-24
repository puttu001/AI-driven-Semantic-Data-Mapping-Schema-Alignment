"""
LLM Operations Module
Handles all LLM-related operations for the CDM mapping workflow
"""

import re
import json
import requests
import traceback
from typing import Dict, List, Optional, Callable

from config.settings import (
    FASTAPI_SERVICE_URL,
    OBJECT_NAME_COL,
    OBJECT_PARENT_COL,
    GLOSSARY_DEFINITION_COL,
    ENTITY_CONCEPT_COL,
    CDM_TABLE_DESC_COL
)
from utils.json_utils import parse_json_with_cleanup
from prompts import optimized_suggestions
from challenger_agent import challenge_mapping
from langchain_community.chat_models import ChatOpenAI
import os

# Global registry for direct LLM function (set by FastAPI when running internally)
_DIRECT_LLM_FUNCTION: Optional[Callable] = None

def register_direct_llm_function(func: Callable):
    """Register a direct LLM function to avoid HTTP calls when running inside FastAPI"""
    global _DIRECT_LLM_FUNCTION
    _DIRECT_LLM_FUNCTION = func


def evaluate_with_reasoning_llm(
    llm: Optional[Dict],
    app_info: Dict,
    candidates: List[Dict],
    cdm_glossary_dict: Optional[Dict] = None
) -> List[Dict]:
    """
    Use FastAPI generic LLM endpoint to evaluate candidates and return top 3.
    Includes challenger agent validation for each proposed mapping.

    Args:
        llm: LLM configuration dict
        app_info: Application/CSV information
        candidates: List of CDM candidate dictionaries
        cdm_glossary_dict: CDM glossary dictionary for challenger validation

    Returns:
        List of dicts with term, reason, score, and challenger_verdict (filtered >= 40, max 3 candidates)
    """
    if not llm or not llm.get("available"):
        print("LLM not available - skipping evaluation")
        return []

    try:
        # Use prompts from prompts folder
        sys_prompt = optimized_suggestions.get_system_prompt()
        human_prompt = optimized_suggestions.get_human_prompt(app_info, candidates[:10])

        # Use direct LLM function if available (when running inside FastAPI)
        global _DIRECT_LLM_FUNCTION
        if _DIRECT_LLM_FUNCTION:
            try:
                content = _DIRECT_LLM_FUNCTION(
                    system_prompt=sys_prompt,
                    user_prompt=human_prompt,
                    response_format="json"
                )
            except Exception as e:
                print(f"❌ Direct LLM failed: {e}, falling back to HTTP", flush=True)
                # Fall through to HTTP request
                content = None
        else:
            content = None
        
        # Fall back to HTTP request if direct call not available or failed
        if content is None:
            response = requests.post(
                f"{FASTAPI_SERVICE_URL}/api/v1/llm/chat",
                json={
                    "system_prompt": sys_prompt,
                    "user_prompt": human_prompt,
                    "response_format": "json"
                },
                timeout=120
            )

            if response.status_code == 200:
                result = response.json()
                content = result.get("content", "{}")
            else:
                print(f"❌ LLM HTTP request failed: {response.status_code}", flush=True)
                return []

        if content:
            # Parse JSON response using centralized utility
            try:
                parsed = parse_json_with_cleanup(content)
                candidates_list = parsed.get("candidates", [])

                if not candidates_list:
                    print(" LLM returned no candidates (all below threshold)")
                    return []

                # Build term->table lookup map from original candidates (from vector search)
                # This preserves correct table names since glossary has duplicates
                term_to_table_map = {}
                for orig_cand in candidates:
                    term = orig_cand.get('term')
                    table = orig_cand.get('table')
                    if term and table:
                        # Store all occurrences with their tables
                        if term not in term_to_table_map:
                            term_to_table_map[term] = []
                        term_to_table_map[term].append(table)

                # Filter and validate candidates with deduplication
                valid_candidates = []
                seen_candidates = set()  # Track (term, table) to prevent duplicates
                
                for cand in candidates_list[:5]:  # Check up to 5 in case of duplicates
                    term = cand.get("term")
                    score = cand.get("score", 0)
                    reason = cand.get("reason", "No reason provided")
                    
                    # Get table name from original candidates (correct source) not glossary
                    table_name = None
                    if term and term in term_to_table_map:
                        # Use the first (highest scoring) table from vector search
                        table_name = term_to_table_map[term][0]
                    elif cdm_glossary_dict and term:
                        # Fallback to glossary (may be wrong for duplicates)
                        cdm_data = cdm_glossary_dict.get(term, {})
                        table_name = cdm_data.get(OBJECT_PARENT_COL)

                    # Create unique key for deduplication
                    candidate_key = (term, table_name)
                    
                    # Ensure score is numeric and >= 30, and not a duplicate
                    try:
                        score = float(score)
                        if score >= 30 and candidate_key not in seen_candidates:
                            valid_candidates.append({
                                "term": term,
                                "reason": reason,
                                "score": score,
                                "table_name": table_name
                            })
                            seen_candidates.add(candidate_key)
                            
                            # Stop once we have 3 unique candidates
                            if len(valid_candidates) >= 3:
                                break
                    except (ValueError, TypeError):
                        print(f"Warning: Invalid score for candidate {term}: {score}", flush=True)
                        continue

                print(f"\n✨ Proposer AI Results ({len(valid_candidates)} candidates):", flush=True)
                for i, vc in enumerate(valid_candidates[:3], 1):
                    table_info = f" ({vc['table_name']})" if vc.get('table_name') else ""
                    print(f"   {i}. {vc['term']}{table_info:<30} Score: {vc['score']:.0f}/100", flush=True)

                # CHALLENGER AGENT VALIDATION

#########################################################################
############################## Challenger Agent#########################
#########################################################################



                # Validate each proposed candidate using the challenger agent
                if valid_candidates and cdm_glossary_dict:
                    print(f"\n🛡️  Challenger Agent Validation:", flush=True)

                    # Initialize LLM for challenger agent
                    try:
                        challenger_llm = ChatOpenAI(
                            model="gpt-4o",
                            temperature=0.0,
                            openai_api_key=os.getenv("OPENAI_API_KEY")
                        )

                        # Create app column representation for challenger
                        app_col_repr = f"""
Table: {app_info.get('csv_table_name', 'N/A')}
Table Description: {app_info.get('csv_table_description', '')}
Column: {app_info.get('csv_column_name', 'N/A')}
Column Description: {app_info.get('csv_column_description', '')}
                        """.strip()

                        # Validate each candidate and filter by challenger verdict
                        accepted_candidates = []
                        rejected_by_challenger = []

                        for i, cand in enumerate(valid_candidates, 1):
                            term = cand['term']

                            # Get COMPLETE CDM term details from glossary using constants from settings.py
                            cdm_data = cdm_glossary_dict.get(term, {})
                            cdm_definition = cdm_data.get(GLOSSARY_DEFINITION_COL, 'No definition available')
                            cdm_parent = cdm_data.get(OBJECT_PARENT_COL, 'Unknown')
                            cdm_entity = cdm_data.get(ENTITY_CONCEPT_COL, 'Unknown')
                            cdm_table_definition = cdm_data.get(CDM_TABLE_DESC_COL, 'No table description available')

                            # Run challenger validation with COMPLETE CDM details
                            challenger_result = challenge_mapping(
                                app_col_repr=app_col_repr,
                                proposed_term=term,
                                proposed_definition=cdm_definition,
                                proposed_parent=cdm_parent,
                                proposed_entity=cdm_entity,
                                proposed_table_definition=cdm_table_definition,
                                proposer_reason=cand['reason'],
                                proposer_confidence="High" if cand['score'] >= 70 else "Medium" if cand['score'] >= 50 else "Low",
                                cdm_glossary_dict=cdm_glossary_dict,
                                llm=challenger_llm
                            )

                            # Add challenger results to candidate
                            cand['challenger_verdict'] = challenger_result.get('verdict', 'REJECT')
                            cand['challenger_reason'] = challenger_result.get('reason', '')
                            cand['challenger_confidence'] = challenger_result.get('confidence_score', 0.0)
                            cand['challenger_issues'] = challenger_result.get('critical_issues', [])
                            cand['challenger_warnings'] = challenger_result.get('warnings', [])

                            # Only keep candidates ACCEPTED by challenger
                            if cand['challenger_verdict'] == 'ACCEPT':
                                accepted_candidates.append(cand)
                                print(f"   ✅ {term:<30} ACCEPTED (Confidence: {cand['challenger_confidence']:.0%})", flush=True)
                            else:
                                rejected_by_challenger.append(cand)
                                reason = cand['challenger_issues'][0] if cand['challenger_issues'] else 'Low confidence'
                                print(f"   ❌ {term:<30} REJECTED ({reason[:50]})", flush=True)

                        # Check if all candidates were rejected by challenger
                        if not accepted_candidates:
                            print(f"\n⚠️  All rejected - keeping best candidate for human review", flush=True)
                            accepted_candidates = [valid_candidates[0]]
                            accepted_candidates[0]['challenger_verdict'] = 'FORCED_ACCEPT'
                            accepted_candidates[0]['challenger_reason'] = 'All candidates rejected by challenger, but keeping best proposer choice for human review'
                            accepted_candidates[0]['challenger_confidence'] = 0.5

                        valid_candidates = accepted_candidates
                        print(f"📊 Final: {len(accepted_candidates)} ACCEPTED, {len(rejected_by_challenger)} REJECTED", flush=True)

                    except Exception as e:
                        print(f"⚠️  Challenger agent failed: {e}", flush=True)
                        print(f"   Proceeding with proposer results only", flush=True)
                        traceback.print_exc()
                

                ##############challenger agent ends here####################

                return valid_candidates

            except Exception as e:
                print(f"Failed to parse LLM response as JSON: {e}")
                print(f"Raw content: {content[:200]}...")
                return []
        else:
            print(f"❌ LLM API error: {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return []

    except requests.RequestException as e:
        print(f"❌ LLM API connection error: {e}")
        return []

    except Exception as e:
        print(f"❌ LLM API call failed: {e}")
        traceback.print_exc()
        return []
