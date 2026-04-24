"""
Term Recommender Module
Core logic for generating new CDM term recommendations using LLM
"""

import json
from typing import Dict, Optional, List

from api.api_client import call_llm_for_new_term_via_api
from .prompts import get_new_term_recommendation_system_prompt, get_new_term_recommendation_human_prompt


def recommend_new_term(
    csv_term_details: Dict,
    existing_cdm_parents: List[str],
    rejection_reason: str,
    llm: Optional[Dict] = None
) -> Optional[Dict]:
    """
    Generate a new CDM term recommendation for a rejected application column.

    Args:
        csv_term_details: Dictionary with csv_table_name, csv_table_description,
                         csv_column_name, csv_column_description
        existing_cdm_parents: List of existing CDM parent entities
        rejection_reason: Why the term was rejected
        llm: LLM instance configuration (FastAPI)

    Returns:
        Dictionary with recommendation details or None if failed:
        {
            'recommended_column_name': str,
            'recommended_parent': str,
            'is_new_parent': bool,
            'reasoning': str,
            'confidence_score': float,
            'definition_suggestion': str
        }
    """
    if not llm:
        print("⚠️  LLM not available for new term recommendation")
        return None

    try:
        # Build prompts
        system_prompt = get_new_term_recommendation_system_prompt()
        human_prompt = get_new_term_recommendation_human_prompt(
            csv_term_details=csv_term_details,
            existing_cdm_parents=existing_cdm_parents,
            rejection_reason=rejection_reason
        )

        print(f"\n🤖 Requesting new term recommendation from LLM...")

        # Call LLM via FastAPI
        response = call_llm_for_new_term_via_api(
            system_prompt=system_prompt,
            human_prompt=human_prompt
        )

        if not response:
            print("❌ Failed to get LLM response")
            return None

        # Parse response
        try:
            recommendation = json.loads(response)

            # Validate required fields
            required_fields = [
                'recommended_column_name',
                'recommended_parent',
                'is_new_parent',
                'reasoning',
                'confidence_score'
            ]

            if not all(field in recommendation for field in required_fields):
                print(f"⚠️  LLM response missing required fields")
                return None

            print(f"✅ Received recommendation: {recommendation['recommended_column_name']}")
            print(f"   Parent: {recommendation['recommended_parent']} (New: {recommendation['is_new_parent']})")
            print(f"   Confidence: {recommendation['confidence_score']:.1f}")

            return recommendation

        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse LLM response as JSON: {e}")
            print(f"Response: {response[:200]}...")
            return None

    except Exception as e:
        print(f"❌ Error during new term recommendation: {e}")
        import traceback
        traceback.print_exc()
        return None


def format_recommendation_for_display(recommendation: Dict) -> str:
    """
    Format a recommendation dictionary for user display.

    Args:
        recommendation: Recommendation dictionary

    Returns:
        Formatted string for display
    """
    if not recommendation:
        return "No recommendation available"

    parent_type = "NEW" if recommendation.get('is_new_parent') else "EXISTING"

    output = f"\n{'='*80}\n"
    output += f"NEW TERM RECOMMENDATION\n"
    output += f"{'='*80}\n"
    output += f"Recommended Column: {recommendation.get('recommended_column_name', 'N/A')}\n"
    output += f"Recommended Parent: {recommendation.get('recommended_parent', 'N/A')} ({parent_type})\n"
    output += f"Confidence Score: {recommendation.get('confidence_score', 0):.1f}/100\n"
    output += f"\nReasoning:\n{recommendation.get('reasoning', 'N/A')}\n"

    if recommendation.get('definition_suggestion'):
        output += f"\nSuggested Definition:\n{recommendation.get('definition_suggestion')}\n"

    output += f"{'='*80}\n"

    return output
