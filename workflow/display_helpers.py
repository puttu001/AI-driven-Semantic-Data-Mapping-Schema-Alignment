"""
Display Helpers Module
Handles all display/UI operations for the CDM mapping workflow
"""

from typing import Dict, List, Optional

from config.settings import (
    GLOSSARY_DEFINITION_COL
)


def display_all_suggestions(
    suggestions: List[Dict],
    cdm_glossary_dict: Optional[Dict] = None
):
    """
    Display all suggestions in a simple list without confidence grouping.

    Args:
        suggestions: List of suggestion dictionaries
        cdm_glossary_dict: CDM glossary dictionary (optional)
    """
    if not suggestions:
        print("No suggestions to display.")
        return

    print("\n" + "="*80)
    print(f"ALL SUGGESTIONS - {len(suggestions)} term(s)")
    print("="*80)

    for idx, sugg in enumerate(suggestions, 1):
        display_single_suggestion(sugg, idx, len(suggestions), cdm_glossary_dict)


def display_single_suggestion(
    suggestion: Dict,
    index: int,
    total: int,
    cdm_glossary_dict: Optional[Dict] = None
):
    """
    Display a single suggestion with its candidates.

    Args:
        suggestion: Suggestion dictionary
        index: Index within the confidence group
        total: Total suggestions in this confidence group
        cdm_glossary_dict: CDM glossary dictionary (optional)
    """
    is_auto_rejected = suggestion.get('is_auto_rejected', False)

    print(f"\n--- Term {index} of {total} ---")

    if is_auto_rejected:
        print("🚫 AUTO-REJECTED")
        print(f"Reason: {suggestion.get('auto_reject_reason', 'Unknown')}")

    print(f"CSV Table: {suggestion.get('csv_table_name', 'N/A')}")
    print(f"CSV Column: {suggestion.get('csv_column_name', 'N/A')}")
    print(f"CSV Column Description: {suggestion.get('csv_column_description', 'N/A')}")

    llm_candidates = suggestion.get('llm_candidates', [])

    if llm_candidates:
        print(f"\nCandidates ({len(llm_candidates)}):")
        for i, cand in enumerate(llm_candidates, 1):
            term = cand.get('term', 'N/A')
            score = cand.get('score', 0)
            reason = cand.get('reason', 'No reason')

            # Get definition from glossary if available
            definition = 'N/A'
            if cdm_glossary_dict and term in cdm_glossary_dict:
                definition = cdm_glossary_dict[term].get(GLOSSARY_DEFINITION_COL, 'N/A')

            # Challenger verdict display (all shown candidates are ACCEPTED by challenger)
            challenger_verdict = cand.get('challenger_verdict', None)
            verdict_status = " ✅ VALIDATED" if challenger_verdict == 'ACCEPT' else ""
            
            # Add table name for clarity
            table_name = cand.get('table_name', '')
            table_info = f" ({table_name})" if table_name else ""

            print(f"\n  {i}. {term}{table_info} (Score: {score:.1f}){verdict_status}")
            print(f"     Definition: {str(definition)[:100]}{'...' if len(str(definition)) > 100 else ''}")
            print(f"     Proposer Reasoning: {reason[:150]}{'...' if len(reason) > 150 else ''}")

            # Display challenger validation details if available
            if challenger_verdict:
                challenger_confidence = cand.get('challenger_confidence', 0.0)
                challenger_reason = cand.get('challenger_reason', '')
                print(f"     🛡️  Challenger Validation (Confidence: {challenger_confidence:.2f})")
                if challenger_reason:
                    print(f"         {challenger_reason[:150]}{'...' if len(challenger_reason) > 150 else ''}")

                # Display any warnings from challenger (even for accepted candidates)
                warnings = cand.get('challenger_warnings', [])
                if warnings:
                    print(f"         ⚡ Warnings: {', '.join(warnings[:2])}")
    else:
        if is_auto_rejected:
            print("\nNo candidates available (auto-rejected - no matches, all rejected by Challenger, or all below threshold)")
        else:
            print("\nNo candidates available (all rejected by Challenger or below score threshold)")

    print("-" * 60)


def display_review_prompt(suggestion: Dict) -> str:
    """
    Display prompt for user review and return the prompt message.

    Args:
        suggestion: Current suggestion being reviewed

    Returns:
        Prompt message string
    """
    llm_candidates = suggestion.get('llm_candidates', [])
    is_auto_rejected = suggestion.get('is_auto_rejected', False)

    if is_auto_rejected:
        print("\nThis term was auto-rejected (no suitable candidates found).")
        print("You can request a new term recommendation or skip.")
        return "Action ([w] for new term recommendation, or press Enter to skip): "

    if not llm_candidates:
        print("\nNo candidates available. Enter 'r' to reject.")
        return "Action ([r]eject only): "

    num_candidates = len(llm_candidates)
    print(f"\nYou have {num_candidates} candidate(s) for this term.")
    print("Actions:")
    print("  [a] - Accept a candidate (you'll choose which one: 1, 2, or 3)")
    print("  [r] - Reject (no mapping for this term)")

    return "Action ([a]ccept or [r]eject): "
