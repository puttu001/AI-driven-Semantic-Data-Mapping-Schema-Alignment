"""
JSON utility functions for cleaning and parsing LLM responses.

This module centralizes JSON parsing logic that was previously duplicated
across api_client.py, workflow.py, and core/llm.py.
"""

import json
from typing import Any, Dict


def clean_json_markdown(content: str) -> str:
    """
    Remove markdown code fences from JSON responses.

    Handles responses that may be wrapped in:
    - ```json ... ```
    - ``` ... ```

    Args:
        content: Raw string content potentially wrapped in markdown

    Returns:
        Cleaned string content without markdown fences
    """
    cleaned_content = content.strip()

    # Remove opening fence
    if cleaned_content.startswith("```json"):
        cleaned_content = cleaned_content[7:]
    elif cleaned_content.startswith("```"):
        cleaned_content = cleaned_content[3:]

    # Remove closing fence
    if cleaned_content.endswith("```"):
        cleaned_content = cleaned_content[:-3]

    return cleaned_content.strip()


def parse_json_with_cleanup(content: str) -> Dict[str, Any]:
    """
    Parse JSON content after cleaning markdown fences.

    Args:
        content: Raw JSON string potentially wrapped in markdown

    Returns:
        Parsed JSON as a dictionary

    Raises:
        json.JSONDecodeError: If content is not valid JSON after cleaning
    """
    cleaned = clean_json_markdown(content)
    return json.loads(cleaned)
