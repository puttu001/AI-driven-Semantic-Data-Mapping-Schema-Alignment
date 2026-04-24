"""
Logical Data Model (LDM) Generator

Takes final CDM mapping results and uses an LLM to infer:
- Logical data types for each attribute
- Primary key identification
- Nullability
- Cross-entity relationships (foreign keys with cardinality)

Produces a structured LDM JSON that feeds into:
- Enhanced Excel export (model_excel_writer.py)
- Physical Data Model generation (pdm_generator.py)
- Frontend ERD rendering
"""

from typing import List, Dict, Optional, Any
from collections import OrderedDict
from datetime import datetime
import json


def _get_system_prompt() -> str:
    """System prompt for LDM inference from mapping results."""
    return """You are a Senior Data Architect specializing in Logical Data Modeling.

Given a set of entities (source tables) and their attributes (columns) from a completed CDM mapping, infer the following properties for a Logical Data Model.

FOR EACH ATTRIBUTE, determine:

1. logical_data_type — Choose EXACTLY ONE from this list:
   - "Identifier" — IDs, keys, codes that uniquely identify records (column names containing: id, key, pk, code, number, no, num when used as identifier)
   - "Text" — Names, descriptions, labels, free-form text (column names containing: name, desc, title, label, comment, note, remarks)
   - "Numeric" — Counts, quantities, whole numbers, scores (column names containing: count, qty, quantity, total, score, rank, level, size)
   - "Currency" — Monetary amounts (column names containing: amount, price, cost, revenue, balance, salary, fee, charge, payment, spend)
   - "Percentage" — Ratios and percentages (column names containing: rate, pct, percent, ratio, margin)
   - "Date" — Calendar dates without time (column names containing: date, dob, birth_date, start_date, end_date)
   - "DateTime" — Timestamps with time component (column names containing: timestamp, created_at, updated_at, datetime, time)
   - "Boolean" — True/false flags (column names containing: flag, is_, has_, indicator, active, enabled, status when binary)
   - "Code" — Short standardized codes, categories, types (column names containing: type, category, status, class, group, segment, tier, cd, district, region, zone)
   - "Email" — Email addresses
   - "Phone" — Phone/contact numbers
   - "Address" — Physical addresses
   - "URL" — Web URLs
   - "Memo" — Long text, comments, descriptions exceeding 500 chars

   RULES:
   - Use column name patterns AND column description AND CDM definition together to decide
   - If ambiguous, prefer the more specific type (e.g., "Currency" over "Numeric" for revenue)
   - "_id" suffix almost always means "Identifier"
   - "total_" prefix with revenue/sales context means "Currency", with count context means "Numeric"

2. is_primary_key — true or false
   - true if the column uniquely identifies rows in this entity
   - Look for: "_id" suffix as the first column, "primary key" in description, "unique identifier" in CDM definition
   - Each entity should have exactly ONE primary key (unless it is a junction table — then composite PK)
   - If no clear PK exists, mark the most likely candidate (usually the first ID column)

3. is_nullable — true (optional) or false (required)
   - Primary keys are ALWAYS false (NOT NULL)
   - Foreign key references are usually false
   - Descriptive fields (name, description) are usually false for core entities
   - Optional metadata (notes, comments, secondary phone) are true
   - Date fields: creation dates are false, end/expiry dates are true
   - Status/type codes are usually false

FOR CROSS-ENTITY RELATIONSHIPS:
4. Analyze ALL entities together to detect relationships:
   - SHARED COLUMN NAMES: If "customer_id" appears in Entity A and Entity B, there is a relationship
   - SEMANTIC REFERENCES: If Entity A has "account_number" and Entity B is "Account" with PK "account_number"
   - NAMING PATTERNS: FK columns often follow the pattern "{referenced_entity}_id" or "{referenced_entity}_code"

   For each relationship provide:
   - source_entity: The entity containing the foreign key
   - source_attribute: The FK column name
   - target_entity: The referenced entity (the one with the PK)
   - target_attribute: The PK column being referenced
   - relationship_type: "1:N" (most common — one parent has many children), "1:1" (extension table), "M:N" (junction table)
   - relationship_name: A descriptive business label (e.g., "Customer places Orders")

   CARDINALITY RULES:
   - If source has the FK and target has the PK → 1:N (target:source — one target has many sources)
   - If both columns are PKs in their respective entities → 1:1
   - Do NOT create self-referencing relationships
   - Do NOT create relationships between the same pair of entities more than once unless through different columns

OUTPUT FORMAT — Return ONLY this JSON structure:
{
  "entities": [
    {
      "entity_name": "original table name exactly as provided",
      "entity_description": "original description exactly as provided",
      "cdm_entity": "mapped CDM parent entity",
      "attributes": [
        {
          "attribute_name": "original column name exactly as provided",
          "attribute_description": "original description exactly as provided",
          "cdm_term": "mapped CDM column name",
          "cdm_definition": "CDM column definition",
          "logical_data_type": "one of the types listed above",
          "is_primary_key": false,
          "is_nullable": true
        }
      ]
    }
  ],
  "relationships": [
    {
      "source_entity": "entity with the FK",
      "source_attribute": "FK column name",
      "target_entity": "entity with the PK",
      "target_attribute": "PK column name",
      "relationship_type": "1:N",
      "relationship_name": "descriptive label"
    }
  ]
}

CRITICAL RULES:
- Return ALL entities and ALL attributes provided — do not skip any
- Every entity MUST have at least one attribute with is_primary_key: true
- Entity names and attribute names must match EXACTLY what was provided (do not rename)
- Return ONLY valid JSON — no markdown, no preamble, no explanation"""


def _get_user_prompt(entities_data: List[Dict]) -> str:
    """
    Build user prompt with all entities and their attributes.

    Args:
        entities_data: List of entity dicts, each with:
            - entity_name, entity_description, cdm_entity
            - attributes: list of dicts with attribute_name, attribute_description, cdm_term, cdm_definition
    """
    lines = ["ENTITIES AND THEIR ATTRIBUTES:\n"]

    for i, entity in enumerate(entities_data, 1):
        lines.append(f"ENTITY {i}: {entity['entity_name']}")
        lines.append(f"  Description: {entity['entity_description']}")
        lines.append(f"  CDM Entity: {entity['cdm_entity']}")
        lines.append(f"  Attributes:")

        for j, attr in enumerate(entity['attributes'], 1):
            cdm_info = f"CDM Term: {attr['cdm_term']}" if attr.get('cdm_term') else "CDM Term: (unmapped)"
            cdm_def = f"CDM Def: {attr['cdm_definition']}" if attr.get('cdm_definition') else ""
            lines.append(
                f"    {j}. {attr['attribute_name']} | "
                f"Description: {attr['attribute_description']} | "
                f"{cdm_info}"
                + (f" | {cdm_def}" if cdm_def else "")
            )

        lines.append("")  # blank line between entities

    lines.append(
        "TASK: Analyze ALL entities and attributes above. "
        "For each attribute, infer its logical_data_type, is_primary_key, and is_nullable. "
        "Then detect all cross-entity relationships by finding shared or semantically related columns. "
        "Return the complete JSON structure as specified."
    )

    return "\n".join(lines)


def _prepare_entities_from_mappings(final_mappings: List[Dict]) -> List[Dict]:
    """
    Group final_mappings by entity (csv_table_name) and structure for LLM prompt.

    Args:
        final_mappings: List of mapping dicts from session, each containing:
            csv_table_name, csv_table_description, csv_column_name,
            csv_column_description, cdm_parent_name, cdm_column_name,
            cdm_column_definition
    Returns:
        List of entity dicts with nested attributes, ordered by entity name.
    """
    entity_map = OrderedDict()

    for mapping in final_mappings:
        entity_name = mapping.get('csv_table_name', '').strip()
        if not entity_name:
            continue

        if entity_name not in entity_map:
            entity_map[entity_name] = {
                'entity_name': entity_name,
                'entity_description': mapping.get('csv_table_description', ''),
                'cdm_entity': mapping.get('cdm_parent_name', ''),
                'attributes': []
            }

        attr_name = mapping.get('csv_column_name', '').strip()
        if not attr_name:
            continue

        # Avoid duplicates
        existing_attrs = {a['attribute_name'] for a in entity_map[entity_name]['attributes']}
        if attr_name in existing_attrs:
            continue

        entity_map[entity_name]['attributes'].append({
            'attribute_name': attr_name,
            'attribute_description': mapping.get('csv_column_description', ''),
            'cdm_term': mapping.get('cdm_column_name', ''),
            'cdm_definition': mapping.get('cdm_column_definition', ''),
        })

    return list(entity_map.values())


def _call_llm_for_ldm(entities_data: List[Dict]) -> Optional[Dict]:
    """
    Call LLM to infer LDM properties for all entities.

    Uses the FastAPI LLM endpoint via mapping_service, with extended max_tokens.

    Returns:
        Parsed JSON dict with "entities" and "relationships", or None on failure.
    """
    from api.services.mapping_service import call_llm_extended_via_api
    from src.data_mapping.utils.json_utils import parse_json_with_cleanup

    system_prompt = _get_system_prompt()
    user_prompt = _get_user_prompt(entities_data)

    print(f"  Sending {len(entities_data)} entities to LLM for LDM inference...")

    raw_response = call_llm_extended_via_api(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        # max_tokens=8000
    )

    if not raw_response:
        print("  LLM returned no response for LDM inference")
        return None

    try:
        parsed = json.loads(raw_response)
    except json.JSONDecodeError:
        parsed = parse_json_with_cleanup(raw_response)

    if not parsed or 'entities' not in parsed:
        print(f"  LLM response missing 'entities' key")
        return None

    return parsed


def _validate_and_fix_ldm(ldm: Dict, original_entities: List[Dict]) -> Dict:
    """
    Validate LLM output against original entities.
    Ensures no entities/attributes are dropped, fixes missing PKs.
    """
    # Build lookup of original entities and their attributes
    original_lookup = {}
    for entity in original_entities:
        attr_names = {a['attribute_name'] for a in entity['attributes']}
        original_lookup[entity['entity_name']] = attr_names

    llm_entities = {e['entity_name']: e for e in ldm.get('entities', [])}

    # Add any missing entities back
    for entity in original_entities:
        name = entity['entity_name']
        if name not in llm_entities:
            # LLM dropped this entity — add it back with defaults
            ldm['entities'].append({
                'entity_name': name,
                'entity_description': entity['entity_description'],
                'cdm_entity': entity['cdm_entity'],
                'attributes': [
                    {
                        'attribute_name': a['attribute_name'],
                        'attribute_description': a['attribute_description'],
                        'cdm_term': a.get('cdm_term', ''),
                        'cdm_definition': a.get('cdm_definition', ''),
                        'logical_data_type': 'Text',
                        'is_primary_key': False,
                        'is_nullable': True,
                    }
                    for a in entity['attributes']
                ]
            })

    # Ensure every entity has at least one PK
    for entity in ldm['entities']:
        has_pk = any(a.get('is_primary_key') for a in entity.get('attributes', []))
        if not has_pk and entity.get('attributes'):
            # Mark the first attribute ending in _id, _key, _code, _no as PK
            for attr in entity['attributes']:
                attr_lower = attr['attribute_name'].lower()
                if any(attr_lower.endswith(suffix) for suffix in ('_id', '_key', '_pk', '_code', '_no')):
                    attr['is_primary_key'] = True
                    attr['is_nullable'] = False
                    break
            else:
                # Fallback: mark first attribute as PK
                entity['attributes'][0]['is_primary_key'] = True
                entity['attributes'][0]['is_nullable'] = False

    # Ensure PKs are not nullable
    for entity in ldm['entities']:
        for attr in entity.get('attributes', []):
            if attr.get('is_primary_key'):
                attr['is_nullable'] = False

    # Ensure relationships reference valid entities
    valid_entity_names = {e['entity_name'] for e in ldm['entities']}
    if 'relationships' in ldm:
        ldm['relationships'] = [
            r for r in ldm['relationships']
            if r.get('source_entity') in valid_entity_names
            and r.get('target_entity') in valid_entity_names
            and r.get('source_entity') != r.get('target_entity')
        ]
    else:
        ldm['relationships'] = []

    return ldm


def generate_logical_data_model(final_mappings: List[Dict]) -> Optional[Dict]:
    """
    Main entry point. Generate a complete Logical Data Model from mapping results.

    Args:
        final_mappings: List of mapping dicts from session['final_mappings'].
            Each dict has keys: csv_table_name, csv_table_description,
            csv_column_name, csv_column_description, cdm_parent_name,
            cdm_column_name, cdm_column_definition, comprehensive_reason, etc.

    Returns:
        Dict with structure:
        {
            "entities": [
                {
                    "entity_name": str,
                    "entity_description": str,
                    "cdm_entity": str,
                    "attributes": [
                        {
                            "attribute_name": str,
                            "attribute_description": str,
                            "cdm_term": str,
                            "cdm_definition": str,
                            "logical_data_type": str,
                            "is_primary_key": bool,
                            "is_nullable": bool
                        }
                    ]
                }
            ],
            "relationships": [
                {
                    "source_entity": str,
                    "source_attribute": str,
                    "target_entity": str,
                    "target_attribute": str,
                    "relationship_type": str,
                    "relationship_name": str
                }
            ],
            "metadata": {
                "total_entities": int,
                "total_attributes": int,
                "total_relationships": int,
                "generated_at": str
            }
        }
        Or None if generation fails.
    """
    if not final_mappings:
        print("  No final mappings provided for LDM generation")
        return None

    print("\n📐 Generating Logical Data Model...")

    # Step 1: Group mappings into entities
    entities_data = _prepare_entities_from_mappings(final_mappings)
    if not entities_data:
        print("  No entities extracted from mappings")
        return None

    total_attrs = sum(len(e['attributes']) for e in entities_data)
    print(f"  Prepared {len(entities_data)} entities with {total_attrs} total attributes")

    # Step 2: Call LLM for inference
    ldm = _call_llm_for_ldm(entities_data)
    if not ldm:
        print("  LDM inference failed — returning fallback model")
        # Build a minimal fallback without LLM
        ldm = {
            'entities': [
                {
                    'entity_name': e['entity_name'],
                    'entity_description': e['entity_description'],
                    'cdm_entity': e['cdm_entity'],
                    'attributes': [
                        {
                            **a,
                            'logical_data_type': 'Text',
                            'is_primary_key': i == 0,
                            'is_nullable': i != 0,
                        }
                        for i, a in enumerate(e['attributes'])
                    ]
                }
                for e in entities_data
            ],
            'relationships': []
        }

    # Step 3: Validate and fix
    ldm = _validate_and_fix_ldm(ldm, entities_data)

    # Step 4: Add metadata
    total_entities = len(ldm['entities'])
    total_attributes = sum(len(e['attributes']) for e in ldm['entities'])
    total_relationships = len(ldm.get('relationships', []))

    ldm['metadata'] = {
        'total_entities': total_entities,
        'total_attributes': total_attributes,
        'total_relationships': total_relationships,
        'generated_at': datetime.now().isoformat(),
    }

    print(f"  LDM complete: {total_entities} entities, {total_attributes} attributes, {total_relationships} relationships")

    return ldm
