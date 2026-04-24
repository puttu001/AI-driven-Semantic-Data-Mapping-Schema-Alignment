import pandas as pd
import os
import re
from dotenv import load_dotenv
import shutil
import time
import traceback
import sqlite3
import textwrap
import json
from typing import List, Dict, TypedDict, Optional, Sequence, Any
from functools import partial

try:
    from langchain_community.vectorstores import Milvus
    from langchain_core.documents import Document
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
    from langchain_core.runnables import RunnableConfig
except ImportError:
    from langchain.vectorstores import Milvus
    from langchain.docstore.document import Document
    from langchain.prompts import ChatPromptTemplate
    from langchain.schema.output_parser import StrOutputParser
    class JsonOutputParser:
        def parse(self, text: str) -> Any:
            try:
                cleaned_text = text.strip()
                if cleaned_text.startswith("```json"):
                    cleaned_text = cleaned_text[len("```json"):].strip()
                if cleaned_text.endswith("```"):
                    cleaned_text = cleaned_text[:-len("```")].strip()
                loaded_json = json.loads(cleaned_text)
                if isinstance(loaded_json, dict) and loaded_json.get("chosen_term") == "null":
                    loaded_json["chosen_term"] = None
                return loaded_json
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse LLM JSON output: '{text}'. Error: {e}")
                if isinstance(text, str) and not text.startswith("{") and not text.startswith("["):
                    if text.lower() not in ["none", "null", "n/a", "```", "```json"]:
                        return {"chosen_term": text.strip(), "reason": "LLM returned non-JSON, using raw output as term.", "confidence": "Low"}
                return {"chosen_term": None, "reason": f"Failed to parse LLM JSON output. Error: {e}", "confidence": "None"}


from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.chat_models import ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.errors import InvalidUpdateError



def challenge_mapping(
    app_col_repr: str,
    proposed_term: str,
    proposed_definition: str,
    proposed_parent: str,
    proposed_entity: str,
    proposed_table_definition: str,
    proposer_reason: str,
    proposer_confidence: str,
    cdm_glossary_dict: Dict,
    llm: ChatOpenAI
) -> Dict:
    """
    Challenger Agent: Validates the proposed mapping.

    Args:
        app_col_repr: Application column representation with table and column details
        proposed_term: Proposed CDM term name
        proposed_definition: CDM term definition
        proposed_parent: CDM parent table name
        proposed_entity: CDM entity/concept name
        proposed_table_definition: CDM table definition
        proposer_reason: Proposer LLM's reasoning
        proposer_confidence: Proposer LLM's confidence level
        cdm_glossary_dict: Full CDM glossary dictionary
        llm: ChatOpenAI instance for challenger validation

    Returns:
        Dict with keys: 'verdict' (ACCEPT/REJECT), 'reason', 'confidence_score'
    """
    
    challenger_system_prompt = """You are a helpful Data Mapping Assistant for BFSI systems.
Your role is to verify that proposed CDM mappings are reasonable and make business sense.

**YOUR MISSION:**
Trust the Proposer Agent's recommendations and ACCEPT mappings that are semantically reasonable, even if not perfect. Only REJECT mappings with obvious, critical errors that would cause data quality issues.

**ACCEPTANCE CRITERIA - ACCEPT if ANY of these are true:**

1. **Proposer Confidence is Low or Higher** (score ≥ 40)
   - If the Proposer scored it 40+ and gave reasonable justification, ACCEPT it
   - Trust the Proposer's semantic analysis and scoring
   - Only scores below 30 should be heavily scrutinized

2. **Semantic Similarity Exists**
   - The app column and CDM term describe similar business concepts
   - They could plausibly represent the same data, even if worded differently
   - A business user would understand the connection

3. **Functional Compatibility**
   - The data types are compatible (IDs→IDs, text→text, numbers→numbers, codes→codes)
   - The fields serve a similar business purpose
   - No obvious type mismatch (like mapping a description to an amount)

4. **No Critical Errors**
   - Not mapping completely unrelated concepts (e.g., "Customer Name" → "Loan Amount")
   - Not violating obvious data type rules (text → numeric ID)
   - Not mapping audit/technical fields to business fields

**REJECTION CRITERIA - Only REJECT if ALL of these are true:**

1. **Clear Semantic Mismatch**: The concepts are completely different and unrelated
2. **Data Type Incompatibility**: Fundamentally different data types (text ↔ amount, description ↔ code)
3. **Very Low Proposer Confidence**: The Proposer scored it below 30 AND you confirm the issues
4. **Critical Business Error**: Would cause obvious data quality or regulatory issues

**DECISION PHILOSOPHY:**

✅ **DEFAULT TO ACCEPT** - If in doubt, ACCEPT the mapping
- The Proposer has already done semantic analysis with embeddings
- Most mappings with reasonable scores are good enough
- Perfect is the enemy of good in data mapping

❌ **REJECT SPARINGLY** - Only for clear, obvious errors
- Completely different business concepts
- Obvious data type violations
- Would break downstream processing

**EXAMPLES OF WHAT TO ACCEPT:**
- "Customer_ID" → "PARTY_IDENTIFIER" (different names, same concept)
- "Loan_Amount" → "OUTSTANDING_AMOUNT" (related financial amounts)
- "Account_Status" → "ACCOUNT_STATUS_CODE" (same concept, different granularity)
- "Branch_Code" → "BRANCH_IDENTIFIER" (identifiers with different patterns)
- Any mapping where Proposer gave a reasonable explanation with score ≥ 60

**EXAMPLES OF WHAT TO REJECT:**
- "Customer_Name" → "LOAN_AMOUNT" (completely different concepts)
- "Description_Text" → "TRANSACTION_AMOUNT" (text to numeric amount)
- "Email_Address" → "ACCOUNT_BALANCE" (unrelated data)
- Technical audit fields → Business data fields (when not appropriate)

**CONFIDENCE SCORING:**
- High (0.8-1.0): Strong match, clear alignment
- Medium (0.7-0.79): Good match, acceptable
- Low (0.5-0.0.69): Questionable but acceptable if other criteria met
- Very Low (0.0-0.49): Clear issues, should reject

**OUTPUT FORMAT (JSON):**
{{
    "verdict": "ACCEPT or REJECT",
    "reason": "Brief explanation - focus on why ACCEPTED or what critical issue caused REJECT",
    "confidence_score": 0.0-1.0,
    "critical_issues": ["Only list CRITICAL issues that justify rejection"],
    "warnings": ["Minor concerns that don't prevent acceptance"]
}}

**REMEMBER:** 
- When in doubt → ACCEPT
- Trust the Proposer's scores ≥ 70
- Only REJECT obvious errors
- Most mappings should be ACCEPTED
"""

    challenger_human_prompt = """**PROPOSED MAPPING TO VALIDATE:**

Application Column Details:
{app_details}

Proposed CDM Term Details:
- CDM Column Name: {cdm_term}
- CDM Column Definition: {cdm_definition}
- CDM Parent Table: {cdm_parent}
- CDM Entity/Concept: {cdm_entity}
- CDM Table Definition: {cdm_table_definition}

Proposer's Reasoning: {proposer_reason}
Proposer's Confidence: {proposer_confidence}

**YOUR TASK:**
Validate this mapping with a bias toward ACCEPTANCE. The Proposer has already done semantic analysis.
✅ ACCEPT if: Proposer confidence ≥ 50 OR concepts are reasonably similar OR no critical errors
❌ REJECT only if: Obvious critical error that would break data quality

Remember: Default to ACCEPT. Only REJECT for clear, critical issues.
"""

    prompt = ChatPromptTemplate.from_messages([
        ("system", challenger_system_prompt),
        ("human", challenger_human_prompt)
    ])
    
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    
    try:
        result = chain.invoke({
            "app_details": app_col_repr,
            "cdm_term": proposed_term,
            "cdm_definition": proposed_definition,
            "cdm_parent": proposed_parent,
            "cdm_entity": proposed_entity,
            "cdm_table_definition": proposed_table_definition,
            "proposer_reason": proposer_reason,
            "proposer_confidence": proposer_confidence
        })
        
        # Validate and normalize the result
        if not isinstance(result, dict):
            return {
                "verdict": "REJECT",
                "reason": "Challenger agent returned invalid format",
                "confidence_score": 0.0,
                "critical_issues": ["Invalid response format"],
                "warnings": []
            }
        
        # Ensure verdict is valid
        verdict = result.get("verdict", "REJECT").upper()
        if verdict not in ["ACCEPT", "REJECT"]:
            verdict = "REJECT"
            result["reason"] = f"Invalid verdict '{result.get('verdict')}'. Defaulting to REJECT. " + result.get("reason", "")
        
        result["verdict"] = verdict
        result["confidence_score"] = float(result.get("confidence_score", 0.0))
        result["critical_issues"] = result.get("critical_issues", [])
        result["warnings"] = result.get("warnings", [])
        
        return result
        
    except Exception as e:
        print(f"    ERROR in challenger agent: {e}")
        traceback.print_exc()
        return {
            "verdict": "REJECT",
            "reason": f"Challenger agent error: {str(e)}",
            "confidence_score": 0.0,
            "critical_issues": [f"Exception during validation: {str(e)}"],
            "warnings": []
        }