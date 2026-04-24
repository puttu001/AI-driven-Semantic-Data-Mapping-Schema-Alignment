"""
Data Processing Utilities
Handles CSV loading, data cleaning, text representation creation, and validation
"""

import os
import pandas as pd
from typing import Dict, List

from config.settings import (
    OBJECT_NAME_COL, OBJECT_PARENT_COL, GLOSSARY_DEFINITION_COL,
    CSV_TABLE_NAME_COL, CSV_TABLE_DESC_COL, CSV_COLUMN_NAME_COL, CSV_COLUMN_DESC_COL,
    CDM_TABLE_DESC_COL, ENTITY_CONCEPT_COL
)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize column names"""
    df.columns = [col.strip() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def clean_column_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data within columns"""
    df = df.fillna('')
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
    return df


def load_and_clean_csv_file(file_path: str) -> pd.DataFrame:
    """Load and clean single CSV file"""
    print(f"Loading CSV file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        df = clean_column_names(df)
        df = clean_column_data(df)
        print(f"Loaded {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        raise Exception(f"Error loading CSV file {file_path}: {e}")


def load_and_combine_csv_files(file_paths: List[str], limit_rows: int = None) -> pd.DataFrame:
    """Load and combine multiple CSV files"""
    combined_df = pd.DataFrame()
    
    for file_path in file_paths:
        if os.path.exists(file_path):
            df = load_and_clean_csv_file(file_path)
            df['source_file'] = os.path.basename(file_path)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        else:
            print(f"WARNING: File not found: {file_path}")
    
    if combined_df.empty:
        raise Exception("No valid CSV files found or all files are empty")
    
    print(f"Combined dataset contains {len(combined_df)} total rows")
    
    # Limit rows if specified (useful for testing)
    if limit_rows:
        combined_df = combined_df.iloc[:limit_rows]
        print(f"Limited to {len(combined_df)} rows for processing")
    
    return combined_df


def create_cdm_representation(row: Dict) -> str:
    """Create a comprehensive text representation of CDM data for embedding"""
    parts = []

    # Parent (Table Name)
    parent_name = str(row.get(OBJECT_PARENT_COL, '')).strip()
    if parent_name:
        parts.append(f"Table: {parent_name}")

    # Parent Definition (Table Description)
    table_desc = str(row.get(CDM_TABLE_DESC_COL, '')).strip()
    if table_desc:
        parts.append(f"Table Description: {table_desc}")

    # Entity
    entity = str(row.get(ENTITY_CONCEPT_COL, '')).strip()
    if entity:
        parts.append(f"Entity: {entity}")

    # Column Name
    object_name = str(row.get(OBJECT_NAME_COL, '')).strip()
    if object_name:
        parts.append(f"Column: {object_name}")

    # Column Definition
    glossary_def = str(row.get(GLOSSARY_DEFINITION_COL, '')).strip()
    if glossary_def and glossary_def.lower() not in ['y', 'n', 'yes', 'no', '']:
        parts.append(f"Column Definition: {glossary_def}")

    return ". ".join(parts).strip()


def create_csv_representation(row: Dict) -> str:
    """Create a comprehensive text representation of CSV data for embedding"""
    parts = []

    # Table Name
    table_name = str(row.get(CSV_TABLE_NAME_COL, '')).strip()
    if table_name:
        parts.append(f"Table: {table_name}")

    # Table Description
    table_desc = str(row.get(CSV_TABLE_DESC_COL, '')).strip()
    if table_desc:
        parts.append(f"Table Description: {table_desc}")

    # Column Name
    column_name = str(row.get(CSV_COLUMN_NAME_COL, '')).strip()
    if column_name:
        parts.append(f"Column: {column_name}")

    # Column Description
    column_desc = str(row.get(CSV_COLUMN_DESC_COL, '')).strip()
    if column_desc:
        parts.append(f"Column Description: {column_desc}")

    return ". ".join(parts).strip()


def validate_required_columns(df: pd.DataFrame, required_cols: List[str], data_type: str) -> bool:
    """Validate that required columns exist in the dataframe"""
    if data_type == "CDM":
        # Updated required columns for new CDM structure
        cdm_required_columns = [OBJECT_NAME_COL, OBJECT_PARENT_COL]  # Column Name, Table Name
        missing_cols = [col for col in cdm_required_columns if col not in df.columns]
        if missing_cols:
            print(f"ERROR: Missing required {data_type} columns: {missing_cols}")
            print(f"Available columns: {df.columns.tolist()}")
            return False
        return True
    else:
        # CSV validation
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"ERROR: Missing required {data_type} columns: {missing_cols}")
            print(f"Available columns: {df.columns.tolist()}")
            return False
        return True


def build_cdm_glossary_dict(cdm_df: pd.DataFrame) -> Dict[str, Dict]:
    """Build CDM glossary dictionary for lookups"""
    glossary_dict = {}
    
    for _, row in cdm_df.iterrows():
        term = row.get(OBJECT_NAME_COL)
        if term and isinstance(term, str):
            glossary_dict[term.strip()] = row.to_dict()
    
    print(f"Built CDM glossary with {len(glossary_dict)} terms")
    return glossary_dict


def build_cdm_terms_list(cdm_df: pd.DataFrame) -> List[str]:
    """Build list of all CDM terms"""
    terms = cdm_df[OBJECT_NAME_COL].dropna().unique().tolist()
    terms = [str(t).strip() for t in terms if str(t).strip()]
    print(f"Built CDM terms list with {len(terms)} unique terms")
    return terms


def run_validation(predicted_file_path: str, validation_file_path: str):
    """Validate predicted mappings against the validation sheet"""
    def normalize_text(text: str) -> str:
        """Convert text to consistent format for comparison"""
        if pd.isna(text):
            return ""
        return str(text).strip().lower().replace("_", "").replace(" ", "")

    print("\n--- Running Validation ---")

    try:
        # Load files
        pred_df = pd.read_csv(predicted_file_path)
        val_df = pd.read_csv(validation_file_path)

        print(f"📄 Predicted file: {len(pred_df)} rows")
        print(f"📄 Validation file: {len(val_df)} rows")

        # Normalize columns
        pred_df["app_table_norm"] = pred_df["app_table"].apply(normalize_text)
        pred_df["app_column_norm"] = pred_df["app_column"].apply(normalize_text)
        pred_df["llm_evaluator_choice_norm"] = pred_df["llm_evaluator_choice"].apply(normalize_text)
        pred_df["cdm_entity_norm"] = pred_df["cdm_entity"].apply(normalize_text)

        val_df["App Table name_norm"] = val_df["App Table name"].apply(normalize_text)
        val_df["App Column Name_norm"] = val_df["App Column Name"].apply(normalize_text)
        val_df["CDM Parent Mapped_norm"] = val_df["CDM Parent Mapped"].apply(normalize_text)
        val_df["CDM Column Mapped_norm"] = val_df["CDM Column Mapped"].apply(normalize_text)

        # Merge predicted and validation data
        merged = pd.merge(
            pred_df,
            val_df,
            left_on=["app_table_norm", "app_column_norm"],
            right_on=["App Table name_norm", "App Column Name_norm"],
            how="inner",
            suffixes=("_pred", "_true")
        )

        print(f"Merged {len(merged)} matching rows (by table + column).")

        # Validation Logic
        def is_exact_match(row):
            column_match = row["app_column_norm"] == row["App Column Name_norm"]

            pred_choice = row.get("llm_evaluator_choice_norm", "")
            val_choice = row.get("CDM Column Mapped_norm", "")

            if pred_choice == "" and val_choice == "":
                entity_match = True  # both blank, treat as correct
            else:
                entity_match = pred_choice == val_choice

            return column_match and entity_match

        merged["is_correct"] = merged.apply(is_exact_match, axis=1)

        # Metrics
        total = len(merged)
        correct = merged["is_correct"].sum()
        incorrect = total - correct
        accuracy = (correct / total * 100) if total > 0 else 0.0

        # Print results
        print("\n=======================")
        print("🧾 VALIDATION SUMMARY")
        print("=======================")
        print(f"✓ Total Records Compared: {total}")
        print(f"✓ Correct Mappings: {correct}")
        print(f"✗ Incorrect Mappings: {incorrect}")
        print(f"📊 Accuracy: {accuracy:.2f}%")
        print("=======================\n")

    except Exception as e:
        print(f"❌ Validation failed: {e}")
