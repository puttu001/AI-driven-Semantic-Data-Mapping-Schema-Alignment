"""
Validation Module for CDM Mapping Accuracy Assessment
Compares LLM-proposed candidates against ground truth validation sheet

Supports variable number of candidates (0-3) and provides multiple accuracy metrics:
- Combined Accuracy (primary): Position-based scoring (Rank 1=100%, Rank 2=66.67%, Rank 3=33.33%)
- Level 1 Accuracy: Binary presence check (correct in top N)
- Level 2 Accuracy: Alternative ranking weights (Rank 1=100%, Rank 2=50%, Rank 3=25%)
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple


def load_validation_sheet(validation_file_path: str) -> pd.DataFrame:
    """
    Load validation sheet with ground truth mappings.
    
    Expected columns:
    - Sr. No. (optional)
    - App Table name
    - App Column Name  
    - CDM Parent Mapped (table name)
    - CDM Column Mapped (column name)
    
    Args:
        validation_file_path: Path to validation CSV file
        
    Returns:
        DataFrame with standardized column names and lookup keys
    """
    df = pd.read_csv(validation_file_path)
    
    # Standardize column names (strip whitespace)
    df.columns = df.columns.str.strip()
    
    # Create lookup key: "table_name|column_name" (case-insensitive)
    df['lookup_key'] = (
        df['App Table name'].astype(str).str.strip().str.lower() + '|' + 
        df['App Column Name'].astype(str).str.strip().str.lower()
    )
    
    return df


def get_ground_truth(validation_df: pd.DataFrame, 
                     app_table: str, 
                     app_column: str) -> Optional[Dict[str, str]]:
    """
    Get ground truth mapping for a specific app table/column combination.
    
    Args:
        validation_df: Loaded validation dataframe with lookup keys
        app_table: Application table name
        app_column: Application column name
    
    Returns:
        Dictionary with {'cdm_table': '...', 'cdm_column': '...'}
        or None if no ground truth exists for this combination
    """
    lookup_key = f"{app_table.strip().lower()}|{app_column.strip().lower()}"
    
    match = validation_df[validation_df['lookup_key'] == lookup_key]
    
    if match.empty:
        return None
    
    return {
        'cdm_table': str(match.iloc[0]['CDM Parent Mapped']).strip(),
        'cdm_column': str(match.iloc[0]['CDM Column Mapped']).strip()
    }


def validate_single_suggestion(suggestion: Dict, 
                               validation_df: pd.DataFrame) -> Dict:
    """
    Validate a single suggestion against ground truth.
    Handles 0, 1, 2, or 3 candidates gracefully.
    
    Args:
        suggestion: Dictionary containing:
            - 'csv_table_name': Application table name
            - 'csv_column_name': Application column name
            - 'other_candidates': List of proposed candidates (0-3 items)
                Each candidate: {'term': '...', 'score': ..., 'table_name': '...'}
        validation_df: Loaded validation dataframe
        
    Returns:
        Dictionary with validation results:
        {
            'app_table': 'store_master',
            'app_column': 'store_code',
            'ground_truth': {'cdm_table': 'df_store_info', 'cdm_column': 'STORE_ID'},
            'proposed_candidates': [...],
            'num_candidates': 3,
            'is_correct_in_top3': True,
            'correct_rank': 1,  # 1, 2, 3, or None
            'level1_score': 1.0,
            'level2_score': 1.0,
            'combined_score': 1.0,
            'status': 'VALIDATED'
        }
    """
    app_table = suggestion.get('csv_table_name', '')
    app_column = suggestion.get('csv_column_name', '')
    
    # Get ground truth
    ground_truth = get_ground_truth(validation_df, app_table, app_column)
    
    if not ground_truth:
        return {
            'app_table': app_table,
            'app_column': app_column,
            'ground_truth': None,
            'proposed_candidates': [],
            'num_candidates': 0,
            'is_correct_in_top3': None,
            'correct_rank': None,
            'level1_score': None,
            'level2_score': None,
            'combined_score': None,
            'status': 'NO_GROUND_TRUTH'
        }
    
    # Extract proposed candidates (may be 0, 1, 2, or 3)
    candidates = suggestion.get('other_candidates', [])
    num_candidates = len(candidates)
    
    # Check if correct mapping is in available candidates
    correct_rank = None
    for idx, candidate in enumerate(candidates, start=1):
        candidate_term = str(candidate.get('term', '')).strip()
        candidate_table = str(candidate.get('table_name', '')).strip()
        
        # Match both table and column (case-insensitive)
        if (candidate_table.lower() == ground_truth['cdm_table'].lower() and 
            candidate_term.lower() == ground_truth['cdm_column'].lower()):
            correct_rank = idx
            break
    
    # Calculate scores
    is_correct = correct_rank is not None
    
    # Level 1: Binary - is correct mapping in candidates?
    level1_score = 1.0 if is_correct else 0.0
    
    # Level 2: Ranking-based scoring (traditional weights)
    # Rank 1: 1.0, Rank 2: 0.5, Rank 3: 0.25, Not found: 0.0
    level2_weights = {1: 1.0, 2: 0.5, 3: 0.25}
    level2_score = level2_weights.get(correct_rank, 0.0)
    
    # Combined Score (Primary metric - Option A)
    # Rank 1: 100% (1.0), Rank 2: 66.67% (0.6667), Rank 3: 33.33% (0.3333), Not found: 0%
    combined_weights = {1: 1.0, 2: 0.6667, 3: 0.3333}
    combined_score = combined_weights.get(correct_rank, 0.0)
    
    return {
        'app_table': app_table,
        'app_column': app_column,
        'ground_truth': ground_truth,
        'proposed_candidates': candidates,
        'num_candidates': num_candidates,
        'is_correct_in_top3': is_correct,
        'correct_rank': correct_rank,
        'level1_score': level1_score,
        'level2_score': level2_score,
        'combined_score': combined_score,
        'status': 'VALIDATED'
    }


def calculate_overall_accuracy(validation_results: List[Dict]) -> Dict:
    """
    Calculate overall accuracy metrics from individual validation results.
    
    Args:
        validation_results: List of individual validation result dictionaries
        
    Returns:
        Dictionary containing:
        {
            'total_rows': 110,
            'validated_rows': 107,
            'skipped_rows': 3,
            'combined_accuracy': 74.32,  # Primary metric (Option A weights)
            'level1_accuracy': 85.05,    # Binary presence
            'level2_accuracy': 72.43,    # Alternative ranking weights
            'rank_distribution': {
                'rank_1': 65,
                'rank_2': 20,
                'rank_3': 6,
                'not_found': 14,
                'no_candidates': 2
            },
            'detailed_results': [...]
        }
    """
    validated = [r for r in validation_results if r['status'] == 'VALIDATED']
    
    if not validated:
        return {
            'total_rows': len(validation_results),
            'validated_rows': 0,
            'skipped_rows': len(validation_results),
            'combined_accuracy': 0.0,
            'level1_accuracy': 0.0,
            'level2_accuracy': 0.0,
            'rank_distribution': {
                'rank_1': 0,
                'rank_2': 0,
                'rank_3': 0,
                'not_found': 0,
                'no_candidates': 0
            },
            'detailed_results': validation_results,
            'message': 'No validation data available'
        }
    
    # Combined Accuracy (Primary metric)
    combined_sum = sum(r['combined_score'] for r in validated)
    combined_accuracy = combined_sum / len(validated)
    
    # Level 1: Presence in top N (any rank)
    level1_sum = sum(r['level1_score'] for r in validated)
    level1_accuracy = level1_sum / len(validated)
    
    # Level 2: Ranking-weighted (alternative weighting)
    level2_sum = sum(r['level2_score'] for r in validated)
    level2_accuracy = level2_sum / len(validated)
    
    # Rank distribution
    rank_counts = {
        'rank_1': 0,
        'rank_2': 0,
        'rank_3': 0,
        'not_found': 0,
        'no_candidates': 0
    }
    
    for r in validated:
        rank = r.get('correct_rank')
        num_cands = r.get('num_candidates', 0)
        
        if num_cands == 0:
            rank_counts['no_candidates'] += 1
        elif rank == 1:
            rank_counts['rank_1'] += 1
        elif rank == 2:
            rank_counts['rank_2'] += 1
        elif rank == 3:
            rank_counts['rank_3'] += 1
        else:
            rank_counts['not_found'] += 1
    
    return {
        'total_rows': len(validation_results),
        'validated_rows': len(validated),
        'skipped_rows': len(validation_results) - len(validated),
        'combined_accuracy': round(combined_accuracy * 100, 2),
        'level1_accuracy': round(level1_accuracy * 100, 2),
        'level2_accuracy': round(level2_accuracy * 100, 2),
        'rank_distribution': rank_counts,
        'detailed_results': validation_results
    }


def run_validation_analysis(suggestions: List[Dict], 
                            validation_file_path: str) -> Dict:
    """
    Main function to run complete validation analysis.
    
    Args:
        suggestions: List of suggestions from workflow, each containing:
            - csv_table_name
            - csv_column_name
            - other_candidates (list of 0-3 candidate dicts)
        validation_file_path: Path to validation CSV file
        
    Returns:
        Complete validation report with accuracy metrics
        
    Example:
        >>> suggestions = [{'csv_table_name': 'store_master', ...}, ...]
        >>> report = run_validation_analysis(suggestions, 'validation.csv')
        >>> print(f"Combined Accuracy: {report['combined_accuracy']}%")
    """
    try:
        # Load validation sheet
        validation_df = load_validation_sheet(validation_file_path)
        
        # Validate each suggestion
        validation_results = []
        for suggestion in suggestions:
            result = validate_single_suggestion(suggestion, validation_df)
            validation_results.append(result)
        
        # Calculate overall metrics
        overall_metrics = calculate_overall_accuracy(validation_results)
        
        return overall_metrics
        
    except Exception as e:
        return {
            'error': str(e),
            'total_rows': len(suggestions),
            'validated_rows': 0,
            'combined_accuracy': 0.0,
            'level1_accuracy': 0.0,
            'level2_accuracy': 0.0,
            'message': f'Validation failed: {e}'
        }


def print_validation_summary(validation_report: Dict):
    """
    Print human-readable validation summary to console.
    Highlights combined accuracy as primary metric.
    
    Args:
        validation_report: Dictionary returned by calculate_overall_accuracy()
    """
    print("\n" + "="*80)
    print("📊 VALIDATION RESULTS - ACCURACY ASSESSMENT")
    print("="*80)
    
    if 'error' in validation_report:
        print(f"❌ Error: {validation_report.get('message', 'Unknown error')}")
        print("="*80)
        return
    
    print(f"Total Rows Processed: {validation_report['total_rows']}")
    print(f"Validated Rows: {validation_report['validated_rows']}")
    print(f"Skipped Rows (no ground truth): {validation_report['skipped_rows']}")
    print()
    print("="*80)
    print(f"🎯 COMBINED ACCURACY: {validation_report['combined_accuracy']}%")
    print("="*80)
    print()
    print("📈 Detailed Metrics:")
    print(f"   Level 1 Accuracy (Correct in Top N): {validation_report['level1_accuracy']}%")
    print(f"   Level 2 Accuracy (Ranking-weighted): {validation_report['level2_accuracy']}%")
    print()
    print("🎯 Rank Distribution:")
    dist = validation_report['rank_distribution']
    print(f"   ✅ Rank 1 (Perfect):     {dist['rank_1']} mappings (100% score)")
    print(f"   ✅ Rank 2 (Good):        {dist['rank_2']} mappings (66.67% score)")
    print(f"   ⚠️  Rank 3 (Acceptable):  {dist['rank_3']} mappings (33.33% score)")
    print(f"   ❌ Not Found:            {dist['not_found']} mappings (0% score)")
    print(f"   ⚪ No Candidates:        {dist['no_candidates']} mappings (0% score)")
    print("="*80)


def export_detailed_validation_results(validation_report: Dict, 
                                       output_path: str):
    """
    Export detailed validation results to CSV for analysis.
    
    Args:
        validation_report: Dictionary returned by calculate_overall_accuracy()
        output_path: Path to save detailed results CSV
    """
    if 'detailed_results' not in validation_report:
        print("⚠️ No detailed results to export")
        return
    
    detailed = validation_report['detailed_results']
    
    # Prepare data for export
    export_data = []
    for result in detailed:
        if result['status'] != 'VALIDATED':
            continue
            
        gt = result.get('ground_truth', {})
        export_data.append({
            'App_Table': result['app_table'],
            'App_Column': result['app_column'],
            'Ground_Truth_Table': gt.get('cdm_table', 'N/A'),
            'Ground_Truth_Column': gt.get('cdm_column', 'N/A'),
            'Num_Candidates_Proposed': result['num_candidates'],
            'Correct_Rank': result['correct_rank'] if result['correct_rank'] else 'Not Found',
            'Combined_Score': result['combined_score'],
            'Level1_Score': result['level1_score'],
            'Level2_Score': result['level2_score'],
            'Candidate_1': result['proposed_candidates'][0]['term'] if len(result['proposed_candidates']) > 0 else '',
            'Candidate_2': result['proposed_candidates'][1]['term'] if len(result['proposed_candidates']) > 1 else '',
            'Candidate_3': result['proposed_candidates'][2]['term'] if len(result['proposed_candidates']) > 2 else ''
        })
    
    df = pd.DataFrame(export_data)
    df.to_csv(output_path, index=False)
    print(f"✅ Detailed validation results exported to: {output_path}")
