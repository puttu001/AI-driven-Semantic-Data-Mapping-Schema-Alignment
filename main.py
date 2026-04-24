"""
Main Entry Point for CDM Mapping Project
Orchestrates the entire CDM mapping pipeline using modularized components
"""

import os
import sys
import time
import traceback
from dotenv import load_dotenv

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

from config.settings import (
    CDM_COLLECTION_NAME, CSV_COLLECTION_NAME, MONGODB_DB_NAME,
    CSV_TABLE_NAME_COL, CSV_COLUMN_NAME_COL,
    LLM_MODIFICATION_ENABLED,
    USE_INPUTS_FOLDER, CDM_DATA_DIR, MAPPING_DATA_DIR,
    MAPPED_OUTPUT_DIR, VALIDATION_OUTPUT_DIR
)
from utils.data_processing import (
    load_and_clean_csv_file, load_and_combine_csv_files,
    validate_required_columns, build_cdm_glossary_dict,
    build_cdm_terms_list, create_cdm_representation,
    create_csv_representation, run_validation
)
from utils.file_operations import (
    save_results_to_csv, save_results_to_mongodb,
    get_latest_cdm_file, get_latest_mapping_file, list_input_files
)
from api.api_client import (
    initialize_embeddings_via_api, create_vector_store_via_api
)
from workflow import EnhancedInteractiveMappingWorkflow


def main():
    """Main execution function with LangGraph workflow and reasoning model integration"""
    print("🚀 --- Enhanced CDM Vector Search Pipeline with Reasoning Model Integration ---")
    
    # Load environment variables
    load_dotenv()
    
    # Display file mode
    if USE_INPUTS_FOLDER:
        print("\n📁 FILE MODE: Using files from INPUTS/ folder (latest uploaded)")
        print(f"   CDM folder: {CDM_DATA_DIR}")
        print(f"   Mapping folder: {MAPPING_DATA_DIR}")
    else:
        print("\n📁 Use INPUTS folder")
    
    # Setup API keys
    openai_api_key = os.getenv("OPENAI_API_KEY")
    mongodb_uri = os.getenv("MONGODB_URI")
    
    if not openai_api_key:
        print("❌ FATAL: OPENAI_API_KEY missing from environment")
        exit(1)

    if not mongodb_uri:
        print("❌ FATAL: MONGODB_URI missing from environment")
        exit(1)
    
    # Initialize embeddings via FastAPI
    print("\n📊 --- Initializing Embeddings ---")
    try:
        embeddings_instance = initialize_embeddings_via_api(openai_api_key)
        if not embeddings_instance:
            print("❌ FATAL: Embeddings initialization failed")
            exit(1)
    except Exception as e:
        print(f"❌ FATAL: Error initializing embeddings: {e}")
        traceback.print_exc()
        exit(1)
    
    # Initialize LLM if enabled
    llm_instance = None
    if LLM_MODIFICATION_ENABLED:
        print("\n🤖 --- LLM will be initialized via FastAPI on first use ---")
        llm_instance = {"type": "fastapi", "available": True}
    else:
        print("\n⚠️  --- LLM Modification Disabled ---")
    
    # Load and process CDM file
    print("\n📋 --- Processing CDM Glossary File ---")
    try:
        if USE_INPUTS_FOLDER:
            # Use latest file from INPUTS folder
            cdm_file_path = get_latest_cdm_file()
            
            if cdm_file_path:
                print(f"📁 Using CDM file from INPUTS: {cdm_file_path.name}")
                cdm_df = load_and_clean_csv_file(str(cdm_file_path))
            else:
                print("⚠️  No CDM file found in INPUTS/CDM_data/")
                print("   Checking for hardcoded file in project root...")
                
                # List available files in INPUTS
                input_files = list_input_files()
                if input_files['cdm_files']:
                    print(f"   Available CDM files in INPUTS: {input_files['cdm_files']}")
                
                else:
                    print(f"❌ FATAL: No CDM file found!")
                    exit(1)
        else:
            print(f"📁 Provide CDM glossary file")
        
        # Build CDM glossary dictionary and terms list
        cdm_glossary_dict = build_cdm_glossary_dict(cdm_df)
        cdm_terms_list = build_cdm_terms_list(cdm_df)
        
        print(f"CDM Glossary: {len(cdm_df)} rows, {len(cdm_glossary_dict)} terms")
        
    except Exception as e:
        print(f"❌ FATAL: Error processing CDM file: {e}")
        traceback.print_exc()
        exit(1)
    
    # Load and combine CSV files
    print("\n📋 --- Processing Mapping CSV Files ---")
    try:
        if USE_INPUTS_FOLDER:
            # Use latest file from Inputs folder
            mapping_file_path = get_latest_mapping_file()
            
            if mapping_file_path:
                print(f"📁 Using mapping file from Inputs: {mapping_file_path.name}")
                combined_csv_df = load_and_clean_csv_file(str(mapping_file_path))
            else:
                print("⚠️  No mapping file found in Inputs/mapping_data/")
                
                # List available files in Inputs
                input_files = list_input_files()
                if input_files['mapping_files']:
                    print(f"   Available mapping files in Inputs: {input_files['mapping_files']}")
                
                else:
                    print(f"❌ FATAL: No mapping files found!")
                    exit(1)
        else:
            print(f"📁 Provide relevant files")
        
        csv_required_columns = [CSV_TABLE_NAME_COL, CSV_COLUMN_NAME_COL]
        if not validate_required_columns(combined_csv_df, csv_required_columns, "CSV"):
            exit(1)
        
        print(f"Combined CSV dataset: {len(combined_csv_df)} rows")
        
    except Exception as e:
        print(f"❌ FATAL: Error processing CSV files: {e}")
        traceback.print_exc()
        exit(1)
    
    # Create CDM vector store via FastAPI
    print("\n🔍 --- Creating CDM Vector Store via FastAPI ---")
    cdm_collection_info = create_vector_store_via_api(
        df=cdm_df,
        collection_name=CDM_COLLECTION_NAME,
        mongodb_uri=mongodb_uri,
        db_name=MONGODB_DB_NAME,
        representation_func=create_cdm_representation
    )

    if not cdm_collection_info:
        print("❌ FATAL: CDM collection creation failed")
        exit(1)

    # Create CSV vector store via FastAPI
    print("\n📈 --- Creating CSV Vector Store via FastAPI ---")
    csv_collection_info = create_vector_store_via_api(
        df=combined_csv_df,
        collection_name=CSV_COLLECTION_NAME,
        mongodb_uri=mongodb_uri,
        db_name=MONGODB_DB_NAME,
        representation_func=create_csv_representation
    )

    if not csv_collection_info:
        print("❌ FATAL: CSV collection creation failed")
        exit(1)

    # Wait for MongoDB Atlas indexes
    print("\n⏳ Waiting 30 seconds for MongoDB Atlas vector indexes...")
    time.sleep(30)

    # Initialize and run interactive workflow
    model_indicator = "✨ Reasoning Model Enhanced" if llm_instance else "📊 Standard Processing"
    print(f"\n🎯 --- Starting {model_indicator} Interactive CDM Mapping Workflow ---")
    
    workflow = EnhancedInteractiveMappingWorkflow(
        cdm_collection_info=cdm_collection_info,
        csv_collection_info=csv_collection_info,
        cdm_glossary_dict=cdm_glossary_dict,
        cdm_terms_list=cdm_terms_list,
        llm=llm_instance
    )
    
    try:
        final_mappings, unmapped_columns = workflow.run_interactive_workflow()

        # Save results
        model_suffix = "_reasoning" if workflow.is_reasoning_model else "_standard"
        output_suffix = f"_langgraph{model_suffix}_{int(time.time())}"
        timestamp = int(time.time())

        # Save to CSV (traditional output)
        save_results_to_csv(final_mappings, unmapped_columns, output_suffix)

        # Save to MongoDB (new persistent storage)
        print("\n" + "="*60)
        print("💾 Saving results to MongoDB...")
        print("="*60)

        execution_metadata = {
            "model_type": "reasoning" if workflow.is_reasoning_model else "standard",
            "timestamp": timestamp,
            "output_suffix": output_suffix,
            "cdm_collection": CDM_COLLECTION_NAME,
            "csv_collection": CSV_COLLECTION_NAME,
            "total_mappings": len(final_mappings),
            "total_unmapped": len(unmapped_columns)
        }

        mongo_result = save_results_to_mongodb(
            final_mappings=final_mappings,
            mongodb_uri=mongodb_uri,
            db_name=MONGODB_DB_NAME,
            execution_metadata=execution_metadata
        )

        if mongo_result:
            print(f"✅ Results successfully saved to MongoDB")
        else:
            print(f"⚠️  Warning: Failed to save results to MongoDB (CSV files still available)")
        
        # Run validation
        try:
            predicted_file = MAPPED_OUTPUT_DIR / f"Final_CDM_Mappings{output_suffix}.csv"
            validation_file = VALIDATION_OUTPUT_DIR / "Validation_Sheet_Iteration Third.csv"
            
            if os.path.exists(validation_file):
                print("\n🔍 Starting validation of model output against ground truth...")
                run_validation(predicted_file_path=str(predicted_file), validation_file_path=str(validation_file))
            else:
                print(f"\n⚠️  Validation file not found: {validation_file}")
        except Exception as e:
            print(f"⚠️  Validation step failed: {e}")

        # Print summary
        print(f"\n✅ --- {model_indicator} Pipeline Completed Successfully ---")
        print(f"📊 CDM Collection: {CDM_COLLECTION_NAME} ({cdm_collection_info['num_entities']} vectors)")
        print(f"📊 CSV Collection: {CSV_COLLECTION_NAME} ({csv_collection_info['num_entities']} vectors)") 
        print(f"✓ Final mappings: {len(final_mappings)}")
        print(f"✗ Unmapped columns: {len(unmapped_columns)}")
        
        # Print mapping statistics
        if final_mappings:
            accepted = len([m for m in final_mappings if m.get('final_decision') == 'Accepted'])
            user_rejected = len([m for m in final_mappings if m.get('final_decision') == 'Rejected'])
            auto_rejected = len([m for m in final_mappings if 'Auto-Rejected' in m.get('final_decision', '')])
            modified = len([m for m in final_mappings if m.get('final_decision') == 'Modified'])

            print(f"\n📈 Mapping Statistics:")
            print(f"  ✓ Accepted: {accepted}")
            print(f"  ✗ User Rejected: {user_rejected}")
            print(f"  🚫 Auto-Rejected (Score < 40 or No Matches): {auto_rejected}")
            print(f"  ✏️  Modified: {modified}")
            print(f"  📊 Total Processed: {accepted + user_rejected + auto_rejected + modified}")
        
        # Print workflow features
        reasoning_features = "🧠 Advanced reasoning capabilities" if workflow.is_reasoning_model else "🤖 Standard LLM processing"
        print(f"\n⚙️  LangGraph Workflow Features:")
        print(f"  • State-driven workflow management")
        print(f"  • Conditional routing and decision points")
        print(f"  • Interactive interrupts for user input")
        print(f"  • Memory checkpointing for workflow state")
        print(f"  • Structured node-based processing")
        print(f"  • {reasoning_features}")
        
        if workflow.is_reasoning_model:
            print(f"\n🧠 Reasoning Model Benefits:")
            print(f"  • Enhanced domain-specific logical analysis")
            print(f"  • Multi-step reasoning for complex mappings")
            print(f"  • Better handling of ambiguous cases")
            print(f"  • Improved confidence assessment")
            print(f"  • More contextually aware alternatives")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  --- User interrupted the {model_indicator} workflow ---")
        print("Partial results may be available in the workflow object")
    except Exception as e:
        print(f"❌ Error during {model_indicator} workflow: {e}")
        traceback.print_exc()

    print("\n✅ Pipeline execution completed!")


if __name__ == "__main__":
    main()
