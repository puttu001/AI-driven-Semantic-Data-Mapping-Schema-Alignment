"""
Simple CDM Setup - Upload CDM CSV to MongoDB
=============================================
Run this once to store your CDM data in MongoDB.
After this, you only need to upload mapping files.

Usage:
    python cdm_setup.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient

# Load environment
load_dotenv(override=True)

# Configuration
CDM_FILE_PATH = r"C:\Users\TransOrg\Desktop\DATAMAPPING2nd\Inputs\CDM_data\starbucks_CDM_UPDATED_20260220_155746.csv"  # UPDATE THIS PATH
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = "cdm_mapping"
CDM_COLLECTION = "cdm_master_data"

def upload_cdm_to_mongodb():
    """Upload CDM CSV data to MongoDB."""
    
    print("\n" + "="*60)
    print("CDM SETUP - Upload to MongoDB")
    print("="*60)
    
    # Check MongoDB URI
    if not MONGODB_URI:
        print("❌ MONGODB_URI not found in .env file")
        return False
    
    # Check CDM file
    cdm_path = Path(CDM_FILE_PATH)
    if not cdm_path.exists():
        print(f"❌ CDM file not found: {CDM_FILE_PATH}")
        print("\nAvailable files in Inputs/CDM_data/:")
        cdm_dir = Path("Inputs/CDM_data")
        if cdm_dir.exists():
            for f in cdm_dir.glob("*.csv"):
                print(f"  - {f.name}")
        return False
    
    print(f"📂 Reading CDM file: {cdm_path.name}")
    
    # Read CSV
    df = pd.read_csv(cdm_path)
    print(f"✅ Loaded {len(df)} rows")
    
    # Connect to MongoDB
    print(f"🔌 Connecting to MongoDB...")
    client = MongoClient(MONGODB_URI)
    
    try:
        client.admin.command('ping')
        print("✅ Connected to MongoDB")
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return False
    
    # Get collection
    db = client[DB_NAME]
    collection = db[CDM_COLLECTION]
    
    # Check existing data
    existing = collection.count_documents({})
    if existing > 0:
        print(f"\n⚠️  Collection has {existing} existing documents")
        response = input("Replace with new data? (yes/no): ").strip().lower()
        if response != 'yes':
            print("❌ Upload cancelled")
            client.close()
            return False
        collection.delete_many({})
        print("✅ Cleared existing data")
    
    # Upload data
    print(f"⬆️  Uploading {len(df)} CDM records...")
    records = df.to_dict('records')
    collection.insert_many(records)
    
    # Verify
    count = collection.count_documents({})
    print(f"✅ Upload complete: {count} documents in {DB_NAME}.{CDM_COLLECTION}")
    
    client.close()
    
    print("\n" + "="*60)
    print("🎉 CDM Setup Complete!")
    print("="*60)
    print("\n✅ CDM data is now in MongoDB")
    print("✅ You can now upload only mapping files in the UI")
    print("\n💡 To update CDM data, run this script again")
    print("="*60 + "\n")
    
    return True

if __name__ == "__main__":
    success = upload_cdm_to_mongodb()
    sys.exit(0 if success else 1)
