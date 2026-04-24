"""
MongoDB Vector Search Index Operations
Handles vector search index creation for MongoDB Atlas
"""


def create_vector_search_index(collection, index_name: str, embedding_dim: int = 3072):
    """Create vector search index on MongoDB collection"""
    try:
        print(f"Creating vector search index '{index_name}' on collection '{collection.name}'...")
        
        # Check if index already exists
        existing_indexes = list(collection.list_search_indexes())
        for idx in existing_indexes:
            if idx.get('name') == index_name:
                print(f"Index '{index_name}' already exists. Skipping creation.")
                return True
        
        # Create vector search index
        search_index_model = {
            "definition": {
                "mappings": {
                    "dynamic": True,
                    "fields": {
                        "embedding": {
                            "type": "knnVector",
                            "dimensions": embedding_dim,
                            "similarity": "cosine"
                        }
                    }
                }
            },
            "name": index_name
        }
        
        collection.create_search_index(search_index_model)
        print(f"✅ Vector search index '{index_name}' created successfully")
        print("⏳ Note: Index creation may take a few minutes to complete in MongoDB Atlas")
        return True
        
    except Exception as e:
        print(f"❌ Error creating vector search index: {e}")
        return False
