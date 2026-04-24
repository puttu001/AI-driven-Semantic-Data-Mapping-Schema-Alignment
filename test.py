from openai import OpenAI
import os

# Load API key from environment variable
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

try:
    # Make a simple test call
    response = client.models.list()
    print("✅ API key is valid. Available models:")
    for model in response.data[:]:  # show first 5 models
        print("-", model.id)
except Exception as e:
    print("❌ API key check failed:", e)
