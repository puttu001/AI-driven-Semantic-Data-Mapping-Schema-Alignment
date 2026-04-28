#!/usr/bin/env python3
"""
Quick test script to verify frontend and backend are properly connected
Run this after starting the FastAPI backend
"""

import requests
import time

BASE_URL = "http://localhost:8000"

def test_frontend_connectivity():
    """Test if frontend pages are being served"""
    print("🧪 Testing Frontend & Backend Connection...\n")
    
    # Test 1: Root page
    print("1️⃣  Testing root page (index.html)...")
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200 and "<html" in response.text.lower():
            print("   ✅ Root page served successfully")
        else:
            print(f"   ❌ Root page failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 2: Processing page
    print("\n2️⃣  Testing processing page...")
    try:
        response = requests.get(f"{BASE_URL}/pages/processing.html")
        if response.status_code == 200 and "<html" in response.text.lower():
            print("   ✅ Processing page served successfully")
        else:
            print(f"   ❌ Processing page failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 3: CSS file
    print("\n3️⃣  Testing stylesheet...")
    try:
        response = requests.get(f"{BASE_URL}/styles/starter.css")
        if response.status_code == 200:
            print("   ✅ Stylesheet served successfully")
        else:
            print(f"   ❌ Stylesheet failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Test 4: API connectivity
    print("\n4️⃣  Testing API health check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            print("   ✅ API is responding")
        else:
            print(f"   ❌ API health check failed: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "="*50)
    print("✨ If all tests passed, you're good to go!")
    print("="*50)

if __name__ == "__main__":
    print(f"Testing connectivity to {BASE_URL}\n")
    time.sleep(1)
    test_frontend_connectivity()
