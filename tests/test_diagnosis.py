#!/usr/bin/env python3
"""
Diagnostic test to figure out what's wrong
"""
import pytest
import os
import sys
from pathlib import Path

def test_basic_python():
    """This should always work"""
    print(f"\n🔍 Python version: {sys.version}")
    print(f"🔍 Current directory: {os.getcwd()}")
    assert 1 + 1 == 2
    print("✅ Basic Python test passed")

def test_project_structure():
    """Check if project structure exists"""
    current_dir = Path.cwd()
    badge_system_dir = current_dir / "badge_system"
    
    print(f"\n🔍 Current directory: {current_dir}")
    print(f"🔍 Looking for badge_system at: {badge_system_dir}")
    print(f"🔍 badge_system exists: {badge_system_dir.exists()}")
    
    if badge_system_dir.exists():
        print(f"🔍 Contents of badge_system:")
        for item in badge_system_dir.iterdir():
            print(f"   - {item.name}")
    
    print("✅ Project structure test completed")

def test_import_attempt():
    """Try to import badge_system"""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    try:
        print(f"\n🔍 Attempting to import badge_system...")
        # from badge_system.aws.lambda_processor import LambdaBadgeProcessor
        from badge_system.client.http_client import HttpClient
        print(f"✅ LambdaBadgeProcessor imported successfully!")
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        
        # Check what files exist
        badge_system_path = project_root / "badge_system"
        aws_path = badge_system_path / "aws"
        processor_path = aws_path / "lambda_processor.py"
        
        print(f"\n🔍 File existence check:")
        print(f"   badge_system/: {badge_system_path.exists()}")
        print(f"   badge_system/aws/: {aws_path.exists()}")
        print(f"   badge_system/aws/lambda_processor.py: {processor_path.exists()}")
        
        return False

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])