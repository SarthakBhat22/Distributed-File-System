#!/usr/bin/env python3
"""
Basic DFS Test - Simple verification that components are working
Tests functionality and cleans up completely to return DFS to initial state
"""

import os
import sys
import tempfile
import builtins

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from client import Client

original_input = builtins.input

def mock_input(prompt=""):
    """Mock input function that auto-confirms deletions"""
    print(prompt, end="")
    if "delete" in prompt.lower() and "? (y/n)" in prompt:
        print("y")
        return "y"
    return original_input("")

def test_basic_functionality():
    """Test basic DFS functionality with complete cleanup"""
    print("Basic DFS Test with Cleanup")
    print("=" * 35)
    
    temp_files = []
    
    try:
        # Test 1: Connect to NameNode
        print("1. Testing NameNode connection...")
        client = Client()
        contents = client.list_directory("/")
        if contents is not None:
            print("NameNode connected")
        else:
            print("NameNode connection failed")
            return False
        
        # Test 2: Create nested directories
        print("2. Testing directory creation...")
        success1 = client.create_directory("/test_basic")
        success2 = client.create_directory("/test_basic/subdir")
        if success1 and success2:
            print("Nested directories created")
        else:
            print("Directory creation failed")
        
        # Test 3: Directory navigation
        print("3. Testing directory navigation...")
        nav_success = client.change_directory("/test_basic/subdir")
        current_dir = client.get_current_directory()
        if nav_success and current_dir == "/test_basic/subdir":
            print("Directory navigation works")
        else:
            print("Directory navigation failed")
            return False
        
        # Test 4: Upload files in different directories
        print("4. Testing file upload in multiple directories...")
        
        # File in root test directory
        test_content1 = "Root test file content.\n" * 25
        temp_file1 = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
        temp_file1.write(test_content1)
        temp_file1.close()
        temp_files.append(temp_file1.name)
        
        client.change_directory("/test_basic")
        success1 = client.write_file(temp_file1.name, "root_test.txt")
        
        # File in subdirectory
        test_content2 = "Subdirectory test file content.\n" * 30
        temp_file2 = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
        temp_file2.write(test_content2)
        temp_file2.close()
        temp_files.append(temp_file2.name)
        
        client.change_directory("/test_basic/subdir")
        success2 = client.write_file(temp_file2.name, "sub_test.txt")
        
        if success1 and success2:
            print("Files uploaded to multiple directories")
        else:
            print("File upload failed")
            return False
        
        # Test 5: List directory contents
        print("5. Testing directory listing...")
        client.change_directory("/test_basic")
        contents = client.list_directory()
        
        if contents and len(contents) >= 2:
            print("Directory listing shows expected contents")
        else:
            print("Directory listing failed")
            return False
        
        # Test 6: Download and verify files
        print("6. Testing file download and integrity...")
        
        download_path1 = tempfile.mktemp(suffix='_downloaded1.txt')
        download_path2 = tempfile.mktemp(suffix='_downloaded2.txt')
        temp_files.extend([download_path1, download_path2])
        
        # Download from root test directory
        client.change_directory("/test_basic")
        success1 = client.read_file("root_test.txt", download_path1)
        
        # Download from subdirectory
        client.change_directory("/test_basic/subdir")
        success2 = client.read_file("sub_test.txt", download_path2)
        
        if success1 and success2:
            print("Files downloaded successfully")
        else:
            print("File download failed")
            return False
        
        with open(download_path1, 'r') as f:
            downloaded_content1 = f.read()
        with open(download_path2, 'r') as f:
            downloaded_content2 = f.read()
        
        if downloaded_content1 == test_content1 and downloaded_content2 == test_content2:
            print("File integrity verified for both files")
        else:
            print("File integrity check failed")
            return False
        
        print("\nAll functionality tests passed!")
        
        # CLEANUP PHASE
        print("\n" + "=" * 35)
        print("CLEANUP PHASE - Returning DFS to initial state")
        print("=" * 35)
        
        builtins.input = mock_input
        
        # Test 7: File deletion
        print("7. Testing file deletion...")
        client.change_directory("/test_basic")
        
        # Delete file in root test directory
        delete_success1 = client.delete_file("root_test.txt")
        
        # Delete file in subdirectory  
        client.change_directory("/test_basic/subdir")
        delete_success2 = client.delete_file("sub_test.txt")
        
        if delete_success1 and delete_success2:
            print("Files deleted successfully")
        else:
            print("File deletion failed")
        
        # Test 8: Directory deletion
        print("8. Testing directory deletion...")
        client.change_directory("/")
        
        # Delete the entire test directory structure
        delete_success = client.delete_directory("/test_basic")
        
        if delete_success:
            print("Directory and all contents deleted")
        else:
            print("Directory deletion failed")
        
        
        builtins.input = original_input
        for temp_file_path in temp_files:
            try:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
            except:
                pass
        
        print(f"\nLocal temp files cleaned up: {len(temp_files)} files")
        print("\nAll tests passed! DFS is working correctly and returned to initial state.")
        return True
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        
        builtins.input = original_input
        for temp_file_path in temp_files:
            try:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
            except:
                pass
        
        return False

if __name__ == "__main__":
    success = test_basic_functionality()
    sys.exit(0 if success else 1)
