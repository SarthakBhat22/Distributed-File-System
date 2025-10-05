# Basic DFS Test with Complete Cleanup

Comprehensive test that verifies your distributed file system functionality and returns it to the initial clean state.

## Prerequisites

1. Redis server running: `redis-server`
2. Start your DFS components:
   ```bash
   # Terminal 1
   python name1.py
   
   # Terminal 2-4  
   python data1.py -p 8001
   python data1.py -p 8002
   python data1.py -p 8003
   ```

## Run Test

```bash
python basic_test.py
```

## What it tests

**Functionality Tests:**
1. NameNode connectivity
2. Nested directory creation
3. Directory navigation
4. File upload in multiple directories
5. Directory listing
6. File download from multiple locations
7. File integrity verification

**Cleanup Tests:**
8. File deletion
9. Directory deletion (recursive)
10. Complete cleanup verification

## Features

- **Automated cleanup**: Automatically confirms deletions (no manual input required)
- **Complete restoration**: Returns DFS to initial empty state
- **Multiple directories**: Tests nested directory structures
- **Multiple files**: Tests files in different locations
- **Integrity checking**: Verifies downloaded files match uploaded content
- **Local cleanup**: Cleans up all temporary files created during testing

## Expected Output

```
Basic DFS Test with Cleanup
===================================
1. Testing NameNode connection...
   NameNode connected
2. Testing directory creation...
   Nested directories created
3. Testing directory navigation...
   Directory navigation works
4. Testing file upload in multiple directories...
   Files uploaded to multiple directories
5. Testing directory listing...
   Directory listing shows expected contents
6. Testing file download and integrity...
   Files downloaded successfully
   File integrity verified for both files

All functionality tests passed!

===================================
CLEANUP PHASE - Returning DFS to initial state
===================================
7. Testing file deletion...
   Files deleted successfully
8. Testing directory deletion...
   Directory and all contents deleted

Local temp files cleaned up: 4 files

All tests passed! DFS is working correctly and returned to initial state.
```

You can verify all components are working. If some parts are failing, the logs can be used to debug the issue.