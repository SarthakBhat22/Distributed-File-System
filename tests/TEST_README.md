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
   ✓ NameNode connected
2. Testing directory creation...
   ✓ Nested directories created
...
9. Verifying complete cleanup...
   ✓ DFS returned to initial clean state

All tests passed! DFS is working correctly and returned to initial state.
```

Takes about 15-20 seconds to run. If successful, your DFS will be completely clean with no test artifacts remaining.