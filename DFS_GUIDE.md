# YADFS Startup Guide

## Quick Start

### Option 1: Manual Start
The easiest way to start the dfs would be to manually run each command in a terminal. This method also requires the user to make any changes to the endpoints directlty in the code.
1. **Start NameNode** (Terminal 1):
   ```bash
   python name1.py
   ```

2. **Start DataNodes** (Terminal 2-4):
   ```bash
   # Terminal 2
   python data1.py -p 8001
   
   # Terminal 3  
   python data1.py -p 8002
   
   # Terminal 4
   python data1.py -p 8003
   ```

3. **Test the system** (Terminal 5):
   ```bash
   python test_fix.py
   ```

### Option 2: Auto Start
```bash
python start_dfs.py
```

## Expected Output

### NameNode should show:
```
NameNode listening on localhost:8000
DataNode localhost:8001 registered
DataNode localhost:8002 registered  
DataNode localhost:8003 registered
```

### DataNodes should show:
```
DataNode server started on localhost:8001
DataNode 8001 registered with NameNode
```

### Test should show:
```
File uploaded successfully!
File downloaded successfully!
```


## File Operations

### Write a file:
```bash
put <local-file-path/file-name> <dfs-path/file-name>
```

### Read a file:
```bash
get <dfs-path/file-name> <local-file-path/file-name>
```

**Note**: When writing a file, the name of the file within the dfs can be different to the local file system. Same goes for reads, the file in the dfs can be written into a file within the local fs with a different name and location.<br><br>
When writing into the dfs, if the 'dfs-path' is empty like so: ```put data/example.txt example.txt```, the file is written to the root directory.