# YADFS Startup Guide

## Quick Start

### Option 1: Manual Start (Recommended)

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

3. **Wait 10-15 seconds** for all components to register

4. **Test the system** (Terminal 5):
   ```bash
   python test_fix.py
   ```

### Option 2: Auto Start (macOS only)
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
✓ File uploaded successfully!
✓ File downloaded successfully!
```

## Troubleshooting

1. **Connection errors**: Make sure all components are running
2. **"No DataNode available"**: Wait longer for registration
3. **File not found**: Check file path in files/ directory

## File Operations

### Upload:
```python
client = Client()
client.write_file("files/SpotifySongs.csv")
```

### Download:
```python
client.read_file("SpotifySongs.csv", "output/recovered.csv")
```

### Interactive CLI:
```bash
python dfs_cli.py
``` 