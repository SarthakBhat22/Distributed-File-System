#!/bin/bash
# start_dfs.sh

echo "Starting DFS components..."

# Start Redis in background
echo "Starting Redis..."
redis-server &
REDIS_PID=$!
sleep 2

# Start NameNode in background
echo "Starting NameNode..."
python3 name1.py &
NAMENODE_PID=$!
sleep 2

# Start DataNodes in background
echo "Starting DataNodes..."
python3 data1.py -p 8001 &
DN1_PID=$!
python3 data1.py -p 8002 &
DN2_PID=$!
python3 data1.py -p 8003 &
DN3_PID=$!

sleep 3
echo "All components started. Starting CLI..."

# Start CLI in foreground
python3 dfs_cli.py

# When CLI exits, kill background processes
echo "Shutting down DFS..."
kill $REDIS_PID $NAMENODE_PID $DN1_PID $DN2_PID $DN3_PID 2>/dev/null
echo "DFS shutdown complete"

# ----------------------------------------

#!/bin/bash
# start_dfs_tmux.sh - Using tmux (better for development)

# Check if tmux is available
if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Please install it first:"
    echo "  macOS: brew install tmux"
    echo "  Ubuntu: sudo apt install tmux"
    exit 1
fi

SESSION_NAME="dfs"

# Kill existing session if it exists
tmux kill-session -t $SESSION_NAME 2>/dev/null

# Create new session and windows
tmux new-session -d -s $SESSION_NAME -n redis 'redis-server'
tmux new-window -t $SESSION_NAME -n namenode 'python3 name1.py'
tmux new-window -t $SESSION_NAME -n datanode1 'python3 data1.py -p 8001'
tmux new-window -t $SESSION_NAME -n datanode2 'python3 data1.py -p 8002'
tmux new-window -t $SESSION_NAME -n datanode3 'python3 data1.py -p 8003'
tmux new-window -t $SESSION_NAME -n cli 'sleep 5 && python3 dfs_cli.py'

echo "DFS started in tmux session '$SESSION_NAME'"
echo "To attach: tmux attach -t $SESSION_NAME"
echo "To kill all: tmux kill-session -t $SESSION_NAME"

# Optionally attach immediately
# tmux attach -t $SESSION_NAME