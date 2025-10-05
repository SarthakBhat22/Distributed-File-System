#!/bin/bash

# YADFS Cluster Launcher Script
# This script launches all components in separate terminal windows

echo "Starting YADFS Cluster..."

# Detect the terminal emulator
if command -v gnome-terminal &> /dev/null; then
    TERMINAL="gnome-terminal"
    TERM_CMD="--"
elif command -v konsole &> /dev/null; then
    TERMINAL="konsole"
    TERM_CMD="-e"
elif command -v xterm &> /dev/null; then
    TERMINAL="xterm"
    TERM_CMD="-e"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    TERMINAL="osascript"
else
    echo "No supported terminal found. Please install gnome-terminal, konsole, or xterm."
    exit 1
fi

# Function to launch command in new terminal
launch_terminal() {
    local title=$1
    local command=$2
    
    if [[ "$TERMINAL" == "gnome-terminal" ]]; then
        gnome-terminal --title="$title" -- bash -c "$command; exec bash"
    elif [[ "$TERMINAL" == "konsole" ]]; then
        konsole --title "$title" -e bash -c "$command; exec bash" &
    elif [[ "$TERMINAL" == "xterm" ]]; then
        xterm -title "$title" -e bash -c "$command; exec bash" &
    elif [[ "$TERMINAL" == "osascript" ]]; then
        osascript -e "tell app \"Terminal\" to do script \"cd $(pwd) && $command\""
    fi
    
    sleep 0.5
}

# Check if Redis is already running
if ! pgrep -x "redis-server" > /dev/null; then
    echo "Starting Redis Server..."
    launch_terminal "Redis Server" "redis-server"
    sleep 2
else
    echo "Redis is already running"
fi

# Start NameNode
echo "Starting NameNode..."
launch_terminal "NameNode" "python3 namenode.py"
sleep 2

# Start DataNodes
echo "Starting DataNode 8001..."
launch_terminal "DataNode 8001" "python3 datanode.py -p 8001"
sleep 1

echo "Starting DataNode 8002..."
launch_terminal "DataNode 8002" "python3 datanode.py -p 8002"
sleep 1

echo "Starting DataNode 8003..."
launch_terminal "DataNode 8003" "python3 datanode.py -p 8003"
sleep 1

# Start Performance Monitor (optional)
echo "Starting Performance Monitor..."
launch_terminal "Performance Monitor" "python3 performance_monitor.py --interval 10"
sleep 1

# Start CLI
echo "Starting DFS CLI..."
launch_terminal "DFS CLI" "python3 dfs_cli.py"

echo ""
echo "YADFS Cluster started successfully!"
echo "All components are running in separate terminal windows."
echo ""
echo "To stop all components, run: ./stop_yadfs.sh"