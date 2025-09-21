#!/usr/bin/env python3
# start_dfs.py

import subprocess
import time
import signal
import sys
import os
from pathlib import Path

class DFSRunner:
    def __init__(self):
        self.processes = []
        self.running = True
    
    def signal_handler(self, sig, frame):
        print("\nShutting down DFS...")
        self.running = False
        self.cleanup()
        sys.exit(0)
    
    def start_process(self, cmd, name, wait_time=2):
        print(f"Starting {name}...")
        process = subprocess.Popen(cmd, shell=True)
        self.processes.append((process, name))
        time.sleep(wait_time)
        
        # Check if process started successfully
        if process.poll() is not None:
            print(f"Warning: {name} may have failed to start")
        else:
            print(f"{name} started successfully (PID: {process.pid})")
        
        return process
    
    def cleanup(self):
        for process, name in reversed(self.processes):
            if process.poll() is None:  # Process is still running
                print(f"Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"Force killing {name}...")
                    process.kill()
    
    def run(self):
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        try:
            # Start all components
            self.start_process("redis-server", "Redis", 2)
            self.start_process("python3 name1.py", "NameNode", 2)
            self.start_process("python3 data1.py -p 8001", "DataNode-8001", 1)
            self.start_process("python3 data1.py -p 8002", "DataNode-8002", 1)
            self.start_process("python3 data1.py -p 8003", "DataNode-8003", 3)
            
            print("\nAll components started! Starting CLI...")
            print("Press Ctrl+C to shutdown everything\n")
            
            # Start CLI in foreground
            cli_process = subprocess.run(["python3", "dfs_cli.py"])
            
        except Exception as e:
            print(f"Error starting DFS: {e}")
        finally:
            self.cleanup()

if __name__ == "__main__":
    runner = DFSRunner()
    runner.run()
