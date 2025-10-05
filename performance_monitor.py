#!/usr/bin/env python3
"""
Distributed Performance monitoring script for YADFS
Queries all DataNodes and NameNode for metrics and displays in terminal
"""

import time
import socket
import json
import argparse
from datetime import datetime

class DistributedPerformanceMonitor:
    def __init__(self, namenode_host, namenode_port):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.monitoring_interval = 10  # seconds
        
        print(f"Initialized Distributed Performance Monitor")
        print(f"NameNode: {namenode_host}:{namenode_port}")
    
    def get_datanodes_from_namenode(self):
        """Query NameNode for list of active DataNodes"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.namenode_host, self.namenode_port))
                s.sendall("get_datanodes".encode())
                response = s.recv(4096).decode()
                
                if response.startswith("datanodes"):
                    nodes = response.split()[1:]
                    return nodes
                return []
        except Exception as e:
            print(f"Error querying NameNode for DataNodes: {e}")
            return []
    
    def query_datanode_metrics(self, datanode_addr):
        """Query a specific DataNode for its metrics"""
        try:
            host, port = datanode_addr.split(':')
            port = int(port)
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((host, port))
                
                # Send using length-prefixed format (same as DataNode expects)
                message = "get_metrics"
                data = message.encode()
                length = len(data).to_bytes(4, 'big')
                s.sendall(length + data)
                
                # Receive response (plain JSON, not length-prefixed)
                response = s.recv(8192).decode()
                metrics = json.loads(response)
                return metrics
                
        except Exception as e:
            print(f"Error querying DataNode {datanode_addr}: {e}")
            return None
    
    def query_namenode_metrics(self):
        """Query NameNode for its metrics"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.namenode_host, self.namenode_port))
                
                # NameNode uses plain strings (no length prefix)
                s.sendall("get_metrics".encode())
                
                response = s.recv(8192).decode()
                metrics = json.loads(response)
                return metrics
                
        except Exception as e:
            print(f"Error querying NameNode: {e}")
            return None
    
    def collect_all_metrics(self):
        """Collect metrics from all nodes in the cluster"""
        datanodes = self.get_datanodes_from_namenode()
        
        datanode_metrics = {}
        for dn in datanodes:
            metrics = self.query_datanode_metrics(dn)
            if metrics:
                datanode_metrics[dn] = metrics
        
        namenode_metrics = self.query_namenode_metrics()
        cluster_metrics = self.calculate_cluster_metrics(datanode_metrics, namenode_metrics)
        
        return {
            'timestamp': time.time(),
            'datanodes': datanode_metrics,
            'namenode': namenode_metrics or {},
            'cluster': cluster_metrics
        }
    
    def calculate_cluster_metrics(self, datanode_metrics, namenode_metrics):
        """Calculate aggregate cluster metrics"""
        cluster = {
            'total_datanodes': len(datanode_metrics),
            'total_blocks_written': 0,
            'total_blocks_read': 0,
            'total_bytes_written': 0,
            'total_bytes_read': 0,
            'avg_write_latency_ms': 0,
            'avg_read_latency_ms': 0,
            'total_errors': 0,
            'cluster_uptime': 0
        }
        
        write_latencies = []
        read_latencies = []
        
        for dn_addr, metrics in datanode_metrics.items():
            cluster['total_blocks_written'] += metrics.get('blocks_written', 0)
            cluster['total_blocks_read'] += metrics.get('blocks_read', 0)
            cluster['total_bytes_written'] += metrics.get('bytes_written', 0)
            cluster['total_bytes_read'] += metrics.get('bytes_read', 0)
            cluster['total_errors'] += metrics.get('errors', 0)
            
            if metrics.get('avg_write_latency_ms', 0) > 0:
                write_latencies.append(metrics['avg_write_latency_ms'])
            if metrics.get('avg_read_latency_ms', 0) > 0:
                read_latencies.append(metrics['avg_read_latency_ms'])
        
        if write_latencies:
            cluster['avg_write_latency_ms'] = sum(write_latencies) / len(write_latencies)
        if read_latencies:
            cluster['avg_read_latency_ms'] = sum(read_latencies) / len(read_latencies)
        
        if namenode_metrics:
            cluster['total_files'] = namenode_metrics.get('total_files', 0)
            cluster['cluster_uptime'] = namenode_metrics.get('uptime', 0)
        
        if cluster.get('cluster_uptime', 0) > 0:
            cluster['write_throughput_mbps'] = (cluster['total_bytes_written'] / cluster['cluster_uptime']) / (1024 * 1024)
            cluster['read_throughput_mbps'] = (cluster['total_bytes_read'] / cluster['cluster_uptime']) / (1024 * 1024)
        else:
            cluster['write_throughput_mbps'] = 0
            cluster['read_throughput_mbps'] = 0
        
        return cluster
    
    def print_metrics_report(self, metrics):
        """Print a formatted metrics report to console"""
        print("\n" + "="*70)
        print(f"YADFS CLUSTER PERFORMANCE REPORT - {datetime.fromtimestamp(metrics['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        cluster = metrics['cluster']
        print("\nCLUSTER OVERVIEW:")
        print(f"  Active DataNodes: {cluster['total_datanodes']}")
        if 'total_files' in cluster:
            print(f"  Total Files: {cluster['total_files']}")
        print(f"  Cluster Uptime: {cluster['cluster_uptime']:.1f}s")
        
        print("\nOPERATIONS:")
        print(f"  Total Blocks Written: {cluster['total_blocks_written']}")
        print(f"  Total Blocks Read: {cluster['total_blocks_read']}")
        print(f"  Total Errors: {cluster['total_errors']}")
        
        print("\nPERFORMANCE:")
        print(f"  Avg Write Latency: {cluster['avg_write_latency_ms']:.2f} ms")
        print(f"  Avg Read Latency: {cluster['avg_read_latency_ms']:.2f} ms")
        print(f"  Write Throughput: {cluster['write_throughput_mbps']:.2f} MB/s")
        print(f"  Read Throughput: {cluster['read_throughput_mbps']:.2f} MB/s")
        
        if metrics['datanodes']:
            print("\nDATANODE DETAILS:")
            for dn_addr, dn_metrics in sorted(metrics['datanodes'].items()):
                print(f"\n  DataNode: {dn_addr}")
                print(f"    Uptime: {dn_metrics.get('uptime', 0):.1f}s")
                print(f"    Blocks Written: {dn_metrics.get('blocks_written', 0)}")
                print(f"    Blocks Read: {dn_metrics.get('blocks_read', 0)}")
                print(f"    Write Latency: {dn_metrics.get('avg_write_latency_ms', 0):.2f} ms")
                print(f"    Read Latency: {dn_metrics.get('avg_read_latency_ms', 0):.2f} ms")
                print(f"    Errors: {dn_metrics.get('errors', 0)}")
        
        print("\n" + "="*70 + "\n")
    
    def start(self):
        """Start the monitoring service"""
        print("Starting Distributed Performance Monitor...\n")
        
        while True:
            try:
                metrics = self.collect_all_metrics()
                self.print_metrics_report(metrics)
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
            
            time.sleep(self.monitoring_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YADFS Distributed Performance Monitor")
    parser.add_argument("--namenode-host", default="localhost", help="NameNode host")
    parser.add_argument("--namenode-port", type=int, default=8000, help="NameNode port")
    parser.add_argument("--interval", type=int, default=10, help="Monitoring interval in seconds")
    
    args = parser.parse_args()
    
    monitor = DistributedPerformanceMonitor(
        namenode_host=args.namenode_host,
        namenode_port=args.namenode_port
    )
    
    monitor.monitoring_interval = args.interval
    
    try:
        monitor.start()
    except KeyboardInterrupt:
        print("\nShutting down Performance Monitor...")