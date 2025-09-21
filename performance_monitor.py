#!/usr/bin/env python3
"""
Performance monitoring script for YADFS
Tracks key metrics and provides performance insights
"""

import time
import psutil
import threading
import json

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {
            'start_time': time.time(),
            'operations': {
                'writes': 0,
                'reads': 0,
                'errors': 0
            },
            'latency': {
                'write_avg': 0,
                'read_avg': 0,
                'write_times': [],
                'read_times': []
            },
            'throughput': {
                'bytes_written': 0,
                'bytes_read': 0,
                'blocks_written': 0,
                'blocks_read': 0
            },
            'system': {
                'cpu_usage': [],
                'memory_usage': [],
                'disk_io': []
            }
        }
        self.lock = threading.Lock()
        
    def record_operation(self, op_type, duration, bytes_transferred=0):
        """Record operation metrics"""
        with self.lock:
            self.metrics['operations'][op_type] += 1
            
            if op_type == 'writes':
                self.metrics['latency']['write_times'].append(duration)
                self.metrics['throughput']['bytes_written'] += bytes_transferred
                self.metrics['throughput']['blocks_written'] += 1
            elif op_type == 'reads':
                self.metrics['latency']['read_times'].append(duration)
                self.metrics['throughput']['bytes_read'] += bytes_transferred
                self.metrics['throughput']['blocks_read'] += 1
            
            # Keep only last 1000 measurements
            if len(self.metrics['latency']['write_times']) > 1000:
                self.metrics['latency']['write_times'] = self.metrics['latency']['write_times'][-1000:]
            if len(self.metrics['latency']['read_times']) > 1000:
                self.metrics['latency']['read_times'] = self.metrics['latency']['read_times'][-1000:]
    
    def record_error(self):
        """Record error occurrence"""
        with self.lock:
            self.metrics['operations']['errors'] += 1
    
    def get_performance_summary(self):
        """Get current performance summary"""
        with self.lock:
            uptime = time.time() - self.metrics['start_time']
            
            # Calculate averages
            write_times = self.metrics['latency']['write_times']
            read_times = self.metrics['latency']['read_times']
            
            write_avg = sum(write_times) / len(write_times) if write_times else 0
            read_avg = sum(read_times) / len(read_times) if read_times else 0
            
            # Calculate throughput
            write_throughput = self.metrics['throughput']['bytes_written'] / uptime if uptime > 0 else 0
            read_throughput = self.metrics['throughput']['bytes_read'] / uptime if uptime > 0 else 0
            
            return {
                'uptime_seconds': uptime,
                'total_operations': sum(self.metrics['operations'].values()),
                'write_operations': self.metrics['operations']['writes'],
                'read_operations': self.metrics['operations']['reads'],
                'error_rate': self.metrics['operations']['errors'] / max(sum(self.metrics['operations'].values()), 1),
                'avg_write_latency_ms': write_avg * 1000,
                'avg_read_latency_ms': read_avg * 1000,
                'write_throughput_mbps': write_throughput / (1024 * 1024),
                'read_throughput_mbps': read_throughput / (1024 * 1024),
                'blocks_written': self.metrics['throughput']['blocks_written'],
                'blocks_read': self.metrics['throughput']['blocks_read']
            }
    
    def start_system_monitoring(self):
        """Start background system monitoring"""
        def monitor_system():
            while True:
                try:
                    cpu_percent = psutil.cpu_percent(interval=1)
                    memory = psutil.virtual_memory()
                    disk_io = psutil.disk_io_counters()
                    
                    with self.lock:
                        self.metrics['system']['cpu_usage'].append(cpu_percent)
                        self.metrics['system']['memory_usage'].append(memory.percent)
                        self.metrics['system']['disk_io'].append({
                            'read_bytes': disk_io.read_bytes if disk_io else 0,
                            'write_bytes': disk_io.write_bytes if disk_io else 0
                        })
                        
                        # Keep only last 100 measurements
                        for key in ['cpu_usage', 'memory_usage', 'disk_io']:
                            if len(self.metrics['system'][key]) > 100:
                                self.metrics['system'][key] = self.metrics['system'][key][-100:]
                    
                    time.sleep(5)
                except Exception as e:
                    print(f"System monitoring error: {e}")
                    time.sleep(5)
        
        thread = threading.Thread(target=monitor_system, daemon=True)
        thread.start()
    
    def print_performance_report(self):
        """Print current performance report"""
        summary = self.get_performance_summary()
        
        print("\n" + "="*50)
        print("YADFS PERFORMANCE REPORT")
        print("="*50)
        print(f"Uptime: {summary['uptime_seconds']:.1f} seconds")
        print(f"Total Operations: {summary['total_operations']}")
        print(f"Write Operations: {summary['write_operations']}")
        print(f"Read Operations: {summary['read_operations']}")
        print(f"Error Rate: {summary['error_rate']:.2%}")
        print(f"Average Write Latency: {summary['avg_write_latency_ms']:.2f} ms")
        print(f"Average Read Latency: {summary['avg_read_latency_ms']:.2f} ms")
        print(f"Write Throughput: {summary['write_throughput_mbps']:.2f} MB/s")
        print(f"Read Throughput: {summary['read_throughput_mbps']:.2f} MB/s")
        print(f"Blocks Written: {summary['blocks_written']}")
        print(f"Blocks Read: {summary['blocks_read']}")
        print("="*50)

# Global monitor instance
monitor = PerformanceMonitor()

def record_operation(op_type, duration, bytes_transferred=0):
    """Global function to record operations"""
    monitor.record_operation(op_type, duration, bytes_transferred)

def record_error():
    """Global function to record errors"""
    monitor.record_error()

if __name__ == "__main__":
    # Start system monitoring
    monitor.start_system_monitoring()
    
    # Print periodic reports
    while True:
        time.sleep(30)
        monitor.print_performance_report() 