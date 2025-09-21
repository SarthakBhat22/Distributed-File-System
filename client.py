import socket
import os
import math
import json
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class Client:
    def __init__(self, namenode_host="localhost", namenode_port=8000):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.block_size = 64 * 1024  # 64KB blocks
        self.executor = ThreadPoolExecutor(max_workers=min(6, os.cpu_count()))
        self.current_dir = "/"  # Tracking current directory for context
        self.exclude_nodes_lock = threading.Lock()
        
    def set_current_directory(self, path):
        """Set the current working directory"""
        self.current_dir = self.normalize_path(path)
    
    def get_current_directory(self):
        """Get the current working directory"""
        return self.current_dir
    
    def normalize_path(self, path):
        """Normalize a path"""
        if not path.startswith('/'):
            path = '/' + path
        
        components = []
        for component in path.split('/'):
            if component == '' or component == '.':
                continue
            elif component == '..':
                if components:
                    components.pop()
            else:
                components.append(component)
        
        return '/' + '/'.join(components) if components else '/'
    
    def path_to_key(self, path):
        """Convert a file path to a safe Redis key (same as namenode)"""
        return path.replace('/', '__')
        
    def get_fresh_connection(self, datanode_addr, timeout=15):
        """Create a fresh connection for each request to avoid reuse issues"""
        try:
            host, port = datanode_addr.split(':')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(timeout)
            sock.connect((host, int(port)))
            return sock
        except Exception as e:
            print(f"Failed to connect to {datanode_addr}: {e}")
            return None

    def get_active_datanode(self, exclude_nodes=None):
        """Get an active DataNode from NameNode with exclusion list"""
        if exclude_nodes is None:
            exclude_nodes = set()
            
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((self.namenode_host, self.namenode_port))
                    s.sendall("get_datanode".encode())
                    response = s.recv(1024).decode()
                    if response.startswith("datanode"):
                        datanode_addr = response.split()[1]
                        if datanode_addr not in exclude_nodes:
                            return datanode_addr
                        else:
                            print(f"Skipping excluded node: {datanode_addr}")
            except Exception as e:
                print(f"Attempt {attempt + 1} failed to get DataNode: {e}")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
        return None

    def send_message(self, sock, message):
        """Send length-prefixed message"""
        data = message.encode() if isinstance(message, str) else message
        length = len(data).to_bytes(4, 'big')
        sock.sendall(length + data)

    def recv_message(self, sock):
        """Receive length-prefixed message"""
        try:
            length_bytes = self.recv_exact(sock, 4)
            if not length_bytes or len(length_bytes) != 4:
                return None
            length = int.from_bytes(length_bytes, 'big')
            return self.recv_exact(sock, length)
        except:
            return None

    def recv_exact(self, sock, size):
        """Receive exactly 'size' bytes from socket"""
        data = b''
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise Exception("Connection closed prematurely")
            data += chunk
        return data

    def get_file_metadata(self, filename):
        """Get file metadata from NameNode with retry logic"""
        def attempt_get_metadata():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.namenode_host, self.namenode_port))
                message = f"get_metadata {filename} {self.current_dir}"
                s.sendall(message.encode())
                response = s.recv(4096).decode()

                if response.startswith("metadata"):
                    metadata_json = response[9:]
                    return json.loads(metadata_json)
                else:
                    raise Exception(f"Failed to get metadata: {response}")
        
        try:
            return self.retry_with_backoff(
                attempt_get_metadata,
                max_retries=3,
                base_delay=0.5,
                max_delay=5.0
            )
        except Exception as e:
            print(f"Error getting file metadata after retries: {e}")
            return None

    def read_block(self, datanode_addr, storage_name, block_id):
        """Read a block from a DataNode using storage name (path-based key)"""
        sock = None
        try:
            sock = self.get_fresh_connection(datanode_addr)
            if not sock:
                return None

            # Extracting numeric block ID
            if isinstance(block_id, str) and block_id.startswith('block_'):
                block_num = block_id.replace('block_', '')
            else:
                block_num = str(block_id)

            metadata = f"read_block {storage_name} {block_num}"
            self.send_message(sock, metadata)

            block_data = self.recv_message(sock)
            return block_data

        except Exception as e:
            print(f"Error reading block from {datanode_addr}: {e}")
            return None
        finally:
            if sock:
                sock.close()

    def read_block_from_file(self, file_path, block_id):
        """Read a specific block from file without loading entire file"""
        try:
            block_offset = block_id * self.block_size
            
            with open(file_path, 'rb') as f:
                f.seek(block_offset)
                block_data = f.read(self.block_size)
                
            return block_data if block_data else None
            
        except Exception as e:
            print(f"Error reading block {block_id} from {file_path}: {e}")
            return None

    def read_file(self, filename, output_path):
        """Read a file using streaming approach for all files (memory efficient)"""
        print(f"Getting metadata for {filename} in directory {self.current_dir}...")
        metadata = self.get_file_metadata(filename)
        if not metadata:
            print(f"Could not find file {filename}")
            return False

        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            storage_name = metadata.get('storage_name')
            if not storage_name:
                print("Error: No storage name found in metadata")
                return False

            total_blocks = len(metadata['blocks'])
            total_size = metadata.get('total_size', 0)
            print(f"Streaming download: {total_blocks} blocks ({total_size:,} bytes)")

            # Dynamic concurrency based on file size
            if total_size < 10 * 1024 * 1024: # Files < 10MB
                max_concurrent_reads = min(6, max(2, os.cpu_count()))
            elif total_size < 100 * 1024 * 1024: # Files < 100MB
                max_concurrent_reads = min(4, max(2, os.cpu_count() // 2))
            else: # Files >= 100MB
                max_concurrent_reads = min(3, max(1, os.cpu_count() // 3))

            with open(output_path, 'wb') as outfile:
                with ThreadPoolExecutor(max_workers=max_concurrent_reads) as executor:
                    future_to_block = {}
                    for block_idx, block in enumerate(metadata['blocks']):
                        future = executor.submit(
                            self.read_single_block_streaming,
                            storage_name, block, block_idx
                        )
                        future_to_block[future] = (block_idx, block)
                    
                    block_results = {}
                    completed_reads = 0
                    
                    for future in as_completed(future_to_block):
                        block_idx, block = future_to_block[future]
                        try:
                            block_data = future.result()
                            if block_data:
                                block_results[block_idx] = block_data
                                completed_reads += 1
                                
                                if completed_reads % 10 == 0 or completed_reads == total_blocks:
                                    print(f"Download progress: {completed_reads}/{total_blocks} blocks")
                            else:
                                raise Exception(f"Failed to read block {block_idx}")
                        except Exception as e:
                            print(f"Error reading block {block_idx}: {e}")
                            return False
                    
                    for block_idx in range(total_blocks):
                        if block_idx in block_results:
                            outfile.write(block_results[block_idx])
                        else:
                            print(f"Missing block data for block {block_idx}")
                            return False

            print(f"Successfully streamed {filename} to {output_path}")
            return True

        except Exception as e:
            print(f"Error in streaming read: {e}")
            return False

    def read_single_block_streaming(self, storage_name, block, block_idx):
        """Read a single block with retry logic"""
        block_id = block['block_id']
        locations = block['locations']
        
        primary_idx = block_idx % len(locations)
        reordered_locations = locations[primary_idx:] + locations[:primary_idx]
        
        for datanode_addr in reordered_locations:
            try:
                block_data = self.read_block(datanode_addr, storage_name, block_id)
                if block_data:
                    return block_data
            except Exception as e:
                print(f"Failed to read block {block_id} from {datanode_addr}: {e}")
        
        return None

    def store_metadata(self, filename, num_blocks, block_size):
        """Store file metadata in NameNode with directory context"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.namenode_host, self.namenode_port))
                message = f"store_metadata {filename} {num_blocks} {block_size} {self.current_dir}"
                s.sendall(message.encode())
                response = s.recv(1024).decode()
                if response == "success":
                    print(f"Successfully stored metadata for {filename}")
                    return True
                else:
                    print(f"Failed to store metadata for {filename}: {response}")
                    return False
        except Exception as e:
            print(f"Error storing metadata: {e}")
            return False


    def write_file(self, file_path, target_filename=None):
        """Write a file using streaming approach for all files (memory efficient)"""
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist")
            return False

        file_size = os.path.getsize(file_path)
        num_blocks = math.ceil(file_size / self.block_size)

        if target_filename:
            filename = target_filename
        else:
            filename = os.path.basename(file_path)

        print(f"Streaming {filename} ({file_size:,} bytes) into {num_blocks} blocks")
        print(f"Target directory: {self.current_dir}")

        if '/' in filename:
            full_path = self.normalize_path(filename)
        else:
            full_path = os.path.join(self.current_dir, filename).replace("\\", "/")
            full_path = self.normalize_path(full_path)
        
        storage_name = self.path_to_key(full_path)

        # Dynamic concurrency based on file size
        if file_size < 10 * 1024 * 1024:  # Files < 10MB
            max_concurrent_blocks = min(6, max(2, os.cpu_count()))
        elif file_size < 100 * 1024 * 1024:  # Files < 100MB  
            max_concurrent_blocks = min(4, max(2, os.cpu_count() // 2))
        else:  # Files >= 100MB
            max_concurrent_blocks = min(3, max(1, os.cpu_count() // 3))
        
        completed_blocks = []
        failed_blocks = []
        
        # Processing blocks with controlled concurrency
        with ThreadPoolExecutor(max_workers=max_concurrent_blocks) as executor:
            futures = {}
            for block_id in range(num_blocks):
                future = executor.submit(
                    self.process_single_block_streaming,
                    file_path, storage_name, block_id, num_blocks
                )
                futures[future] = block_id
            
            for future in as_completed(futures):
                block_id = futures[future]
                try:
                    success = future.result()
                    if success:
                        completed_blocks.append(block_id)
                        if len(completed_blocks) % 10 == 0 or len(completed_blocks) == num_blocks:
                            print(f"Progress: {len(completed_blocks)}/{num_blocks} blocks completed")
                    else:
                        failed_blocks.append(block_id)
                        print(f"Block {block_id} failed")
                except Exception as e:
                    failed_blocks.append(block_id)
                    print(f"Block {block_id} threw exception: {e}")

        print(f"\nStreaming Write Summary:")
        print(f"   Successful: {len(completed_blocks)}/{num_blocks} blocks")
        print(f"   Failed: {len(failed_blocks)}/{num_blocks} blocks")
        
        if failed_blocks:
            print(f"   Failed blocks: {sorted(failed_blocks)}")
            return False

        print(f"All blocks streamed successfully!")
        return self.store_metadata(filename, num_blocks, self.block_size)

    def process_single_block_streaming(self, file_path, storage_name, block_id, total_blocks, max_retries=3):
        """Process a single block with comprehensive retry logic"""
        exclude_nodes = set()
        
        for attempt in range(max_retries):
            try:
                block_data = self.read_block_from_file(file_path, block_id)
                if not block_data:
                    return False

                def get_datanode():
                    with self.exclude_nodes_lock:
                        current_excludes = exclude_nodes.copy()
                    
                    datanode_addr = self.get_active_datanode(current_excludes)
                    if not datanode_addr:
                        raise Exception("No active DataNode available")
                    return datanode_addr
                
                try:
                    datanode_addr = self.retry_with_backoff(
                        get_datanode,
                        max_retries=2,
                        base_delay=0.5,
                        max_delay=3.0
                    )
                except:
                    print(f"No DataNode available for block {block_id} on attempt {attempt + 1}")
                    continue
                
                # Sending block with timeout
                timeout = max(10, len(block_data) // (512 * 1024) + 5)
                success = self.send_block_to_datanode_with_timeout(
                    datanode_addr, block_data, storage_name, block_id, total_blocks, timeout
                )
                
                if success:
                    return True
                else:
                    exclude_nodes.add(datanode_addr)
                    print(f"Failed to send block {block_id} to {datanode_addr}, excluding from future attempts")
                    
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for block {block_id}: {e}")
            
            # Progressive backoff
            if attempt < max_retries - 1:
                backoff_delay = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                time.sleep(backoff_delay)
        
        return False


    def write_block_with_retry_thread_safe(self, block_data, storage_name, block_id, total_blocks, exclude_nodes, max_retries=3):
        """Thread-safe block write with retry logic using storage_name"""
        for attempt in range(max_retries):
            try:
                # Thread-safe access to exclude list
                with self.exclude_nodes_lock:
                    current_excludes = exclude_nodes.copy()
                
                datanode_addr = self.get_active_datanode(current_excludes)
                if not datanode_addr:
                    print(f"No active DataNode available for block {block_id}")
                    time.sleep(0.5 * (attempt + 1))
                    continue
                
                timeout = 10 if len(block_data) == self.block_size else 15
                
                if self.send_block_to_datanode_with_timeout(datanode_addr, block_data, storage_name, block_id, total_blocks, timeout):
                    return True
                else:
                    # Thread-safe addition to exclusion list on failure
                    with self.exclude_nodes_lock:
                        exclude_nodes.add(datanode_addr)
                    
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for block {block_id}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(0.5 * (2 ** attempt))
        
        return False

    def send_block_to_datanode_with_timeout(self, datanode_addr, block_data, storage_name, block_id, total_blocks, timeout=15):
        """Send block with configurable timeout using storage_name"""
        sock = None
        try:
            print(f"    Block {block_id}: Connecting to {datanode_addr} ({len(block_data)} bytes, timeout={timeout}s)...")
            sock = self.get_fresh_connection(datanode_addr, timeout)
            if not sock:
                return False

            print(f"    Block {block_id}: Connected! Sending metadata...")
            metadata = f"write_block {storage_name} {block_id} {total_blocks}"
            self.send_message(sock, metadata)

            print(f"    Block {block_id}: Sending {len(block_data)} bytes of block data...")
            self.send_message(sock, block_data)

            print(f"    Block {block_id}: Waiting for response...")
            response_data = self.recv_message(sock)
            if response_data:
                response = response_data.decode()
                if response == "success":
                    print(f"    Block {block_id}: DataNode confirmed success")
                    return True
                else:
                    print(f"    Block {block_id}: DataNode failed: {response}")
                    return False
            else:
                print(f"    Block {block_id}: No response from DataNode")
                return False
                    
        except Exception as e:
            print(f"    Block {block_id}: Error sending to DataNode {datanode_addr}: {e}")
            return False
        finally:
            if sock:
                sock.close()

    def retry_with_backoff(self, func, max_retries=3, base_delay=1.0, max_delay=30.0):
        """Generic retry function with exponential backoff and jitter"""
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(0, delay * 0.1)
                total_delay = delay + jitter
                
                print(f"Retry attempt {attempt + 1}/{max_retries} failed: {e}")
                print(f"Retrying in {total_delay:.2f} seconds...")
                time.sleep(total_delay)
        
        return None

    def create_directory(self, path):
        """Create a directory in the distributed file system"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.namenode_host, self.namenode_port))
                message = f"mkdir {path}"
                s.sendall(message.encode())
                response = s.recv(1024).decode()
                
                if response.startswith("mkdir_result"):
                    parts = response.split(' ', 2)
                    success = parts[1] == "True"
                    msg = parts[2] if len(parts) > 2 else ""
                    
                    if success:
                        print(f"Successfully created directory: {path}")
                        return True
                    else:
                        print(f"Failed to create directory: {msg}")
                        return False
                else:
                    print(f"Unexpected response: {response}")
                    return False
        except Exception as e:
            print(f"Error creating directory: {e}")
            return False

    def list_directory(self, path=None):
        """List contents of a directory"""
        if path is None:
            path = self.current_dir
            
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.namenode_host, self.namenode_port))
                message = f"ls {path}"
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                
                if response.startswith("ls_result success"):
                    try:
                        prefix = "ls_result success "
                        if response.startswith(prefix):
                            json_data = response[len(prefix):].strip()
                            json_data = json_data.strip()
                            contents = json.loads(json_data)
                            
                            print(f"\nContents of '{path}':")
                            print("-" * 60)
                            for item in contents:
                                item_type = "DIR" if item['type'] == 'directory' else "FILE"
                                created_time = datetime.fromtimestamp(float(item['created'])).strftime('%Y-%m-%d %H:%M:%S')
                                
                                if item['type'] == 'file':
                                    size_str = f"{int(item.get('size', 0)):,} bytes"
                                    print(f"{item_type:<4} {item['name']:<25} {size_str:<15} {created_time}")
                                else:
                                    print(f"{item_type:<4} {item['name']:<25} {'':15} {created_time}")
                            print("-" * 60)
                            return contents
                        else:
                            print("Error: Invalid response format - no JSON data found")
                            return None
                    except json.JSONDecodeError as e:
                        print(f"Error parsing directory listing: {e}")
                        print(f"Raw response: {response}")
                        return None
                elif response.startswith("ls_result error"):
                    error_msg = response.split(' ', 2)[2] if len(response.split(' ', 2)) > 2 else "Unknown error"
                    print(f"Error listing directory: {error_msg}")
                    return None
                else:
                    print(f"Unexpected response format: {response}")
                    return None
        except Exception as e:
            print(f"Error listing directory: {e}")
            return None

    def change_directory(self, path):
        """Change the current working directory"""
        if path == "..":
            if self.current_dir != "/":
                self.current_dir = "/".join(self.current_dir.rstrip("/").split("/")[:-1]) or "/"
            return True
        elif path.startswith("/"):
            if self.path_exists(path):
                self.current_dir = self.normalize_path(path)
                return True
            else:
                print(f"Directory '{path}' does not exist")
                return False
        else:
            new_path = os.path.join(self.current_dir, path).replace("\\", "/")
            normalized_path = self.normalize_path(new_path)
            if self.path_exists(normalized_path):
                self.current_dir = normalized_path
                return True
            else:
                print(f"Directory '{normalized_path}' does not exist")
                return False
    
    def delete_block_from_datanode(self, datanode_addr, storage_name, block_id):
        """Delete a specific block from a DataNode"""
        try:
            sock = self.get_fresh_connection(datanode_addr, timeout=10)
            if not sock:
                return False
            
            metadata = f"delete_block {storage_name} {block_id}"
            metadata_bytes = metadata.encode()
            
            sock.sendall(len(metadata_bytes).to_bytes(4, 'big'))
            sock.sendall(metadata_bytes)
            
            response = sock.recv(1024).decode()
            sock.close()
            
            return response in ["success", "block_not_found"]  # Both are acceptable
            
        except Exception as e:
            print(f"Error deleting block {block_id} from {datanode_addr}: {e}")
            return False

    def delete_blocks_from_datanodes(self, blocks_info):
        """Delete blocks from DataNodes in parallel"""
        if not blocks_info:
            return True
        
        print(f"Deleting {len(blocks_info)} blocks from DataNodes...")
        
        futures = []
        for block_info in blocks_info:
            storage_name = block_info['storage_name']
            block_id = block_info['block_id']
            locations = block_info['locations']
            
            for datanode_addr in locations:
                future = self.executor.submit(
                    self.delete_block_from_datanode,
                    datanode_addr, storage_name, block_id
                )
                futures.append((future, datanode_addr, block_id))
        
        successful_deletions = 0
        for future, datanode_addr, block_id in futures:
            try:
                success = future.result(timeout=10)
                if success:
                    successful_deletions += 1
                    print(f"  Successfully deleted {block_id} from {datanode_addr}")
                else:
                    print(f"  Failed to delete {block_id} from {datanode_addr}")
            except Exception as e:
                print(f"  Error deleting {block_id} from {datanode_addr}: {e}")
        
        print(f"Block deletion summary: {successful_deletions}/{len(futures)} operations successful")
        return successful_deletions > 0

    def delete_file(self, filename):
        """Delete a file from the distributed file system"""
        try:
            full_path = filename if '/' in filename else os.path.join(self.current_dir, filename)
            full_path = self.normalize_path(full_path)
            
            print(f"Are you sure you want to delete file '{full_path}'? (y/n): ", end='')
            confirmation = input().strip().lower()
            
            if confirmation != 'y':
                print("Deletion cancelled.")
                return False
            
            print(f"Deleting file {full_path}...")
            
            # Requesting deletion from NameNode
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.namenode_host, self.namenode_port))
                message = f"delete_file {filename} {self.current_dir}"
                s.sendall(message.encode())
                response = s.recv(8192).decode()
                
                if response.startswith("delete_file_result success"):
                    json_part = response[len("delete_file_result success "):]
                    blocks_info = json.loads(json_part)

                    if blocks_info:
                        blocks_deleted = self.delete_blocks_from_datanodes(blocks_info)
                        if blocks_deleted:
                            print(f"Successfully deleted file '{full_path}' and its blocks")
                        else:
                            print(f"File metadata deleted, but some blocks may remain on DataNodes")
                    else:
                        print(f"Successfully deleted file '{full_path}' (no blocks to clean up)")
                    
                    return True
                    
                elif response.startswith("delete_file_result error"):
                    error_msg = response[len("delete_file_result error "):]
                    print(f"Failed to delete file: {error_msg}")
                    return False
                else:
                    print(f"Unexpected response: {response}")
                    return False
                    
        except Exception as e:
            print(f"Error deleting file: {e}")
            return False

    def delete_directory(self, path):
        """Delete a directory and all its contents"""
        try:
            path = self.normalize_path(path)
            
            if path == "/":
                print("Cannot delete root directory")
                return False
            
            contents = self.list_directory(path)
            if contents is None:
                print(f"Directory '{path}' does not exist or cannot be read")
                return False
            
            file_count = sum(1 for item in contents if item['type'] == 'file')
            dir_count = sum(1 for item in contents if item['type'] == 'directory')
            
            print(f"\nDirectory '{path}' contains:")
            print(f"  - {file_count} file(s)")
            print(f"  - {dir_count} subdirectory(ies)")
            print(f"\nThis will permanently delete the directory and ALL its contents.")
            print(f"Are you sure you want to delete '{path}' and everything in it? (y/n): ", end='')
            
            confirmation = input().strip().lower()
            if confirmation != 'y':
                print("Deletion cancelled.")
                return False
            
            print(f"\nDeleting directory '{path}' and all contents...")
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.namenode_host, self.namenode_port))
                message = f"delete_directory {path}"
                s.sendall(message.encode())
                response = s.recv(16384).decode()  # Large buffer for comprehensive response
                
                if response.startswith("delete_directory_result success"):
                    json_part = response[len("delete_directory_result success "):]
                    result = json.loads(json_part)
                    
                    blocks_info = result['blocks_info']
                    deleted_files = result['deleted_files']
                    deleted_directories = result['deleted_directories']
                    
                    print(f"Metadata deletion completed:")
                    print(f"  - {deleted_files} files")
                    print(f"  - {deleted_directories} directories")
                    
                    if blocks_info:
                        print(f"\nCleaning up {len(blocks_info)} blocks from DataNodes...")
                        blocks_deleted = self.delete_blocks_from_datanodes(blocks_info)
                        if blocks_deleted:
                            print(f"Successfully deleted directory '{path}' and all its data")
                        else:
                            print(f"Directory deleted, but some blocks may remain on DataNodes")
                    else:
                        print(f"Successfully deleted directory '{path}' (no blocks to clean up)")
                    
                    return True
                    
                elif response.startswith("delete_directory_result error"):
                    error_msg = response[len("delete_directory_result error "):]
                    print(f"Failed to delete directory: {error_msg}")
                    return False
                else:
                    print(f"Unexpected response: {response}")
                    return False
                    
        except KeyboardInterrupt:
            print("\nDeletion cancelled by user")
            return False
        except Exception as e:
            print(f"Error deleting directory: {e}")
            return False

    def path_exists(self, path):
        """Check if a path exists in the file system"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.namenode_host, self.namenode_port))
                message = f"exists {path}"
                s.sendall(message.encode())
                response = s.recv(1024).decode()
                
                if response.startswith("exists_result"):
                    exists = response.split(' ')[1] == "True"
                    return exists
                else:
                    print(f"Unexpected response: {response}")
                    return False
        except Exception as e:
            print(f"Error checking path existence: {e}")
            return False

    def __del__(self):
        """Cleanup thread pool on object destruction"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)
    
if __name__ == "__main__":
    client = Client()
    
    """ Only for quick testing purposes """
    print("=== WRITE OPERATION ===")
    success = client.write_file("")
    
    if success:
        print("\n=== READ OPERATION ===")
        client.read_file("", "")
    else:
        print("\nWrite failed, skipping read operation")