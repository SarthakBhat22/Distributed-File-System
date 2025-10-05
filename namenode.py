import socket
import threading
import time
import random
import json
from datetime import datetime, timedelta
import redis
from collections import defaultdict, OrderedDict
import os


class NameNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.datanodes = {}
        self.server_socket = None
        self.heartbeat_timeout = 30
        self.last_status_print = datetime.now()
        self.status_print_interval = 10
        self.start_time = time.time()
        
        self.metadata_cache = OrderedDict()
        self.max_cache_size = 1000
        self.cache_lock = threading.Lock()
        
        self.datanode_load = defaultdict(int)
        self.load_lock = threading.Lock()
        
        self.dir_lock = threading.Lock()
        self.datanodes_lock = threading.Lock()

        self.redis_client = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=True,
            db=0
        )

        self.init_directory_structure()

        self.monitor_thread = threading.Thread(target=self.monitor_heartbeats)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def init_directory_structure(self):
        """Initialize the root directory structure in Redis if it doesn't exist"""
        try:
            root_exists = self.redis_client.hexists('directories', '/')
            if not root_exists:
                root_dir = {
                    'type': 'directory',
                    'created': time.time(),
                    'children': {},
                    'files': {}  # Track files in this directory
                }
                self.redis_client.hset('directories', '/', json.dumps(root_dir))
                print("Initialized root directory structure")
        except Exception as e:
            print(f"Error initializing directory structure: {e}")

    def normalize_path(self, path):
        """Normalize a path to handle . and .. components"""
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
        
        normalized = '/' + '/'.join(components) if components else '/'
        return normalized

    def get_parent_path(self, path):
        """Get the parent directory path"""
        if path == '/':
            return None
        return os.path.dirname(path) or '/'

    def path_to_key(self, path):
        """Convert a file path to a safe Redis key"""
        # Replace path separators with a safe delimiter
        return path.replace('/', '__')

    def key_to_path(self, key):
        """Convert a Redis key back to a file path"""
        return key.replace('__', '/')

    def get_directory_data(self, path):
        """Get directory data from Redis"""
        try:
            path = self.normalize_path(path)
            dir_json = self.redis_client.hget('directories', path)
            if dir_json:
                return json.loads(dir_json)
            return None
        except Exception as e:
            print(f"Error getting directory data for {path}: {e}")
            return None

    def save_directory_data(self, path, dir_data):
        """Save directory data to Redis"""
        try:
            path = self.normalize_path(path)
            self.redis_client.hset('directories', path, json.dumps(dir_data))
            return True
        except Exception as e:
            print(f"Error saving directory data for {path}: {e}")
            return False

    def path_exists(self, path):
        """Check if a path (directory or file) exists"""
        path = self.normalize_path(path)

        if self.redis_client.hexists('directories', path):
            return True

        file_key = self.path_to_key(path)
        if self.redis_client.hexists('files', file_key):
            return True
            
        return False

    def create_directory(self, path):
        """Create a directory at the specified path"""
        try:
            path = self.normalize_path(path)
            
            with self.dir_lock:
                if self.path_exists(path):
                    return False, "Directory already exists"

                parent_path = self.get_parent_path(path)
                if parent_path and not self.path_exists(parent_path):
                    return False, "Parent directory does not exist"
                
                dir_data = {
                    'type': 'directory',
                    'created': time.time(),
                    'children': {},
                    'files': {}
                }
                
                if not self.save_directory_data(path, dir_data):
                    return False, "Failed to save directory data"

                if parent_path:
                    parent_data = self.get_directory_data(parent_path)
                    if parent_data:
                        dir_name = os.path.basename(path)
                        parent_data['children'][dir_name] = {
                            'type': 'directory',
                            'created': time.time()
                        }
                        self.save_directory_data(parent_path, parent_data)
                
                print(f"Created directory: {path}")
                return True, "Directory created successfully"
                
        except Exception as e:
            print(f"Error creating directory: {e}")
            return False, str(e)

    def list_directory(self, path):
        """List contents of a directory"""
        try:
            path = self.normalize_path(path)
            
            with self.dir_lock:
                dir_data = self.get_directory_data(path)
                if not dir_data:
                    return None, f"Directory '{path}' does not exist"
                
                contents = []
                
                for name, info in dir_data.get('children', {}).items():
                    contents.append({
                        'name': name,
                        'type': info['type'],
                        'created': float(info['created'])  # To ensure float type
                    })
                
                for name, info in dir_data.get('files', {}).items():
                    contents.append({
                        'name': name,
                        'type': 'file',
                        'created': float(info['created']),  # float type
                        'size': int(info.get('size', 0))    # int type
                    })
                
                return contents, "Success"
                
        except Exception as e:
            print(f"Error listing directory {path}: {e}")
            return None, f"Internal error: {str(e)}"

    def register_file_in_directory(self, file_path, file_metadata):
        """Register a file in its parent directory"""
        try:
            file_path = self.normalize_path(file_path)
            parent_path = self.get_parent_path(file_path)
            filename = os.path.basename(file_path)
            
            if parent_path is None:
                parent_path = '/'
            
            with self.dir_lock:
                parent_data = self.get_directory_data(parent_path)
                if not parent_data:
                    parent_data = {
                        'type': 'directory',
                        'created': time.time(),
                        'children': {},
                        'files': {}
                    }
                
                parent_data['files'][filename] = {
                    'type': 'file',
                    'created': file_metadata.get('creation_time', time.time()),
                    'size': file_metadata.get('total_size', 0)
                }
                
                self.save_directory_data(parent_path, parent_data)
                
                print(f"Registered file {filename} in directory {parent_path}")
                return True
                
        except Exception as e:
            print(f"Error registering file in directory: {e}")
            return False

    def get_file_full_path(self, filename, current_dir="/"):
        """Get the full path of a file, searching from current directory"""
        try:
            current_dir = self.normalize_path(current_dir)
            
            if '/' in filename:
                if filename.startswith('/'):
                    full_path = self.normalize_path(filename)
                else:
                    full_path = self.normalize_path(os.path.join(current_dir, filename))
                
                if self.path_exists(full_path):
                    return full_path
            else:
                full_path = os.path.join(current_dir, filename)
                if self.path_exists(full_path):
                    return full_path
            
            return None
            
        except Exception as e:
            print(f"Error getting file full path: {e}")
            return None

    def store_file_metadata(self, filename, block_count, block_size, blocks_metadata=None, current_dir="/"):
        """Store file metadata with directory integration and path-based keys"""
        try:
            if '/' in filename:
                full_path = self.normalize_path(filename)
                actual_filename = os.path.basename(full_path)
            else:
                full_path = os.path.join(self.normalize_path(current_dir), filename)
                actual_filename = filename

            file_key = self.path_to_key(full_path)
            
            if blocks_metadata is None:
                blocks = []
                total_size = 0
                active_datanodes = list(self.datanodes.keys())

                if not active_datanodes:
                    raise Exception("No active DataNodes available")

                for i in range(block_count):
                    locations = self.get_optimal_datanodes(i, 3)
                    current_block_size = block_size if i < block_count-1 else block_size//2
                    total_size += current_block_size

                    block = {
                        'block_id': f'block_{i}',
                        'size': current_block_size,
                        'locations': locations,
                        'timestamp': time.time()
                    }
                    blocks.append(block)
            else:
                blocks = blocks_metadata
                total_size = sum(block['size'] for block in blocks)

            file_metadata = {
                'filename': actual_filename,
                'full_path': full_path,
                'storage_name': file_key,  # For DataNode block storage
                'blocks': blocks,
                'total_size': total_size,
                'creation_time': time.time()
            }

            self.redis_client.hset('files', file_key, json.dumps(file_metadata))
            self.cache_metadata(file_key, file_metadata)

            self.register_file_in_directory(full_path, file_metadata)
            
            print(f"Successfully stored metadata for {actual_filename} at {full_path} with key {file_key}")
            return True

        except Exception as e:
            print(f"Error storing metadata: {e}")
            return False

    def get_file_metadata(self, filename, current_dir="/"):
        """Retrieve file metadata with directory-aware lookup and path-based keys"""
        try:
            full_path = self.get_file_full_path(filename, current_dir)
            if not full_path:
                print(f"File {filename} not found in directory {current_dir}")
                return None
            
            file_key = self.path_to_key(full_path)
            
            # Cache first
            cached_metadata = self.get_cached_metadata(file_key)
            if cached_metadata:
                with self.datanodes_lock:
                    active_datanodes = set(self.datanodes.keys())
                
                for block in cached_metadata['blocks']:
                    block['locations'] = [
                        loc for loc in block['locations']
                        if loc in active_datanodes
                    ]
                    
                    if not block['locations']:
                        print(f"Block {block['block_id']} has no active DataNodes")
                        return None
                
                return json.dumps(cached_metadata)
            
            # Fallback to Redis
            metadata_json = self.redis_client.hget('files', file_key)
            if metadata_json:
                metadata = json.loads(metadata_json)
                with self.datanodes_lock:
                    active_datanodes = set(self.datanodes.keys())

                for block in metadata['blocks']:
                    block['locations'] = [
                        loc for loc in block['locations']
                        if loc in active_datanodes
                    ]

                    if not block['locations']:
                        print(f"Block {block['block_id']} has no active DataNodes")
                        return None

                self.cache_metadata(file_key, metadata)
                return json.dumps(metadata)
                
            return None
        except Exception as e:
            print(f"Error retrieving metadata: {e}")
            return None
    
    def delete_file(self, filename, current_dir="/"):
        """Delete a file and all its blocks"""
        try:
            full_path = self.get_file_full_path(filename, current_dir)
            if not full_path:
                return False, f"File '{filename}' not found"
            
            file_key = self.path_to_key(full_path)
            
            with self.dir_lock:
                metadata_json = self.redis_client.hget('files', file_key)
                if not metadata_json:
                    return False, f"File metadata not found"
                
                metadata = json.loads(metadata_json)
                
                parent_path = self.get_parent_path(full_path)
                actual_filename = os.path.basename(full_path)
                
                if parent_path:
                    parent_data = self.get_directory_data(parent_path)
                    if parent_data and 'files' in parent_data:
                        parent_data['files'].pop(actual_filename, None)
                        self.save_directory_data(parent_path, parent_data)
                
                self.redis_client.hdel('files', file_key)
                
                with self.cache_lock:
                    self.metadata_cache.pop(file_key, None)
                
                print(f"Successfully deleted file metadata for {full_path}")
                
                blocks_info = []
                for block in metadata.get('blocks', []):
                    blocks_info.append({
                        'block_id': block['block_id'],
                        'locations': block['locations'],
                        'storage_name': metadata['storage_name']
                    })
                
                return True, blocks_info
                
        except Exception as e:
            print(f"Error deleting file: {e}")
            return False, f"Internal error: {str(e)}"

    def get_directory_contents_recursive(self, path):
        """Get all files and directories under a path recursively"""
        try:
            path = self.normalize_path(path)
            all_items = {'files': [], 'directories': []}
            
            def collect_items(current_path):
                dir_data = self.get_directory_data(current_path)
                if not dir_data:
                    return
                
                for filename, file_info in dir_data.get('files', {}).items():
                    file_path = os.path.join(current_path, filename).replace('\\', '/')
                    file_path = self.normalize_path(file_path)
                    all_items['files'].append(file_path)
                
                for dirname, dir_info in dir_data.get('children', {}).items():
                    subdir_path = os.path.join(current_path, dirname).replace('\\', '/')
                    subdir_path = self.normalize_path(subdir_path)
                    all_items['directories'].append(subdir_path)
                    collect_items(subdir_path)
            
            collect_items(path)
            return all_items
            
        except Exception as e:
            print(f"Error getting directory contents: {e}")
            return None

    def delete_directory(self, path):
        """Delete a directory and all its contents"""
        try:
            path = self.normalize_path(path)
            
            if path == "/":
                return False, "Cannot delete root directory"
            
            with self.dir_lock:
                if not self.path_exists(path):
                    return False, f"Directory '{path}' does not exist"
                
                contents = self.get_directory_contents_recursive(path)
                if contents is None:
                    return False, "Failed to get directory contents"
                
                all_blocks_info = []
                
                for file_path in contents['files']:
                    file_key = self.path_to_key(file_path)
                    
                    metadata_json = self.redis_client.hget('files', file_key)
                    if metadata_json:
                        metadata = json.loads(metadata_json)
                        for block in metadata.get('blocks', []):
                            all_blocks_info.append({
                                'block_id': block['block_id'],
                                'locations': block['locations'],
                                'storage_name': metadata['storage_name']
                            })

                    self.redis_client.hdel('files', file_key)
                    
                    # Removing from cache
                    with self.cache_lock:
                        self.metadata_cache.pop(file_key, None)
                
                all_dirs = contents['directories'] + [path]
                for dir_path in reversed(sorted(all_dirs)):
                    self.redis_client.hdel('directories', dir_path)
                
                # Removing from parent directory
                parent_path = self.get_parent_path(path)
                if parent_path:
                    parent_data = self.get_directory_data(parent_path)
                    if parent_data:
                        dirname = os.path.basename(path)
                        parent_data['children'].pop(dirname, None)
                        self.save_directory_data(parent_path, parent_data)
                
                return True, {
                    'blocks_info': all_blocks_info,
                    'deleted_files': len(contents['files']),
                    'deleted_directories': len(all_dirs)
                }
                
        except Exception as e:
            print(f"Error deleting directory: {e}")
            return False, f"Internal error: {str(e)}"

    def get_optimal_datanodes(self, block_id, num_replicas=3):
        """Get optimal DataNodes for block placement using load balancing"""
        with self.load_lock:
            with self.datanodes_lock:
                active_datanodes = list(self.datanodes.keys())
            
            if not active_datanodes:
                return []
            
            sorted_nodes = sorted(active_datanodes, key=lambda x: self.datanode_load[x])
            selected_nodes = sorted_nodes[:num_replicas]
            
            for node in selected_nodes:
                self.datanode_load[node] += 1
                
            return selected_nodes

    def get_cached_metadata(self, file_key):
        """Get metadata from cache if available"""
        with self.cache_lock:
            if file_key in self.metadata_cache:
                metadata = self.metadata_cache.pop(file_key)
                self.metadata_cache[file_key] = metadata
                return metadata
        return None

    def cache_metadata(self, file_key, metadata):
        """Cache metadata with LRU eviction"""
        with self.cache_lock:
            if file_key in self.metadata_cache:
                self.metadata_cache.pop(file_key)
            elif len(self.metadata_cache) >= self.max_cache_size:
                self.metadata_cache.popitem(last=False)
            
            self.metadata_cache[file_key] = metadata

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"NameNode listening on {self.host}:{self.port}")

        while True:
            try:
                datanode_socket, addr = self.server_socket.accept()
                print(f"\nGot connection from {addr}")
                datanode_thread = threading.Thread(
                    target=self.handle_datanode_registration,
                    args=(datanode_socket,)
                )
                datanode_thread.start()
            except Exception as e:
                print(f"Error accepting connection: {e}")

    def handle_datanode_registration(self, datanode_socket):
        try:
            data = datanode_socket.recv(1024)
            message = data.decode()

            if message.startswith("register"):
                datanode_addr = message.split()[1]
                self.register_datanode(datanode_addr)
                response = f"DataNode {datanode_addr} registered"
            elif message.startswith("heartbeat"):
                datanode_addr = message.split()[1]
                self.update_heartbeat(datanode_addr)
                response = "Heartbeat acknowledged"
            elif message == "get_datanodes":
                with self.datanodes_lock:
                    datanodes = " ".join(self.datanodes.keys())
                response = f"datanodes {datanodes}"
            elif message == "get_datanode":
                with self.datanodes_lock:
                    if self.datanodes:
                        datanode = random.choice(list(self.datanodes.keys()))
                        response = f"datanode {datanode}"
                    else:
                        response = "no_datanode_available"
            elif message.startswith("store_metadata"):
                parts = message.split()
                if len(parts) >= 4:
                    _, filename, block_count, block_size = parts[:4]
                    current_dir = parts[4] if len(parts) > 4 else "/"
                    success = self.store_file_metadata(
                        filename, 
                        int(block_count), 
                        int(block_size),
                        current_dir=current_dir
                    )
                    response = "success" if success else "error"
                else:
                    response = "error: insufficient parameters"
            elif message.startswith("get_metadata"):
                parts = message.split()
                if len(parts) >= 2:
                    _, filename = parts[:2]
                    current_dir = parts[2] if len(parts) > 2 else "/"
                    metadata = self.get_file_metadata(filename, current_dir)
                    if metadata:
                        response = f"metadata {metadata}"
                    else:
                        response = "file_not_found"
                else:
                    response = "error: insufficient parameters"
            elif message.startswith("mkdir"):
                _, path = message.split(' ', 1)
                success, msg = self.create_directory(path)
                response = f"mkdir_result {success} {msg}"
            elif message.startswith("ls"):
                try:
                    parts = message.split(' ', 1)
                    path = parts[1] if len(parts) > 1 else "/"
                    contents, msg = self.list_directory(path)
                    if contents is not None:
                        # Ensuring clean JSON serialization
                        json_data = json.dumps(contents, ensure_ascii=False, separators=(',', ':'))
                        response = f"ls_result success {json_data}"
                    else:
                        response = f"ls_result error {msg}"
                except Exception as e:
                    response = f"ls_result error Failed to list directory: {str(e)}"
            elif message.startswith("exists"):
                _, path = message.split(' ', 1)
                exists = self.path_exists(path)
                response = f"exists_result {exists}"
            elif message.startswith("delete_file"):
                parts = message.split(' ', 2)
                if len(parts) >= 2:
                    filename = parts[1]
                    current_dir = parts[2] if len(parts) > 2 else "/"
                    success, result = self.delete_file(filename, current_dir)
                    if success:
                        response = f"delete_file_result success {json.dumps(result)}"
                    else:
                        response = f"delete_file_result error {result}"
                else:
                    response = "delete_file_result error insufficient parameters"
            elif message.startswith("delete_directory"):
                parts = message.split(' ', 1)
                if len(parts) >= 2:
                    path = parts[1]
                    success, result = self.delete_directory(path)
                    if success:
                        response = f"delete_directory_result success {json.dumps(result)}"
                    else:
                        response = f"delete_directory_result error {result}"
                else:
                    response = "delete_directory_result error insufficient parameters"
            elif message == "get_metrics":
                self.handle_get_metrics(datanode_socket)
                return
            else:
                response = "Invalid message"

            datanode_socket.sendall(response.encode())
        except Exception as e:
            print(f"Error handling DataNode communication: {e}")
        finally:
            datanode_socket.close()

    def register_datanode(self, datanode_addr):
        with self.datanodes_lock:
            self.datanodes[datanode_addr] = datetime.now()
        print(f"DataNode {datanode_addr} registered")
        self.print_datanode_status()

    def update_heartbeat(self, datanode_addr):
        with self.datanodes_lock:
            if datanode_addr in self.datanodes:
                self.datanodes[datanode_addr] = datetime.now()
            else:
                print(f"Heartbeat from unregistered DataNode {datanode_addr}")
        if datanode_addr not in self.datanodes:
            self.register_datanode(datanode_addr)

    def get_metrics(self):
        """Return current NameNode metrics"""
        try:
            uptime = time.time() - self.start_time
            
            # Count total files
            total_files = len(self.redis_client.hkeys('files'))
            
            # Count total blocks
            total_blocks = 0
            for file_key in self.redis_client.hkeys('files'):
                metadata_json = self.redis_client.hget('files', file_key)
                if metadata_json:
                    metadata = json.loads(metadata_json)
                    total_blocks += len(metadata.get('blocks', []))
            
            with self.datanodes_lock:
                active_datanodes = len(self.datanodes)
            
            return {
                'namenode': f"{self.host}:{self.port}",
                'uptime': uptime,
                'total_files': total_files,
                'total_blocks': total_blocks,
                'active_datanodes': active_datanodes,
            }
        except Exception as e:
            print(f"Error getting metrics: {e}")
            return {
                'namenode': f"{self.host}:{self.port}",
                'uptime': time.time() - self.start_time,
                'total_files': 0,
                'total_blocks': 0,
                'active_datanodes': 0,
            }

    def handle_get_metrics(self, datanode_socket):
        """Handle metrics request from performance monitor"""
        try:
            metrics = self.get_metrics()
            metrics_json = json.dumps(metrics)
            datanode_socket.sendall(metrics_json.encode())
            print(f"Sent metrics to performance monitor")
        except Exception as e:
            print(f"Error sending metrics: {e}")

    def monitor_heartbeats(self):
        while True:
            try:
                current_time = datetime.now()
                dead_nodes = []

                with self.datanodes_lock:
                    datanode_items = list(self.datanodes.items())
                
                # Checking for dead nodes outside the lock to minimize lock time
                for datanode_addr, last_heartbeat in datanode_items:
                    if (current_time - last_heartbeat) > timedelta(seconds=self.heartbeat_timeout):
                        dead_nodes.append(datanode_addr)

                # Removing dead nodes with lock
                if dead_nodes:
                    with self.datanodes_lock:
                        for node in dead_nodes:
                            if node in self.datanodes:
                                print(f"\nDataNode {node} considered dead - no heartbeat for {self.heartbeat_timeout} seconds")
                                del self.datanodes[node]
                    if dead_nodes:
                        self.print_datanode_status()

                if (current_time - self.last_status_print) > timedelta(seconds=self.status_print_interval):
                    self.print_datanode_status()
                    self.last_status_print = current_time

                time.sleep(5)
            except Exception as e:
                print(f"Error in heartbeat monitoring: {e}")

    def print_datanode_status(self):
        print("\nCurrent DataNode Status:")
        print("------------------------")
        current_time = datetime.now()
        
        with self.datanodes_lock:
            datanode_items = list(self.datanodes.items())
        
        for datanode_addr, last_heartbeat in sorted(datanode_items):
            time_diff = (current_time - last_heartbeat).seconds
            print(f"DataNode: {datanode_addr}, Last heartbeat: {time_diff} seconds ago")
        print("------------------------")
        self.last_status_print = datetime.now()


if __name__ == "__main__":
    namenode_host = "localhost"
    namenode_port = 8000
    namenode = NameNode(namenode_host, namenode_port)
    namenode.start()
