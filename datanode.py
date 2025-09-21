import socket
import threading
import time
import os
import argparse
import random

class DataNode:
    def __init__(self, host, port, namenode_host, namenode_port):
        self.host = host
        self.port = port
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.data_dir = f"datanode_{port}"
        self.peer_datanodes = set()
        self.replication_factor = 3
        
        # Performance metrics
        self.blocks_written = 0
        self.blocks_read = 0
        self.start_time = time.time()

        self.peer_datanodes_lock = threading.Lock()
        self.metrics_lock = threading.Lock()

        os.makedirs(self.data_dir, exist_ok=True)
        print(f"Initialized DataNode on port {self.port}")

        self.start_server()

        # Starting heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

        # Initial registration with NameNode
        try:
            self.register_with_namenode()
            print(f"DataNode {self.port} registered with NameNode")
        except Exception as e:
            print(f"Initial registration failed: {e}")

    def start_server(self):
        """Start server socket to receive data from clients and other DataNodes"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(50)
            print(f"DataNode server started on {self.host}:{self.port}")

            # Starting thread to accept connections
            accept_thread = threading.Thread(target=self.accept_connections)
            accept_thread.daemon = True
            accept_thread.start()
        except Exception as e:
            print(f"Error starting server: {e}")

    def accept_connections(self):
        """Accept incoming connections and handle them"""
        while True:
            try:
                client_socket, addr = self.server_socket.accept()
                client_socket.settimeout(30)
                handler_thread = threading.Thread(
                    target=self.handle_connection,
                    args=(client_socket, addr)
                )
                handler_thread.daemon = True
                handler_thread.start()
            except Exception as e:
                print(f"Error accepting connection: {e}")

    def recv_exact(self, sock, size):
        """Receive exactly 'size' bytes from socket"""
        data = b''
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise Exception("Connection closed prematurely")
            data += chunk
        return data

    def handle_connection(self, client_socket, addr):
        """Handle incoming connection from client or peer DataNode"""
        try:
            command_data = self.recv_message(client_socket)
            if not command_data:
                return
                
            metadata = command_data.decode().strip()
            command = metadata.split()[0]

            if command == "write_block":
                self.handle_write_block(client_socket, metadata)
            elif command == "replicate_block":
                self.handle_replicate_block(client_socket, metadata)
            elif command == "read_block":
                self.handle_read_block(client_socket, metadata)
            elif command == "delete_block":
                self.handle_delete_block(client_socket, metadata)
            else:
                print(f"Unknown command: {command}")

        except Exception as e:
            print(f"Error handling connection from {addr}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
    
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

    def handle_read_block(self, client_socket, metadata):
        start_time = time.time()
        try:
            _, storage_name, block_id = metadata.split()
            block_id = block_id.replace('block_', '')
            block_id = int(block_id)

            block_path = os.path.join(self.data_dir, f"{storage_name}.block{block_id}")
            print(f"Reading block: {block_path}")

            if not os.path.exists(block_path):
                print(f"Block not found: {block_path}")
                return

            with open(block_path, 'rb') as f:
                block_data = f.read()
            
            self.send_message(client_socket, block_data)
            
            with self.metrics_lock:
                self.blocks_read += 1
            
            read_time = time.time() - start_time
            print(f"Successfully sent block {block_id} of {storage_name} in {read_time:.3f}s")

        except Exception as e:
            print(f"Error handling read block request: {e}")

    def handle_write_block(self, client_socket, metadata):
        """Handle write block request from client with atomic writes"""
        start_time = time.time()
        try:
            _, storage_name, block_id, total_blocks = metadata.split()
            block_id = int(block_id)

            block_data = self.recv_message(client_socket)
            if not block_data:
                print(f"No data received for block {block_id}")
                self.send_message(client_socket, "error: no data")
                return

            print(f"Received {len(block_data)} bytes for block {block_id}")

            # Use atomic write to prevent corruption
            block_path = os.path.join(self.data_dir, f"{storage_name}.block{block_id}")
            
            try:
                self.atomic_write_block(block_path, block_data)
                print(f"Atomically wrote block {block_id} to {block_path}")
            except Exception as e:
                print(f"Failed to write block {block_id}: {e}")
                self.send_message(client_socket, f"error: write failed - {str(e)}")
                return

            # Update metrics with thread safety
            with self.metrics_lock:
                self.blocks_written += 1
            
            write_time = time.time() - start_time
            
            # Send success response to client
            self.send_message(client_socket, "success")

            replication_thread = threading.Thread(
                target=self.replicate_block_with_retry,
                args=(storage_name, block_id, block_data)
            )
            replication_thread.daemon = True
            replication_thread.start()
            
            print(f"Block {block_id} ({len(block_data)} bytes) written to {block_path} in {write_time:.3f}s")

        except Exception as e:
            print(f"Error handling write block: {e}")
            try:
                self.send_message(client_socket, f"error: {str(e)}")
            except:
                pass

    def atomic_write_block(self, block_path, block_data):
        """Atomically write block data to prevent corruption during failures"""
        temp_path = block_path + '.tmp'
        try:
            with open(temp_path, 'wb') as f:
                f.write(block_data)
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(temp_path, block_path)
            return True
            
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            raise e

    def retry_with_backoff(self, func, max_retries=3, base_delay=1.0, max_delay=30.0):
        """Generic retry function with exponential backoff and jitter"""
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = random.uniform(0, delay * 0.1)  # 10% jitter
                total_delay = delay + jitter
                
                print(f"Retry attempt {attempt + 1}/{max_retries} failed: {e}")
                print(f"Retrying in {total_delay:.2f} seconds...")
                time.sleep(total_delay)
        
        return None

    def replicate_block(self, storage_name, block_id, block_data):
        """Replicate block to other DataNodes with retry logic"""
        try:
            def get_nodes():
                nodes = self.get_available_datanodes()
                if not nodes:
                    raise Exception("No DataNodes available")
                return nodes
            
            available_nodes = self.retry_with_backoff(
                get_nodes,
                max_retries=3,
                base_delay=0.5
            )
            
            if not available_nodes:
                print(f"No DataNodes available for replication after retries")
                return

            my_addr = f"{self.host}:{self.port}"
            with self.peer_datanodes_lock:
                available_nodes.discard(my_addr)

            if not available_nodes:
                print(f"No other DataNodes available for replication of block {block_id}")
                return

            replication_nodes = random.sample(
                list(available_nodes),
                min(self.replication_factor - 1, len(available_nodes))
            )

            successful_replications = 0
            for node in replication_nodes:
                if self.send_replica_to_node_with_retry(node, storage_name, block_id, block_data):
                    successful_replications += 1

            print(f"Successfully replicated block {block_id} to {successful_replications}/{len(replication_nodes)} nodes")

        except Exception as e:
            print(f"Error in replication process: {e}")
    
    def replicate_block_with_retry(self, storage_name, block_id, block_data):
        """Replicate block with retry logic and backoff"""
        def attempt_replication():
            return self.replicate_block(storage_name, block_id, block_data)
        
        try:
            self.retry_with_backoff(
                attempt_replication,
                max_retries=3,
                base_delay=1.0,
                max_delay=10.0
            )
        except Exception as e:
            print(f"Failed to replicate block {block_id} after all retries: {e}")

    def send_replica_to_node_with_retry(self, node, storage_name, block_id, block_data):
        """Send replica with retry logic"""
        def attempt_send():
            success = self.send_replica_to_node(node, storage_name, block_id, block_data)
            if not success:
                raise Exception(f"Failed to send replica to {node}")
            return success
        
        try:
            return self.retry_with_backoff(
                attempt_send,
                max_retries=2,
                base_delay=0.5,
                max_delay=5.0
            )
        except Exception as e:
            print(f"Failed to send replica to {node} after retries: {e}")
            return False

    def send_replica_to_node(self, node, storage_name, block_id, block_data):
        """Send a replica to a specific node using storage name"""
        try:
            host, port = node.split(':')
            port = int(port)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)
                s.connect((host, port))

                metadata = f"replicate_block {storage_name} {block_id}"
                self.send_message(s, metadata)

                self.send_message(s, block_data)
                
                s.settimeout(5)
                response_data = self.recv_message(s)
                if response_data:
                    response = response_data.decode()
                    if response == "success":
                        print(f"Successfully replicated block {block_id} to {node}")
                        return True
                
                return False

        except Exception as e:
            print(f"Error sending replica to {node}: {e}")
            return False

    def handle_replicate_block(self, client_socket, metadata):
        """Handle replication request from another DataNode using storage name"""
        try:
            _, storage_name, block_id = metadata.split()
            block_id = int(block_id)
            
            block_data = self.recv_message(client_socket)
            if not block_data:
                self.send_message(client_socket, "error: no data")
                return

            block_path = os.path.join(self.data_dir, f"{storage_name}.block{block_id}")
            with open(block_path, 'wb') as f:
                f.write(block_data)

            self.send_message(client_socket, "success")
            print(f"Successfully received replica of block {block_id} ({len(block_data)} bytes) as {block_path}")

        except Exception as e:
            print(f"Error handling replication: {e}")
            try:
                self.send_message(client_socket, f"error: {str(e)}")
            except:
                pass
    
    def handle_delete_block(self, client_socket, metadata):
        """Handle delete block request from NameNode"""
        try:
            parts = metadata.split()
            if len(parts) < 3:
                client_socket.sendall("error: insufficient parameters".encode())
                return
            
            _, storage_name, block_id = parts
            block_id = block_id.replace('block_', '')
            
            block_path = os.path.join(self.data_dir, f"{storage_name}.block{block_id}")
            
            if os.path.exists(block_path):
                os.remove(block_path)
                print(f"Successfully deleted block: {block_path}")
                client_socket.sendall("success".encode())
            else:
                print(f"Block not found: {block_path}")
                client_socket.sendall("block_not_found".encode())
                
        except Exception as e:
            print(f"Error deleting block: {e}")
            try:
                client_socket.sendall(f"error: {str(e)}".encode())
            except:
                pass

    def get_available_datanodes(self):
        """Get list of available DataNodes from NameNode"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.namenode_host, self.namenode_port))
                s.sendall("get_datanodes".encode())
                response = s.recv(1024).decode()
                nodes = response.split()[1:] if response.startswith("datanodes") else []
                
                with self.peer_datanodes_lock:
                    self.peer_datanodes.update(nodes)
                
                return set(nodes)
        except Exception as e:
            print(f"Error getting available DataNodes: {e}")
            return set()

    def send_heartbeat(self):
        """Send periodic heartbeat signals with retry logic"""
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        while True:
            def attempt_heartbeat():
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((self.namenode_host, self.namenode_port))
                    heartbeat_msg = f"heartbeat {self.host}:{self.port}"
                    s.sendall(heartbeat_msg.encode())
                    response = s.recv(1024).decode()
                    return response
            
            try:
                response = self.retry_with_backoff(
                    attempt_heartbeat,
                    max_retries=3,
                    base_delay=1.0,
                    max_delay=10.0
                )
                
                consecutive_failures = 0 
                
                if int(time.time()) % 60 == 0:
                    print(f"Heartbeat confirmed: {response}")
                    
            except Exception as e:
                consecutive_failures += 1
                print(f"Heartbeat failed (attempt {consecutive_failures}/{max_consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    print("Too many consecutive heartbeat failures - attempting re-registration")
                    try:
                        self.register_with_namenode()
                        consecutive_failures = 0  # Reseting on successful registration
                    except:
                        print("Re-registration also failed")
            
            time.sleep(10)

    def register_with_namenode(self):
        """Register with the NameNode using retry logic"""
        def attempt_register():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.namenode_host, self.namenode_port))
                register_msg = f"register {self.host}:{self.port}"
                s.sendall(register_msg.encode())
                response = s.recv(1024).decode()
                print(f"DataNode {self.port} - NameNode registration response: {response}")
                return response
        
        try:
            self.retry_with_backoff(
                attempt_register,
                max_retries=5,
                base_delay=2.0,
                max_delay=30.0
            )
            print(f"DataNode {self.port} successfully registered with NameNode")
        except Exception as e:
            print(f"Failed to register DataNode {self.port} with NameNode after all retries: {e}")
            print("DataNode will continue running and attempt registration during heartbeat")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataNode script")
    parser.add_argument("-p", type=int, required=True, help="Port for the DataNode to use")
    args = parser.parse_args()

    datanode_port = args.p
    datanode = DataNode("localhost", datanode_port, "localhost", 8000)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Shutting down DataNode {datanode.port}...")
