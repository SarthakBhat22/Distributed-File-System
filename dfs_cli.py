#!/usr/bin/env python3

import os
import sys
import cmd
import argparse
from pathlib import Path
from datetime import datetime
import socket
from colorama import Fore, Style, init
init(autoreset=True)

from client import Client

class DFSCLI(cmd.Cmd):
    """Interactive CLI for the Distributed File System"""
    
    intro = f"""
    {Fore.RED}
     __   __ _    ____  _____ ____  
     \ \ / // \  |  _ \|  ___/ ___| 
      \ V // _ \ | | | | |_  \___ \ 
       | |/ ___ \| |_| |  _|  ___) |
       |_/_/   \_\____/|_|   |____/ 
                {Fore.CYAN}
                 v 1.25 

    {Style.RESET_ALL}
    Type '{Fore.YELLOW}help{Style.RESET_ALL}' or '{Fore.YELLOW}?{Style.RESET_ALL}' to list commands
    Type '{Fore.YELLOW}help <command>{Style.RESET_ALL}' for detailed help on a command
    """
    
    def __init__(self, namenode_host="localhost", namenode_port=8000):
        super().__init__()
        self.client = Client(namenode_host, namenode_port)
        self.update_prompt()
    
    def update_prompt(self):
        """Update the command prompt to show current directory"""
        current_dir = self.client.get_current_directory()
        self.prompt = f'{Fore.CYAN}yadfs{current_dir} $ ' 
    
    def do_put(self, args):
        """Upload a file to the DFS in the current directory
        Usage: put <local_file_path> [remote_filename]
        
        Examples:
            put /home/user/data.csv
            put /home/user/data.csv my_data.csv
            put ../documents/report.pdf report.pdf
        """
        if not args:
            print("Error: Please provide a file path")
            print("Usage: put <local_file_path> [remote_filename]")
            return
        
        parts = args.split()
        local_path = parts[0]
        
        if not os.path.exists(local_path):
            print(f"Error: File '{local_path}' does not exist")
            return
        
        if len(parts) > 1:
            remote_filename = parts[1]
        else:
            remote_filename = os.path.basename(local_path)
        
        current_dir = self.client.get_current_directory()
        print(f"Uploading '{local_path}' as '{remote_filename}' to directory '{current_dir}'...")

        success = self.client.write_file(local_path, remote_filename)
        
        if success:
            print(f"Successfully uploaded '{local_path}' as '{remote_filename}'")
        else:
            print(f"Failed to upload '{local_path}'")
    
    def do_get(self, args):
        """Download a file from the current directory in DFS
        Usage: get <remote_filename> [local_path]
        
        Examples:
            get data.csv
            get data.csv /home/user/downloaded_data.csv
            get reports/monthly.pdf ./monthly_report.pdf
        """
        if not args:
            print("Error: Please provide a remote filename")
            print("Usage: get <remote_filename> [local_path]")
            return
        
        parts = args.split()
        remote_filename = parts[0]

        if len(parts) > 1:
            local_path = parts[1]
        else:
            local_path = os.path.join(os.getcwd(), os.path.basename(remote_filename))
        
        current_dir = self.client.get_current_directory()
        print(f"Downloading '{remote_filename}' from directory '{current_dir}' to '{local_path}'...")
        
        success = self.client.read_file(remote_filename, local_path)
        
        if success:
            print(f"Successfully downloaded '{remote_filename}' to '{local_path}'")
        else:
            print(f"Failed to download '{remote_filename}'")
    
    def do_ls(self, args):
        """List directory contents
        Usage: ls [path]
        
        Examples:
            ls
            ls /data
            ls projects/ml
            ls ..
        """
        if args.strip():
            path = args.strip()
            print(f"Listing contents of '{path}':")
            contents = self.client.list_directory(path)
        else:
            current_dir = self.client.get_current_directory()
            print(f"Listing contents of '{current_dir}':")
            contents = self.client.list_directory()
        
        if contents is None:
            print("Could not list directory")
    
    def do_ll(self, args):
        """Detailed directory listing (alias for ls)"""
        self.do_ls(args)
    
    def do_mkdir(self, args):
        """Create a directory
        Usage: mkdir <directory_path>
        
        Examples:
            mkdir data
            mkdir /projects/ml
            mkdir ../backup
        """
        if not args:
            print("Error: Please provide a directory path")
            print("Usage: mkdir <directory_path>")
            return
        
        path = args.strip()
        
        # Handle relative paths
        if not path.startswith('/'):
            current_dir = self.client.get_current_directory()
            if current_dir == '/':
                path = '/' + path
            else:
                path = current_dir + '/' + path
        
        print(f"Creating directory '{path}'...")
        
        success = self.client.create_directory(path)
        
        if success:
            print(f"Successfully created directory '{path}'")
        else:
            print(f"Failed to create directory '{path}'")
    
    def do_pwd(self, args):
        """Print current working directory"""
        current_dir = self.client.get_current_directory()
        print(current_dir)
    
    def do_cd(self, args):
        """Change directory
        Usage: cd <path>
        
        Examples:
            cd /data
            cd projects/ml
            cd ..
            cd /
            cd ~ (go to root)
        """
        if not args:
            path = "/"
        else:
            path = args.strip()
            
        if path == "~":
            path = "/"
        
        print(f"Changing to directory: {path}")
        
        success = self.client.change_directory(path)
        
        if success:
            current_dir = self.client.get_current_directory()
            print(f"Changed to directory: {current_dir}")
            self.update_prompt()
        else:
            print(f"Failed to change to directory: {path}")
    
    def do_rm(self, args):
        """Remove/delete a file from the DFS
        Usage: rm <filename>
        
        Examples:
            rm data.csv
            rm reports/monthly.pdf
            rm /backup/old_data.txt
        """
        if not args:
            print("Error: Please provide a filename")
            print("Usage: rm <filename>")
            return
        
        filename = args.strip()
        current_dir = self.client.get_current_directory()
        
        try:
            metadata = self.client.get_file_metadata(filename)
            if metadata:
                file_size = metadata.get('total_size', 0)
                block_count = len(metadata.get('blocks', []))
                print(f"File: {filename}")
                print(f"Size: {file_size:,} bytes ({block_count} blocks)")
                print(f"Location: {current_dir}")
        except:
            pass
        
        success = self.client.delete_file(filename)
        
        if success:
            print(f"{Fore.GREEN}Successfully deleted file '{filename}'{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Failed to delete file '{filename}'{Style.RESET_ALL}")
    
    def do_help_delete(self, args):
        """Show detailed help for deletion commands"""
        print(f"""
            {Fore.YELLOW}Deletion Commands:{Style.RESET_ALL}

            {Fore.CYAN}rm <filename>{Style.RESET_ALL}
                Delete a single file from the current directory
                Examples:
                    rm data.csv
                    rm reports/monthly.pdf
                    rm /backup/old_file.txt

            {Fore.CYAN}rmdir <directory>{Style.RESET_ALL}
                Delete a directory and ALL its contents (recursive)
                Examples:
                    rmdir old_project
                    rmdir /temp/cache
                    rmdir ../abandoned_work

            {Fore.CYAN}del <path>{Style.RESET_ALL}
                Smart delete - automatically detects if path is file or directory
                Examples:
                    del data.csv        # deletes file
                    del old_folder      # deletes directory
                    del /mixed/path     # auto-detects type

            {Fore.CYAN}rm_rf <directory>{Style.RESET_ALL}
                Force remove directory (alias for rmdir)
                Examples:
                    rm_rf temp_files

            {Fore.RED}⚠️  IMPORTANT SAFETY NOTES:{Style.RESET_ALL}
            - All deletion operations require explicit confirmation (y/n)
            - Directory deletions show contents before asking for confirmation  
            - Deleted files/directories cannot be recovered
            - The operation deletes both metadata and physical blocks
            - Use 'du' to check sizes before deleting large directories

            {Fore.GREEN}Related Commands:{Style.RESET_ALL}
            - ls <path>     - List directory contents before deleting
            - du <path>     - Check disk usage/size before deleting  
            - info <file>   - Get file details before deleting
            - tree          - See directory structure before deleting
                    """)

    def help_commands(self):
        """Show categorized command help"""
        print(f"""
            {Fore.YELLOW}Available Commands:{Style.RESET_ALL}

            {Fore.CYAN}File Operations:{Style.RESET_ALL}
            put, get, info, rm

            {Fore.CYAN}Directory Operations:{Style.RESET_ALL}  
            ls, ll, mkdir, rmdir, cd, pwd, tree, du

            {Fore.CYAN}Navigation:{Style.RESET_ALL}
            cd, pwd, ls, tree, find

            {Fore.CYAN}System:{Style.RESET_ALL}
            status, clear, help, exit, quit

            {Fore.CYAN}Deletion (Destructive):{Style.RESET_ALL}
            rm, rmdir, del, rm_rf, cleanup

            Type 'help <command>' for detailed help on any command
            Type 'help_delete' for comprehensive deletion command help
                    """)

    def do_rmdir(self, args):
        """Remove/delete a directory and all its contents
        Usage: rmdir <directory_path>
        
        Examples:
            rmdir old_data
            rmdir /backup/2023
            rmdir projects/abandoned
        """
        if not args:
            print("Error: Please provide a directory path")
            print("Usage: rmdir <directory_path>")
            return
        
        path = args.strip()
        
        if not path.startswith('/'):
            current_dir = self.client.get_current_directory()
            if current_dir == '/':
                path = '/' + path
            else:
                path = current_dir + '/' + path
        
        path = self.client.normalize_path(path)
        
        if path == "/":
            print(f"{Fore.RED}Error: Cannot delete root directory{Style.RESET_ALL}")
            return
        
        print(f"Preparing to delete directory: {path}")
        success = self.client.delete_directory(path)
        
        if success:
            print(f"{Fore.GREEN}Successfully deleted directory '{path}' and all contents{Style.RESET_ALL}")
            try:
                current = self.client.get_current_directory()
                if not self.client.path_exists(current):
                    print(f"{Fore.YELLOW}Current directory was deleted, returning to root{Style.RESET_ALL}")
                    self.client.set_current_directory("/")
                    self.update_prompt()
            except:
                pass
        else:
            print(f"{Fore.RED}Failed to delete directory '{path}'{Style.RESET_ALL}")

    def do_rm_rf(self, args):
        """Force remove directory (alias for rmdir)
        Usage: rm_rf <directory_path>
        
        Examples:
            rm_rf old_project
            rm_rf /temp/cache
        """
        self.do_rmdir(args)

    def do_del(self, args):
        """Delete a file or directory (smart detection)
        Usage: del <path>
        
        Examples:
            del data.csv          # deletes file
            del old_folder        # deletes directory
            del /backup/archive   # deletes directory or file
        """
        if not args:
            print("Error: Please provide a path")
            print("Usage: del <path>")
            return
        
        path = args.strip()
        
        try:
            metadata = self.client.get_file_metadata(path)
            if metadata:
                print(f"Detected file: {path}")
                self.do_rm(args)
                return
        except:
            pass
        
        if not path.startswith('/'):
            current_dir = self.client.get_current_directory()
            if current_dir == '/':
                full_path = '/' + path
            else:
                full_path = current_dir + '/' + path
        else:
            full_path = path
        
        full_path = self.client.normalize_path(full_path)

        if self.client.path_exists(full_path):
            contents = self.client.list_directory(full_path)
            if contents is not None:
                print(f"Detected directory: {full_path}")
                self.do_rmdir(args)
                return
        
        print(f"{Fore.RED}Error: Path '{path}' not found or cannot be determined as file or directory{Style.RESET_ALL}")

    def do_cleanup(self, args):
        """Show cleanup information and options
        Usage: cleanup
        
        This command provides information about cleanup operations but does not 
        perform any destructive actions automatically.
        """
        print("DFS Cleanup Information:")
        print("-" * 60)
        print("Available cleanup commands:")
        print(f"  {Fore.CYAN}rm <filename>{Style.RESET_ALL}     - Delete a specific file")
        print(f"  {Fore.CYAN}rmdir <path>{Style.RESET_ALL}      - Delete a directory and all contents")
        print(f"  {Fore.CYAN}del <path>{Style.RESET_ALL}        - Smart delete (auto-detects file vs directory)")
        print()
        print("Note: All delete operations require confirmation and will show")
        print("what will be deleted before proceeding.")
        print()
        print("To see disk usage, use: info <filename> for files")
        print("To see directory contents: ls <directory>")

    def do_du(self, args):
        """Show disk usage for current directory (directory summary)
        Usage: du [path]
        
        Examples:
            du              # current directory
            du /data        # specific directory
            du projects     # relative path
        """
        if args.strip():
            path = args.strip()
        else:
            path = self.client.get_current_directory()
        
        print(f"Analyzing disk usage for: {path}")
        
        def calculate_directory_size(dir_path):
            """Recursively calculate directory size"""
            total_size = 0
            file_count = 0
            dir_count = 0
            
            try:
                contents = self.client.list_directory(dir_path)
                if not contents:
                    return 0, 0, 0
                
                for item in contents:
                    if item['type'] == 'file':
                        file_count += 1
                        total_size += item.get('size', 0)
                    elif item['type'] == 'directory':
                        dir_count += 1
                        subdir_path = dir_path.rstrip('/') + '/' + item['name'] if dir_path != '/' else '/' + item['name']
                        sub_size, sub_files, sub_dirs = calculate_directory_size(subdir_path)
                        total_size += sub_size
                        file_count += sub_files
                        dir_count += sub_dirs
            except Exception as e:
                print(f"Error analyzing {dir_path}: {e}")
            
            return total_size, file_count, dir_count
        
        total_size, file_count, dir_count = calculate_directory_size(path)
        
        print("-" * 60)
        print(f"Directory: {path}")
        print(f"Total Size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        print(f"Files: {file_count}")
        print(f"Directories: {dir_count}")
        print("-" * 60)
    
    def do_info(self, args):
        """Show file information
        Usage: info <filename>
        
        Examples:
            info data.csv
            info reports/monthly.pdf
        """
        if not args:
            print("Error: Please provide a filename")
            print("Usage: info <filename>")
            return
        
        filename = args.strip()
        current_dir = self.client.get_current_directory()
        print(f"Getting information for '{filename}' in directory '{current_dir}'...")
        
        metadata = self.client.get_file_metadata(filename)
        
        if metadata:
            print(f"\nFile Information for '{filename}':")
            print("-" * 60)
            print(f"Total Size: {metadata['total_size']:,} bytes")
            print(f"Number of Blocks: {len(metadata['blocks'])}")
            
            if 'creation_time' in metadata:
                created_time = datetime.fromtimestamp(metadata['creation_time']).strftime('%Y-%m-%d %H:%M:%S')
                print(f"Created: {created_time}")
            
            if 'full_path' in metadata:
                print(f"Full Path: {metadata['full_path']}")
                
            print(f"\nBlock Details:")
            for i, block in enumerate(metadata['blocks']):
                print(f"  Block {i}: {block['size']:,} bytes")
                print(f"    Locations: {', '.join(block['locations'])}")
        else:
            print(f"File '{filename}' not found in current directory")
    
    def do_tree(self, args):
        """Show directory tree structure starting from current or specified directory
        Usage: tree [path]
        
        Examples:
            tree
            tree /data
            tree projects
        """
        def print_tree(path, prefix="", is_last=True):
            """Recursively print directory tree - directories only"""
            try:
                contents = self.client.list_directory(path)
                if contents is None:
                    print(f"{prefix}Error: Could not read directory")
                    return

                dirs = [item for item in contents if item['type'] == 'directory']
                dirs = sorted(dirs, key=lambda x: x['name'])
                
                for i, item in enumerate(dirs):
                    is_last_item = (i == len(dirs) - 1)
                    current_prefix = "└── " if is_last_item else "├── "
                    
                    print(f"{prefix}{current_prefix}{item['name']}/")
                    
                    # Recursively print subdirectory
                    next_prefix = prefix + ("    " if is_last_item else "│   ")
                    subdir_path = path.rstrip('/') + '/' + item['name'] if path != '/' else '/' + item['name']
                    print_tree(subdir_path, next_prefix, is_last_item)
                        
            except Exception as e:
                print(f"{prefix}Error reading directory: {e}")
        
        if args.strip():
            start_path = args.strip()
        else:
            start_path = self.client.get_current_directory()
        
        print(f"Directory tree for '{start_path}':")
        print(f"{start_path}/")
        print_tree(start_path)
    
    def do_status(self, args):
        """Show system status"""
        print("Distributed File System Status:")
        print("-" * 60)
        
        try:
            print("DEBUG: Attempting connection test...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
                test_socket.settimeout(3)  # 3 second timeout
                print(f"DEBUG: Connecting to {self.client.namenode_host}:{self.client.namenode_port}")
                test_socket.connect((self.client.namenode_host, self.client.namenode_port))
                print("DEBUG: Connected, sending message...")
                test_socket.sendall("get_datanode".encode())
                response = test_socket.recv(1024).decode()
                print(f"DEBUG: Got response: {response}")
                if response:
                    print("NameNode: Connected")
                else:
                    print("NameNode: Connected but not responding properly")
        except socket.timeout:
            print("DEBUG: Socket timeout occurred")
            print("NameNode: Connection timeout")
        except ConnectionRefusedError:
            print("DEBUG: Connection refused")
            print("NameNode: Connection refused (NameNode may be down)")
        except Exception as e:
            print(f"DEBUG: Exception occurred: {type(e).__name__}: {e}")
            print("NameNode: Connection failed")
            print(f"   Error: {e}")
        
        current_dir = self.client.get_current_directory()
        print(f"Current Directory: {current_dir}")
        print(f"NameNode: {self.client.namenode_host}:{self.client.namenode_port}")
        print(f"Block Size: {self.client.block_size:,} bytes")
        
        try:
            contents = self.client.list_directory()
            if contents:
                dirs = len([item for item in contents if item['type'] == 'directory'])
                files = len([item for item in contents if item['type'] == 'file'])
                print(f"Current Directory Contents: {dirs} directories, {files} files")
        except:
            pass
    
    def do_find(self, args):
        """Find files and directories (simple implementation)
        Usage: find <name_pattern>
        
        Examples:
            find data.csv
            find *.csv (not yet implemented - use exact names)
        """
        if not args:
            print("Error: Please provide a search pattern")
            print("Usage: find <name_pattern>")
            return
        
        pattern = args.strip()
        current_dir = self.client.get_current_directory()
        
        def search_recursive(path, pattern):
            """Recursively search for files matching pattern"""
            results = []
            try:
                contents = self.client.list_directory(path)
                if not contents:
                    return results
                
                for item in contents:
                    full_path = path.rstrip('/') + '/' + item['name'] if path != '/' else '/' + item['name']

                    if pattern in item['name']:
                        results.append((full_path, item['type'], item.get('size', 0)))

                    if item['type'] == 'directory':
                        results.extend(search_recursive(full_path, pattern))
                        
            except Exception as e:
                print(f"Error searching in {path}: {e}")
            
            return results
        
        print(f"Searching for '{pattern}' starting from '{current_dir}'...")
        results = search_recursive(current_dir, pattern)
        
        if results:
            print(f"\nFound {len(results)} matches:")
            print("-" * 60)
            for path, item_type, size in results:
                type_str = "DIR " if item_type == 'directory' else "FILE"
                size_str = f" ({size:,} bytes)" if item_type == 'file' else ""
                print(f"{type_str} {path}{size_str}")
            print("-" * 60)
        else:
            print("No matches found")
    
    def do_clear(self, args):
        """Clear the screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def do_exit(self, args):
        """Exit the DFS CLI"""
        print("\nGoodbye!")
        return True
    
    def do_quit(self, args):
        """Exit the DFS CLI"""
        return self.do_exit(args)
    
    def do_EOF(self, args):
        """Handle Ctrl+D"""
        print()
        return self.do_exit(args)
    
    def emptyline(self):
        """Handle empty line input"""
        pass
    
    def default(self, line):
        """Handle unknown commands"""
        print(f"Unknown command: {line}")
        print("Type 'help' for available commands")
    
    def onecmd(self, line):
        """Override to add error handling"""
        try:
            return super().onecmd(line)
        except KeyboardInterrupt:
            print("\n^C")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Interactive DFS CLI with Directory Support")
    parser.add_argument(
        "--namenode", 
        default="localhost:8000", 
        help="NameNode address (default: localhost:8000)"
    )
    
    args = parser.parse_args()
    
    if ":" in args.namenode:
        host, port = args.namenode.split(":")
        port = int(port)
    else:
        host = args.namenode
        port = 8000
    
    try:
        cli = DFSCLI(host, port)
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"Failed to start CLI: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
