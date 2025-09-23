# Yet Another Distributed File System
Yet Another Distributed File System (or YADFS) is a distributed, scalable, fault-tolerant block type file storage system that can be used to handle multiple, large files. It can be scaled up quite easily and can handle millions of megabytes across thousands of nodes.

<img width="701" height="245" alt="image" src="https://github.com/user-attachments/assets/58a4f145-221b-4446-ab26-8adf9cf6bf65" /><br>

This file system features a Namenode, Datanodes and the Client code. All three of these can be used on different machines as long as the IP and port of each component is known. <br>

Here are the functions of the main components:<br>
1. **Namenode**: Stores information about the Datanodes (alive and dead nodes) and file metadata. Coordinates with the entire system and is a centralised control center.
2. **Datanode**: Stores the file blocks and is responsible for atomic writes and replication.
3. **Client**: Coordinates with the Namenode and the Datanodes when writing and reading files. Consists of the CLI as well.

<br>This system is heavily inspired by the Hadooop distributed file system, which consists of a similar architecture. You can read more about [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html).

Note: Make sure you have the latest versions of Python and Redis installed on your local machine.<br>

### Writes
The process followed during writes is shown in this diagram:
<img width="1117" height="650" alt="Screenshot 2025-09-10 at 11 33 36 PM" src="https://github.com/user-attachments/assets/1a539423-c3a0-4716-8893-b788d85cb094" />
<br>
1. The Client knows which active datanodes are available since sthe Namenode keeps track with regular heartbeats.<br><br>
2. It picks random Datanodes to write each block. Once it has successfully written the original blocks and the Datanodes send an acknowledgement for a successful write, the Datanodes asynchronously replicate each original block to match the replication factor.<br><br>
3. If Datanodes fail, each Datanode communicates with the other Datanodes to ask if they have replicas for their blocks. This is done using retries with backoff.<br><br>
4. File metadata is updated and sent to the Namenode, where it is stored in Redis. The metadata contains information like filename, location within the dfs, block locations, replica locations, timestamp.<br><br>
5. Namenode is always aware of the condition of each Datanode that had connected and registered successfully with it. After 30 seconds of inactivity, the node is considered to be dead unless it comes back up and registers again.
### Reads
The following process demonstrates how reads are handled for files:
<img width="1078" height="630" alt="Screenshot 2025-09-10 at 11 33 46 PM" src="https://github.com/user-attachments/assets/b1e63188-dc9f-470e-ad92-0c5039ba193f" />
<br>
1. The Client queries the Namenode with the file it wants to read within the file system. Reading a file not present in the dfs returns an error.<br><br>
2. The Namenode returns the metadata along with the available Datanodes that contain the blocks for the file.<br><br>
3. Client recieves this list and does a round-robin read of the Datanodes to read each block to avoid unbalanced load on some nodes. If reads from a node fail, another node with the replica is queried.<br><br>
4. Once the blocks are recieved, they can be reconstructed on the client side and written into the local system of the user.<br><br>