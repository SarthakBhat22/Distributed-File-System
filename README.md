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

### Reads
The following process demonstrates how reads are handled for files:
<img width="1078" height="630" alt="Screenshot 2025-09-10 at 11 33 46 PM" src="https://github.com/user-attachments/assets/b1e63188-dc9f-470e-ad92-0c5039ba193f" />