# Python Logger System (Distributed, Multi-Process, Single-Thread)

This project provides a flexible and feature-rich Python logging solution that supports three operational modes: single-threaded, multi-process (queue-based), and distributed (Socket server-based). It is specifically designed to handle the log aggregation needs of complex and large-scale applications.

---

## ✨ Key Features

* **Three Operational Modes:** Easily switch between Single-Thread, Multi-Process, and Distributed modes using a single configuration class (`LogConfig`).
* **Log Isolation (Multi-Process):** Utilizes `QueueHandler` and `QueueListener` to achieve non-blocking logging, preventing I/O contention in multi-process applications.
* **Log Aggregation (Distributed):** Uses `SocketHandler` and an independent TCP log server (`LogRecordReceiver`) to centralize logs from multiple workers/hosts into a single file.
* **Singleton Design:** The `LoggerManager` class employs the Singleton pattern, ensuring the log manager is initialized only once throughout the application's lifecycle.
* **Log Rotation:** Employs `RotatingFileHandler` for log file management, supporting limits on file size and backup count.

---

## 🛠️ Operational Modes Explained

### 1. SINGLE_THREAD Mode

The simplest mode where log records are written synchronously to the file and console in the calling thread.

* **Mechanism:** The `Root Logger` directly mounts a `RotatingFileHandler` and a `StreamHandler`.
* **Applicability:** Simple scripts, single-threaded applications, and development/debugging.

### 2. MULTI_PROCESS Mode

Utilizes Python's `multiprocessing.Queue` to handle logs from multiple child processes. Worker processes put log records into the queue, and a single `QueueListener` in the main process is responsible for safely retrieving and writing them to disk.

* **Mechanism:** Worker `Root Logger` mounts a `QueueHandler`; the main process starts a `QueueListener` and mounts the `RotatingFileHandler` (etc.) onto the Listener.
* **Advantage:** Achieves non-blocking logging, improving performance in multi-process applications.

### 3. DISTRIBUTED Mode

Uses TCP Sockets to transmit log records to an independently running log server process.

* **Mechanism:**
    * **Client (Worker):** The `Root Logger` mounts a `SocketHandler`, serializing and sending logs over the network.
    * **Server (Log Server Process):** An independent process runs the `LogRecordReceiver` (a TCP server) which receives the logs and passes them to a dedicated `_server_logger` for writing.
* **Advantage:** Centralized log management, suitable for multi-machine, multi-service distributed deployments.

---

