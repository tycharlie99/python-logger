import logging
import logging.handlers
import socketserver
import time
import random
import threading
import socket
import pickle  # Used for serializing and deserializing LogRecord
import struct  # Used for handling packet length
import os
import multiprocessing
from multiprocessing import Process, Pool, current_process

# ===================================================================
# Configuration Parameters (Merged from both)
# ===================================================================
SERVER_HOST = "127.0.0.1"
SERVER_PORT = logging.handlers.DEFAULT_TCP_LOGGING_PORT  # Default port 9020
LOG_FILE_NAME = "centralized_socket_log.log"
NUM_WORKERS = 4  # Number of worker processes (Pool size)
NUM_TASKS = 10   # Number of total tasks to be processed

# ===================================================================
# 1. Singleton Logger Server Core
# ===================================================================

_singleton_logger = None
_singleton_lock = threading.Lock()


def get_singleton_logger(name="CentralSocketLogger", log_file=LOG_FILE_NAME):
    """
    Get or create Singleton Logger instance (called only in the Server Process).
    Configures FileHandler and StreamHandler for central logging.
    """
    global _singleton_logger
    with _singleton_lock:
        if _singleton_logger is None:
            # 1. Create Logger instance
            _singleton_logger = logging.getLogger(name)
            # Use DEBUG level to capture all incoming logs
            _singleton_logger.setLevel(logging.DEBUG)

            # Formatter enhanced to include process ID (PID)
            formatter = logging.Formatter(
                "[SERVER] [%(asctime)s] %(levelname)s - %(name)s (PID:%(process)d): %(message)s"
            )

            # 2. FileHandler: Write all aggregated logs to a file
            # Use mode="w" to overwrite the log file on each run
            file_handler = logging.FileHandler(
                log_file, mode="w", encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            _singleton_logger.addHandler(file_handler)

            # 3. StreamHandler: Optional, print to console in real time
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            _singleton_logger.addHandler(ch)

            print(
                f"\n[SERVER INIT] Singleton Logger '{name}' initialized successfully."
                f" Logs will be written to {log_file}"
            )

        return _singleton_logger


# ===================================================================
# 2. Socket Server Handler & Receiver
# ===================================================================


class LogRecordHandler(socketserver.StreamRequestHandler):
    """Handles a single TCP connection, receiving serialized LogRecord objects."""

    def handle(self):
        # Retrieve the central logger instance within the server's thread
        target_logger = get_singleton_logger()

        while True:
            try:
                # 1. Read 4 bytes for the data length
                chunk = self.connection.recv(4)
                if len(chunk) < 4:
                    # Connection closed or error
                    break
                # Unpack the length (network byte order, big-endian)
                slen = struct.unpack(">L", chunk)[0]

                # 2. Read the complete LogRecord data
                chunk = self.connection.recv(slen)
                while len(chunk) < slen:
                    # Keep reading until the whole packet is received
                    chunk += self.connection.recv(slen - len(chunk))

                # 3. Deserialize LogRecord using pickle
                obj = pickle.loads(chunk)
                record = logging.makeLogRecord(obj)

                # 4. Route to Singleton Logger (triggers FileHandler/StreamHandler)
                target_logger.handle(record)

            except (EOFError, ConnectionResetError):
                # Client disconnected gracefully or was reset
                break
            except Exception as e:
                # Handle other unexpected errors
                # print(f"Error during log handling: {e}") # Uncomment for debugging
                break


class LogRecordReceiver(socketserver.ThreadingTCPServer):
    """TCP Server that starts and manages the log receiving service."""

    # Allow the socket to be reused immediately after closure
    allow_reuse_address = True

    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        super().__init__((host, port), LogRecordHandler)
        self.abort = 0
        self.timeout = 1

    def serve_until_stopped(self):
        """Run in a loop until externally terminated."""
        print(f"[SERVER PROCESS] Log Receiver started on {self.server_address}")
        while not self.abort:
            # Handle one request, blocking until connection
            self.handle_request()

    def stop(self):
        """Gracefully shut down the server by sending a dummy connection."""
        self.abort = 1
        # Send dummy connection to interrupt blocking handle_request()
        try:
            with socket.create_connection(self.server_address, timeout=0.5):
                pass
        except:
            pass
        self.server_close()


def run_log_server():
    """Target function to run Logger Server in a separate process."""
    try:
        # Initialize the Singleton Logger immediately within the server process
        get_singleton_logger()
        server = LogRecordReceiver(host=SERVER_HOST, port=SERVER_PORT)
        server.serve_until_stopped()
    except Exception:
        # Catch exceptions if the process is terminated (e.g., by terminate())
        pass
    finally:
        print("\n[SERVER PROCESS] Log Server process has shut down.")


# ===================================================================
# 3. Distributed Worker (Client) Logic
# ===================================================================


def setup_worker_logger(worker_id: int):
    """
    Configure Logger for each worker, using SocketHandler to send logs
    to the Central Log Server.
    """
    logger_name = f"Worker-{worker_id}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Log all details locally

    # Clear any existing default handlers to avoid duplicate logs
    for h in logger.handlers[:]:
        logger.removeHandler(h)

    # Use SocketHandler to send logs to the central server
    socket_handler = logging.handlers.SocketHandler(SERVER_HOST, SERVER_PORT)
    logger.addHandler(socket_handler)

    return logger


def worker_task(worker_id: int):
    """Simulates a worker task execution and records various log levels."""
    logger = setup_worker_logger(worker_id)
    process_name = current_process().name
    pid = os.getpid()

    logger.info(f"Task {worker_id} starting. Process: {process_name}, PID: {pid}")

    delay = random.uniform(1, 3)
    logger.debug(f"Simulating computation for {delay:.2f} seconds...")
    time.sleep(delay)

    result = worker_id * 10 + delay

    if random.random() < 0.2:
        logger.error(f"Task {worker_id}: Simulated critical failure!")
        # Return a failure signal
        return f"Task {worker_id} FAILED"

    logger.warning(
        f"Task {worker_id} completed successfully. Result: {result:.2f}. Duration: {delay:.2f}s"
    )

    logger.info(f"Task {worker_id} exiting.")

    # Return the result for Pool to collect
    return f"Task {worker_id} SUCCESS with result {result:.2f}"


# ===================================================================
# 4. Main Dispatcher Program (DP Simulation Main)
# ===================================================================

if __name__ == "__main__":
    # Force spawn start method for better compatibility across OS and mixing processes/threads
    multiprocessing.set_start_method("spawn", force=True)

    # Ensure the old log file is cleared before starting
    if os.path.exists(LOG_FILE_NAME):
        os.remove(LOG_FILE_NAME)

    # --- Start Server Process ---
    server_process = Process(target=run_log_server)
    server_process.start()

    # Wait for the server socket to fully open
    time.sleep(1.5)

    print("\n[DP MAIN] Log Server started, beginning task assignment...")

    # Configure a logger for the Main Process itself (optional but useful)
    main_logger = setup_worker_logger(0)
    main_logger.info("Main program starting Worker Pool tasks...")

    # --- Start Client/Worker Process Pool ---
    with Pool(processes=NUM_WORKERS) as pool:
        workers_ids = range(1, NUM_TASKS + 1)
        # Use pool.map to assign tasks and wait for results
        results = pool.map(worker_task, workers_ids)

        main_logger.info("All worker tasks have completed.")

    # Wait a short time to ensure all final log packets are delivered to the server
    time.sleep(1)

    # --- Shutdown and Report ---
    print("\n[DP MAIN] Shutting down Log Server...")
    # Attempt a soft stop (LogRecordReceiver.stop) before force terminate
    # Note: We can't directly call 'server.stop()' as 'server' is in another process,
    # so we rely on the logic in run_log_server and terminate().
    server_process.terminate()
    server_process.join()

    # --- Final check and print file content ---
    if os.path.exists(LOG_FILE_NAME):
        print(f"\n✅ Logs successfully aggregated and written to file: {LOG_FILE_NAME}")
        
        # Print first few lines for confirmation
        print("\n--- File Content Preview (First 5 lines) ---")
        try:
            with open(LOG_FILE_NAME, "r", encoding="utf-8") as f:
                for i in range(5):
                    line = f.readline().strip()
                    if line:
                        print(line)
        except Exception as e:
            print(f"Could not read log file: {e}")
        print("------------------------------------------")

    print("\n--- Worker Task Results ---")
    print(results)
