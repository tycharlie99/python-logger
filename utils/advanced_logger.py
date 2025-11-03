from __future__ import annotations
import logging
import logging.handlers
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener, SocketHandler
import multiprocessing
import os
import time
import random
import threading
import socket
import pickle  # Used for serializing and deserializing LogRecord
import struct  # Used for handling packet length
from typing import Any, Literal, cast
from socketserver import ThreadingTCPServer, StreamRequestHandler

# --- I. Configuration and Mode Definition ---

# Defines the type alias for logging modes
LogMode = Literal["SINGLE_THREAD", "MULTI_PROCESS", "DISTRIBUTED"]


class LogConfig:
    """Application logging configuration for centralized parameter management."""

    LOG_MODE: LogMode = "SINGLE_THREAD"  # Default to single-thread mode
    LOG_FILE: str = "app.log"
    LOG_LEVEL: int = logging.DEBUG
    MAX_BYTES: int = 10 * 1024 * 1024  # 10MB
    BACKUP_COUNT: int = 5
    FORMATTER: logging.Formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
    )
    # Configuration specific to distributed mode
    SERVER_HOST: str = "127.0.0.1"
    SERVER_PORT: int = logging.handlers.DEFAULT_TCP_LOGGING_PORT


# --- II. Distributed Log Server Core (Version 3) ---

_server_logger: logging.Logger | None = None
_server_lock = threading.Lock()


def get_server_logger() -> logging.Logger:
    """
    Gets or creates the server-side Singleton Logger.
    Called only in the log server process.
    """
    global _server_logger
    with _server_lock:
        if _server_logger is None:
            _server_logger = logging.getLogger("LogServer")
            _server_logger.setLevel(logging.DEBUG)  # Receives logs of all levels

            if not _server_logger.handlers:
                handlers = LoggerManager._setup_common_handlers(
                    _server_logger,
                    LogConfig.FORMATTER,
                    LogConfig.LOG_LEVEL,
                    LogConfig.LOG_FILE,
                    LogConfig.MAX_BYTES,
                    LogConfig.BACKUP_COUNT,
                )
                for h in handlers:
                    _server_logger.addHandler(h)

        return _server_logger


class LogRecordHandler(StreamRequestHandler):
    """Handles the received LogRecord data."""

    def handle(self) -> None:
        # Use LogConfig's static configuration because LogServer runs independently
        target_logger = get_server_logger()
        while True:
            try:
                # 1. Read length
                chunk = self.connection.recv(4)
                if len(chunk) < 4:
                    break
                slen = struct.unpack(">L", chunk)[0]

                # 2. Read data
                chunk = self.connection.recv(slen)
                while len(chunk) < slen:
                    chunk += self.connection.recv(slen - len(chunk))

                # 3. Deserialize and process
                obj = pickle.loads(chunk)
                record = logging.makeLogRecord(obj)
                target_logger.handle(record)

            except (EOFError, ConnectionResetError):
                break
            except Exception:
                # Avoid logging errors causing infinite recursion, just exit here
                break


class LogRecordReceiver(ThreadingTCPServer):
    """TCP Server that receives client logs."""

    allow_reuse_address = True

    def __init__(self, host: str, port: int) -> None:
        super().__init__((host, port), LogRecordHandler)
        self.abort = 0
        self.timeout = 1

    def serve_until_stopped(self) -> None:
        print(f"\n[SERVER PROCESS] Log Receiver started on {self.server_address}")
        while not self.abort:
            self.handle_request()

    def stop(self) -> None:
        self.abort = 1
        try:
            if self.server_address:
                with socket.create_connection(
                    cast(tuple[str, int], self.server_address), timeout=0.5
                ):
                    pass
        except Exception:
            pass
        self.server_close()


def run_log_server_process(host: str, port: int) -> None:
    """Runs the log server in a separate process."""
    try:
        # Since LogConfig is static, we assume it's set in the main program
        get_server_logger()
        server = LogRecordReceiver(host=host, port=port)
        server.serve_until_stopped()
    except Exception:
        pass
    finally:
        print("\n[SERVER PROCESS] Log Server process has shut down.")


# --- III. Logger Manager Singleton (Unified Interface) ---


class LoggerManager:
    """
    Unified Singleton interface that initializes different types of logging
    based on LogConfig.LOG_MODE.
    """

    _instance: LoggerManager | None = None
    _is_initialized: bool = False
    logger: logging.Logger

    # Static check fix: Declare instance attribute
    _config: LogConfig

    # Specific for multi-process/distributed modes
    _log_queue: multiprocessing.Queue[Any] | None = None
    _listener: QueueListener | None = None
    _server_process: multiprocessing.Process | None = None

    # B008 warning fix: Move LogConfig() call into function body
    def __new__(cls, config: LogConfig | None = None) -> LoggerManager:
        if config is None:
            config = LogConfig()

        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._config = config
        return cls._instance

    @staticmethod
    def _setup_common_handlers(
        logger: logging.Logger,
        formatter: logging.Formatter,
        level: int,
        log_file: str,
        max_bytes: int,
        backup_count: int,
    ) -> list[logging.Handler]:
        """Sets up common StreamHandler and RotatingFileHandler"""
        handlers = []

        # File handler
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)

        return handlers

    # The default value for the config parameter in __init__ is necessary,
    # but __new__ already handles the Singleton's config setup.
    # The config here is for type hinting and avoiding B008 (if __new__ wasn't present)
    def __init__(self, config: LogConfig | None = None) -> None:
        # Use self._config to ensure the config from the first Singleton creation is used
        config = self._config

        if LoggerManager._is_initialized:
            return

        # 1. Initialize base Logger (Use Root Logger so AppLogger in child processes can propagate)
        self.logger = logging.getLogger()
        self.logger.setLevel(config.LOG_LEVEL)

        # 2. Clear default handlers
        for h in self.logger.handlers[:]:
            self.logger.removeHandler(h)

        # 3. Configure handlers based on mode
        if config.LOG_MODE == "SINGLE_THREAD":
            self._init_single_thread(config)
        elif config.LOG_MODE == "MULTI_PROCESS":
            self._init_multi_process(config)
        elif config.LOG_MODE == "DISTRIBUTED":
            self._init_distributed(config)
        else:
            raise ValueError(f"Unknown log mode: {config.LOG_MODE}")

        LoggerManager._is_initialized = True
        self.logger.info(f"LoggerManager initialized successfully in {config.LOG_MODE} mode.")

    def _init_single_thread(self, config: LogConfig) -> None:
        """Version 1: Single-thread/single-process local logging"""
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(module)s - %(message)s")
        handlers = self._setup_common_handlers(
            self.logger,
            formatter,
            config.LOG_LEVEL,
            config.LOG_FILE,
            config.MAX_BYTES,
            config.BACKUP_COUNT,
        )
        for h in handlers:
            self.logger.addHandler(h)

    def _init_multi_process(self, config: LogConfig) -> None:
        """Version 2: Multi-process logging (using QueueHandler/QueueListener)"""
        if self._log_queue is None:
            self._log_queue = multiprocessing.Queue(-1)  # Shared queue

        # Only start QueueListener in the Main Process (Root Logger receives)
        # Only handlers processed by QueueListener need formatter
        if multiprocessing.current_process().name == "MainProcess":
            formatter = logging.Formatter(
                "%(asctime)s - PID:%(process)d - TID:%(thread)d - %(levelname)s - %(module)s - %(message)s"
            )
            handlers = self._setup_common_handlers(
                self.logger,
                formatter,
                config.LOG_LEVEL,
                config.LOG_FILE,
                config.MAX_BYTES,
                config.BACKUP_COUNT,
            )

            self._listener = QueueListener(self._log_queue, *handlers)
            self._listener.start()

    def _init_distributed(self, config: LogConfig) -> None:
        """Version 3: Distributed logging (using SocketHandler)"""

        # 1. Configure SocketHandler in the main program (Note: this is the Root Logger of the Main Process)
        socket_handler = SocketHandler(config.SERVER_HOST, config.SERVER_PORT)
        self.logger.addHandler(socket_handler)

        # 2. Only the Main Process starts the log server process
        if multiprocessing.current_process().name == "MainProcess":
            self._server_process = multiprocessing.Process(
                target=run_log_server_process,
                args=(config.SERVER_HOST, config.SERVER_PORT),
            )
            self._server_process.start()
            # Give server process time to start
            time.sleep(1.5)
            self.logger.info("Distributed Log Server Process started.")

    @staticmethod
    def setup_worker_logger(
        log_queue: multiprocessing.Queue[Any] | None = None,
        # ✅ Fix: Use passed parameters, avoid reliance on static LogConfig
        log_mode: LogMode = "SINGLE_THREAD",
        log_level: int = logging.DEBUG,
        server_host: str = LogConfig.SERVER_HOST,
        server_port: int = LogConfig.SERVER_PORT,
    ) -> None:
        """Sets up logging for worker processes (specific to multi-process modes)"""

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)  # Use passed level

        # Clear any other possible handlers
        for h in root_logger.handlers[:]:
            root_logger.removeHandler(h)
        print(f"[Worker PID:{os.getpid()}] Configuring logger for mode: {log_mode}")

        if log_mode == "MULTI_PROCESS" and log_queue:
            # Multi-process mode: Attach QueueHandler to Root Logger
            print(f"[Worker PID:{os.getpid()}] Setting up QueueHandler for multi-process logging.")
            root_logger.addHandler(QueueHandler(log_queue))
        elif log_mode == "DISTRIBUTED":
            # Distributed mode: Attach SocketHandler to Root Logger
            print(f"[Worker PID:{os.getpid()}] Setting up SocketHandler for distributed logging.")
            socket_handler = SocketHandler(server_host, server_port)
            root_logger.addHandler(socket_handler)

        # Ensure AppLogger in worker processes has no Handlers (AppLogger should propagate to Root Logger)
        app_logger = logging.getLogger("AppLogger")
        for h in app_logger.handlers[:]:
            app_logger.removeHandler(h)
        app_logger.propagate = True
        app_logger.setLevel(log_level)

    @staticmethod
    def shutdown() -> None:
        """Shuts down the Listener and Server process"""
        instance = LoggerManager._instance
        if instance is None:
            return

        if instance._listener:
            instance._listener.stop()
            instance._listener = None

        if instance._server_process:
            if instance._server_process.is_alive():
                print("\n[MAIN] Shutting down Log Server Process...")
                instance._server_process.terminate()
                instance._server_process.join()
            instance._server_process = None

        # Clear Singleton state
        LoggerManager._is_initialized = False
        LoggerManager._instance = None

        # Close and clear all logger handlers
        logging.shutdown()

    # --- Wrapper methods for convenience ---
    def debug(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.critical(msg, *args, **kwargs)


# --- IV. Application Test Logic ---


def worker_task(
    name: str | int,
    log_queue: multiprocessing.Queue[Any] | None = None,
    log_mode: LogMode = "SINGLE_THREAD",
    log_level: int = logging.DEBUG,
    server_host: str = LogConfig.SERVER_HOST,
    server_port: int = LogConfig.SERVER_PORT,
) -> None:
    """Simulates a worker thread/process task"""
    print(f"Worker {name} (PID: {os.getpid()}) starting task...")
    # print(f"Worker {name} configuring logger for mode: {log_mode}")

    # Set up the logging sending mechanism for the worker process (using the passed log_mode)
    if log_mode in ("MULTI_PROCESS", "DISTRIBUTED"):
        print(f"Worker {name} setting up logger for mode: {log_mode}")
        LoggerManager.setup_worker_logger(log_queue, log_mode, log_level, server_host, server_port)

    # Use AppLogger consistently (Root Logger is responsible for transmission)
    logger = logging.getLogger("AppLogger")
    logger.propagate = True  # Ensure propagation to Root Logger
    logger.setLevel(log_level)  # Ensure AppLogger level is set correctly

    logger.info(f"Worker {name} start. PID: {os.getpid()}")
    for i in range(3):
        logger.debug(f"Worker {name} step {i}")
        rand_num = random.uniform(0.1, 0.5)
        time.sleep(rand_num)

    if random.random() < 0.2:
        logger.error(f"Worker {name} failed: Simulated error.")
    else:
        logger.info(f"Worker {name} done")


def run_single_thread_test() -> None:
    # Static configuration must be set first for LoggerManager's internal use
    LogConfig.LOG_MODE = "SINGLE_THREAD"
    LogConfig.LOG_FILE = "single_thread_app.log"
    if os.path.exists(LogConfig.LOG_FILE):
        os.remove(LogConfig.LOG_FILE)

    logger = LoggerManager()
    logger.info("--- Starting Single-Thread Test ---")

    # Pass configuration to worker_task
    worker_task("MainThread", log_mode=LogConfig.LOG_MODE, log_level=LogConfig.LOG_LEVEL)
    logger.error("Single thread test complete.")
    LoggerManager.shutdown()


def run_multi_process_test() -> None:
    # Ensure cross-platform compatibility
    multiprocessing.set_start_method("spawn", force=True)
    LogConfig.LOG_MODE = "MULTI_PROCESS"
    LogConfig.LOG_FILE = "multi_process_app.log"
    if os.path.exists(LogConfig.LOG_FILE):
        os.remove(LogConfig.LOG_FILE)

    print("Starting multi-process test...")

    # First call initializes Listener and Queue
    LoggerManager()
    logger = LoggerManager()  # Get instance
    log_queue = logger._instance._log_queue if logger._instance else None

    logger.info("--- Starting Multi-Process Test ---")

    # Parameter packing, explicitly passed to Worker Process
    worker_args = (
        LogConfig.LOG_MODE,
        LogConfig.LOG_LEVEL,
        LogConfig.SERVER_HOST,
        LogConfig.SERVER_PORT,
    )

    procs = [
        multiprocessing.Process(
            target=worker_task,
            args=(i, log_queue, *worker_args),  # Pass i, log_queue, log_mode, log_level, host, port
        )
        for i in range(3)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    logger.info("All workers finished. Shutting down listener.")
    LoggerManager.shutdown()


# Please add this function to the file, e.g., after worker_task
def distributed_pool_worker_wrapper(name: int) -> None:
    """
    Wrapper function for multiprocessing.Pool.map to pass a single argument (name).
    It relies on the static LogConfig class for the remaining distributed logging configuration.
    """
    return worker_task(
        name=name,
        log_queue=None,  # Pool does not use Queue
        log_mode="DISTRIBUTED",
        log_level=LogConfig.LOG_LEVEL,
        server_host=LogConfig.SERVER_HOST,
        server_port=LogConfig.SERVER_PORT,
    )


def run_distributed_test() -> None:
    multiprocessing.set_start_method("spawn", force=True)
    LogConfig.LOG_MODE = "DISTRIBUTED"
    LogConfig.LOG_FILE = "distributed_app.log"
    if os.path.exists(LogConfig.LOG_FILE):
        os.remove(LogConfig.LOG_FILE)

    # Main controller program starts the log server
    logger = LoggerManager()
    main_logger = logger

    main_logger.info("--- Starting Distributed Test ---")

    NUM_WORKERS = 4
    workers_ids = range(1, NUM_WORKERS + 1)

    # Removed internal pool_worker_wrapper definition

    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        # ✅ Directly call the module-level function
        pool.map(distributed_pool_worker_wrapper, workers_ids)

    # Wait for logs to be transmitted
    time.sleep(1)

    main_logger.info("All distributed tasks completed. Shutting down server.")
    LoggerManager.shutdown()

    if os.path.exists(LogConfig.LOG_FILE):
        print(f"\n✅ Logs successfully aggregated and written to file: {LogConfig.LOG_FILE}")


if __name__ == "__main__":
    # Select which mode to run
    print("Select the logging mode to test:")
    print("1: Single-Thread/Multi-Threading")
    print("2: Multi-Processing (QueueHandler)")
    print("3: Distributed (SocketHandler)")
    choice = input("Enter number (1/2/3): ")

    if choice == "1":
        run_single_thread_test()
    elif choice == "2":
        run_multi_process_test()
    elif choice == "3":
        run_distributed_test()
    else:
        print("Invalid choice.")

    print("\n--- Test Finished ---")
