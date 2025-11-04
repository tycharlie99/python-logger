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


LogMode = Literal["SINGLE_THREAD", "MULTI_PROCESS", "DISTRIBUTED"]


class LogConfig:
    """Application logging configuration for centralized parameter management."""

    LOG_MODE: LogMode = "SINGLE_THREAD"  # Default to single-thread mode
    LOG_FILE: str = "app.log"
    LOG_LEVEL: int = logging.DEBUG
    MAX_BYTES: int = 10 * 1024 * 1024  # 10MB
    BACKUP_COUNT: int = 5
    FORMATTER: logging.Formatter = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(module)s - %(message)s"
    )
    # Configuration specific to distributed mode
    SERVER_HOST: str = "127.0.0.1"
    SERVER_PORT: int = logging.handlers.DEFAULT_TCP_LOGGING_PORT


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
            _server_logger = logging.getLogger()
            _server_logger.setLevel(logging.DEBUG)  # Receives logs of all levels

            for h in _server_logger.handlers[:]:
                _server_logger.removeHandler(h)

            handlers = LoggerManager._setup_common_handlers(
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
        get_server_logger()
        server = LogRecordReceiver(host=host, port=port)
        server.serve_until_stopped()
    except Exception:
        pass
    finally:
        print("\n[SERVER PROCESS] Log Server process has shut down.")


class LoggerManager:
    """
    Unified Singleton interface that initializes different types of logging
    based on LogConfig.LOG_MODE.
    """

    _instance: LoggerManager | None = None
    _is_initialized: bool = False
    logger: logging.Logger

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
        formatter: logging.Formatter,
        level: int,
        log_file: str,
        max_bytes: int,
        backup_count: int,
    ) -> list[logging.Handler]:
        """Sets up common StreamHandler and RotatingFileHandler"""
        handlers = []

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
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)

        return handlers

    def __init__(self, config: LogConfig | None = None) -> None:
        config = self._config

        if LoggerManager._is_initialized:
            return

        if config.LOG_MODE == "SINGLE_THREAD":
            self._init_single_thread(config)
        elif config.LOG_MODE == "MULTI_PROCESS":
            self._init_multi_process(config)
        elif config.LOG_MODE == "DISTRIBUTED":
            self._init_distributed(config)
        else:
            raise ValueError(f"Unknown log mode: {config.LOG_MODE}")

        LoggerManager._is_initialized = True

    def _init_single_thread(self, config: LogConfig) -> None:
        """Single-thread/single-process local logging"""
        logger = logging.getLogger()
        logger.setLevel(config.LOG_LEVEL)

        for h in logger.handlers[:]:
            logger.removeHandler(h)
        handlers = self._setup_common_handlers(
            config.FORMATTER,
            config.LOG_LEVEL,
            config.LOG_FILE,
            config.MAX_BYTES,
            config.BACKUP_COUNT,
        )

        for h in handlers:
            logger.addHandler(h)

    def _init_multi_process(self, config: LogConfig) -> None:
        """Multi-process logging (using QueueHandler/QueueListener)"""
        if self._log_queue is None:
            self._log_queue = multiprocessing.Queue(-1)  # Shared queue

        # Only start QueueListener in the Main Process (Root Logger receives)
        # Only handlers processed by QueueListener need formatter
        if multiprocessing.current_process().name == "MainProcess":
            handlers = self._setup_common_handlers(
                config.FORMATTER,
                config.LOG_LEVEL,
                config.LOG_FILE,
                config.MAX_BYTES,
                config.BACKUP_COUNT,
            )

            self._listener = QueueListener(self._log_queue, *handlers)
            self._listener.start()

    def _init_distributed(self, config: LogConfig) -> None:
        """Distributed logging (using SocketHandler)"""

        if multiprocessing.current_process().name == "MainProcess":
            self._server_process = multiprocessing.Process(
                target=run_log_server_process,
                args=(
                    config.SERVER_HOST,
                    config.SERVER_PORT,
                ),
                daemon=True,
            )
            self._server_process.start()
            # Give server process time to start
            time.sleep(1.5)

    @staticmethod
    def get_log_queue() -> multiprocessing.Queue[Any] | None:
        """
        Returns the multiprocessing.Queue instance used for log transmission
        in MULTI_PROCESS mode.
        """
        instance = LoggerManager._instance
        if instance is None:
            return None

        return instance._log_queue

    @staticmethod
    def setup_worker_logger(
        log_queue: multiprocessing.Queue[Any] | None = None,
        log_mode: LogMode = "SINGLE_THREAD",
        log_level: int = logging.DEBUG,
        server_host: str = LogConfig.SERVER_HOST,
        server_port: int = LogConfig.SERVER_PORT,
    ) -> None:
        """Sets up logging for worker processes (specific to multi-process modes)"""

        logger = logging.getLogger()
        logger.setLevel(log_level)

        for h in logger.handlers[:]:
            logger.removeHandler(h)

        if log_mode == "MULTI_PROCESS" and log_queue:
            logger.addHandler(QueueHandler(log_queue))
        elif log_mode == "DISTRIBUTED":
            logger.addHandler(SocketHandler(server_host, server_port))

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


def worker_task(
    name: str | int,
    log_queue: multiprocessing.Queue[Any] | None = None,
    log_mode: LogMode = "SINGLE_THREAD",
    log_level: int = logging.DEBUG,
    server_host: str = LogConfig.SERVER_HOST,
    server_port: int = LogConfig.SERVER_PORT,
) -> None:
    """Simulates a worker thread/process task"""

    # Set up the logging sending mechanism for the worker process (using the passed log_mode)
    # root logger in each process need to add the handler for transmission
    if log_mode in ("MULTI_PROCESS", "DISTRIBUTED"):
        LoggerManager.setup_worker_logger(log_queue, log_mode, log_level, server_host, server_port)

    logger = logging.getLogger(str(name))
    logger.propagate = True
    logger.setLevel(log_level)

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

    LoggerManager()
    logger = logging.getLogger(__name__)
    logger.info("--- Starting Single-Thread Test ---")

    # Pass configuration to worker_task
    worker_task("MainThread", log_mode=LogConfig.LOG_MODE, log_level=LogConfig.LOG_LEVEL)
    logger.error("Single thread test complete.")
    LoggerManager.shutdown()


def run_multi_process_test() -> None:
    # Ensure cross-platform compatibility
    multiprocessing.set_start_method("spawn", force=True)
    LogConfig.LOG_MODE = "MULTI_PROCESS"

    LoggerManager()
    logger = logging.getLogger(__name__)
    log_queue = LoggerManager.get_log_queue()
    LoggerManager.setup_worker_logger(log_queue, LogConfig.LOG_MODE, LogConfig.LOG_LEVEL)

    logger.info("--- Starting Multi-Process Test ---")

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

    # Main controller program starts the log server
    LoggerManager()
    logger = logging.getLogger(__name__)
    LoggerManager.setup_worker_logger(
        log_queue=None,
        log_mode=LogConfig.LOG_MODE,
        log_level=LogConfig.LOG_LEVEL,
        server_host=LogConfig.SERVER_HOST,
        server_port=LogConfig.SERVER_PORT,
    )

    logger.info("--- Starting Distributed Test ---")

    NUM_WORKERS = 4
    workers_ids = range(1, NUM_WORKERS + 1)

    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        pool.map(distributed_pool_worker_wrapper, workers_ids)

    logger.info("All worker processes have been dispatched.")

    time.sleep(1)

    LoggerManager.shutdown()


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
