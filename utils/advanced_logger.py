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
import yaml

APP_ENV = os.getenv("APP_ENV", "DEV").upper()

LogMode = Literal["SINGLE_THREAD", "MULTI_PROCESS", "DISTRIBUTED"]
NOTICE_LEVEL_NUM = 25
NOTICE_LEVEL_NAME = "NOTICE"

logging.addLevelName(NOTICE_LEVEL_NUM, NOTICE_LEVEL_NAME)


def notice(self: logging.Logger, message: str, *args: object, **kws: Any) -> None:
    if self.isEnabledFor(NOTICE_LEVEL_NUM):
        self._log(NOTICE_LEVEL_NUM, message, args, **kws)


logging.Logger.notice = notice  # type: ignore


def get_module_logger(name: str) -> logging.Logger:
    """Helper function to get a module-specific logger."""
    return logging.getLogger(name)


class LogConfig:
    """Application logging configuration for centralized parameter management."""

    def __init__(self) -> None:
        config_path = os.path.join(
            os.path.dirname(__file__), f"./config/logger_{APP_ENV.lower()}.yaml"
        )
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)
            self.logger_cfg = config_data

    def get_logger_mode(self) -> LogMode:
        return self.logger_cfg.get("mode", "SINGLE_THREAD")

    def get_logger_level(self) -> int:
        level_str = self.logger_cfg.get("level", "INFO").upper()
        level_num = getattr(logging, level_str, logging.NOTSET)
        if level_num == logging.NOTSET:
            if level_str == NOTICE_LEVEL_NAME:
                return NOTICE_LEVEL_NUM

            return logging.INFO

        return level_num

    def get_logger_server_address(self) -> tuple[str, int]:
        host = socket.gethostbyname(socket.gethostname())
        port = logging.handlers.DEFAULT_TCP_LOGGING_PORT
        return (host, port)

    def get_handler_config(self) -> dict[str, Any]:
        handlers_cfg = self.logger_cfg.get("handlers", {})
        formatters_cfg = self.logger_cfg.get("formatters", {})
        for handler in handlers_cfg.values():
            fmt_name = handler.get("formatter")
            if fmt_name and fmt_name in formatters_cfg:
                fmt_str = formatters_cfg[fmt_name].get("format", "")
                handler["formatter"] = logging.Formatter(fmt_str)
        return handlers_cfg


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
            config = LogConfig()
            _server_logger = logging.getLogger()
            _server_logger.setLevel(config.get_logger_level())  # Receives logs of all levels

            for h in _server_logger.handlers[:]:
                _server_logger.removeHandler(h)

            handlers = LoggerManager._setup_common_handlers()
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


def run_log_server_process() -> None:
    """Runs the log server in a separate process."""
    try:
        config = LoggerManager._instance._config if LoggerManager._instance else LogConfig()
        (host, port) = config.get_logger_server_address()
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

    def __new__(cls) -> LoggerManager:
        config = LogConfig()

        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._config = config
        return cls._instance

    @staticmethod
    def _setup_common_handlers() -> list[logging.Handler]:
        """Sets up common StreamHandler and RotatingFileHandler"""
        config = LoggerManager._instance._config if LoggerManager._instance else LogConfig()
        handlers_cfg = config.get_handler_config()

        handlers = []

        log_file = handlers_cfg["file"]["filename"]
        log_dir = os.path.dirname(log_file)
        if os.path.exists(log_file) and os.path.isfile(log_file):
            os.remove(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=handlers_cfg["file"]["maxBytes"],
            backupCount=handlers_cfg["file"]["backupCount"],
            encoding="utf-8",
        )
        file_handler.setLevel(handlers_cfg["file"]["level"])
        file_handler.setFormatter(handlers_cfg["file"]["formatter"])
        handlers.append(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(handlers_cfg["stream"]["level"])
        stream_handler.setFormatter(handlers_cfg["stream"]["formatter"])
        handlers.append(stream_handler)

        return handlers

    def __init__(self) -> None:
        if LoggerManager._is_initialized:
            return
        config = self._config

        # config_data = config.get_handler_config()

        logger_mode = config.get_logger_mode()

        if logger_mode == "SINGLE_THREAD":
            self._init_single_thread(config)
        elif logger_mode == "MULTI_PROCESS":
            self._init_multi_process(config)
        elif logger_mode == "DISTRIBUTED":
            self._init_distributed(config)
        else:
            raise ValueError(f"Unknown log mode: {logger_mode}")

        LoggerManager._is_initialized = True

    def _init_single_thread(self, config: LogConfig) -> None:
        """Single-thread/single-process local logging"""
        logger = logging.getLogger()
        logger_level = config.get_logger_level()
        logger.setLevel(logger_level)

        for h in logger.handlers[:]:
            logger.removeHandler(h)
        handlers = self._setup_common_handlers()

        for h in handlers:
            logger.addHandler(h)

    def _init_multi_process(self, config: LogConfig) -> None:
        """Multi-process logging (using QueueHandler/QueueListener)"""
        if self._log_queue is None:
            self._log_queue = multiprocessing.Queue(-1)  # Shared queue

        if multiprocessing.current_process().name == "MainProcess":
            handlers = self._setup_common_handlers()

            self._listener = QueueListener(self._log_queue, *handlers, respect_handler_level=True)
            self._listener.start()

    def _init_distributed(self, config: LogConfig) -> None:
        """Distributed logging (using SocketHandler)"""

        if multiprocessing.current_process().name == "MainProcess":
            # (server_host, server_port) = config.get_logger_server_address()
            self._server_process = multiprocessing.Process(
                target=run_log_server_process,
                args=(),
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
    ) -> None:
        """Sets up logging for worker processes (specific to multi-process modes)"""

        config = LoggerManager._instance._config if LoggerManager._instance else LogConfig()
        log_mode = config.get_logger_mode()
        log_level = config.get_logger_level()
        (server_host, server_port) = config.get_logger_server_address()

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

    def notice(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.notice(msg, *args, **kwargs)

    def warning(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: object, **kwargs: Any) -> None:  # noqa: ANN401
        self.logger.critical(msg, *args, **kwargs)


def worker_task(
    name: str | int,
    log_queue: multiprocessing.Queue[Any] | None = None,
) -> None:
    """Simulates a worker thread/process task"""

    # Set up the logging sending mechanism for the worker process (using the passed log_mode)
    # root logger in each process need to add the handler for transmission
    config = LoggerManager._instance._config if LoggerManager._instance else LogConfig()
    log_mode = config.get_logger_mode()
    if log_mode in ("MULTI_PROCESS", "DISTRIBUTED"):
        LoggerManager.setup_worker_logger(log_queue)

    logger = get_module_logger(f"worker_{name}")

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
    # LogConfig.LOG_MODE = "SINGLE_THREAD"

    LoggerManager()
    logger = logging.getLogger(__name__)
    logger.info("--- Starting Single-Thread Test ---")
    # logger.notice("--- Test NOTICE level message ---")

    # Pass configuration to worker_task
    worker_task("MainThread")
    logger.error("Single thread test complete.")
    LoggerManager.shutdown()


def run_multi_process_test() -> None:
    # Ensure cross-platform compatibility
    multiprocessing.set_start_method("spawn", force=True)

    LoggerManager()
    log_queue = LoggerManager.get_log_queue()
    LoggerManager.setup_worker_logger(log_queue)
    logger = get_module_logger(__name__)

    logger.info("--- Starting Multi-Process Test ---")

    procs = [
        multiprocessing.Process(
            target=worker_task,
            args=(
                i,
                log_queue,
            ),
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
    )


def run_distributed_test() -> None:
    multiprocessing.set_start_method("spawn", force=True)

    # Main controller program starts the log server
    LoggerManager()
    LoggerManager.setup_worker_logger(
        log_queue=None,
    )
    logger = get_module_logger(__name__)

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
