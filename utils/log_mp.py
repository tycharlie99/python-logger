import logging
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import multiprocessing
import os
from typing import Optional
import time
import random


class LoggerSingleton:
    _instance: Optional["LoggerSingleton"] = None
    _is_initialized: bool = False
    _log_queue: Optional[multiprocessing.Queue] = None
    _listener: Optional[QueueListener] = None

    _log_file: str
    _level: int
    _max_bytes: int
    _backup_count: int
    logger: logging.Logger

    def __new__(
        cls,
        log_queue: multiprocessing.Queue,
        log_file="app.log",
        level=logging.DEBUG,
        max_bytes=10 * 1024 * 1024,
        backup_count=5,
    ):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._log_file = log_file
            cls._instance._level = level
            cls._instance._max_bytes = max_bytes
            cls._instance._backup_count = backup_count
            cls._instance._log_queue = log_queue
        return cls._instance

    @staticmethod
    def _setup_listener(
        queue_ref: multiprocessing.Queue,
        log_file: str,
        level: int,
        max_bytes: int,
        backup_count: int,
    ):
        formatter = logging.Formatter(
            "%(asctime)s - %(process)d - %(thread)d - %(levelname)s - %(module)s - %(message)s"
        )

        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)

        listener = QueueListener(queue_ref, file_handler, stream_handler)
        listener.start()
        return listener

    def __init__(self, *args, **kwargs):
        if LoggerSingleton._is_initialized:
            return

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self._level)

        # Remove old handlers
        for h in self.logger.handlers[:]:
            self.logger.removeHandler(h)

        # Add QueueHandler
        self.logger.addHandler(QueueHandler(self._log_queue))

        # Start listener in main process
        if (
            LoggerSingleton._listener is None
            and multiprocessing.current_process().name == "MainProcess"
        ):
            LoggerSingleton._listener = LoggerSingleton._setup_listener(
                self._log_queue,
                self._log_file,
                self._level,
                self._max_bytes,
                self._backup_count,
            )

        LoggerSingleton._is_initialized = True
        self.logger.info("LoggerSingleton initialized (cross-platform process-safe).")

    @staticmethod
    def setup_worker_logger(log_queue: multiprocessing.Queue):
        """
        Worker process call, set QueueHandler
        """
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        for h in root.handlers[:]:
            root.removeHandler(h)
        root.addHandler(QueueHandler(log_queue))

    @staticmethod
    def shutdown():
        if LoggerSingleton._listener:
            LoggerSingleton._listener.stop()
            LoggerSingleton._listener = None

    # --- Wrapper methods ---
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)


def worker_task(name, log_queue):
    LoggerSingleton.setup_worker_logger(log_queue)
    logger = logging.getLogger()
    logger.info(f"Worker {name} start")
    for i in range(3):
        logger.debug(f"Worker {name} step {i}")
        rand_num = random.uniform(0.5, 2)
        time.sleep(rand_num)
    logger.info(f"Worker {name} done")


if __name__ == "__main__":
    # Ensure cross-platform spawn mode
    multiprocessing.set_start_method("fork", force=True)

    # Native multiprocessing queue
    log_queue = multiprocessing.Queue(-1)

    # Main process initializes LoggerSingleton
    logger = LoggerSingleton(log_queue, log_file="multi_process_application.log")
    logger.info("Main process start")

    # Start multiple workers
    procs = [
        multiprocessing.Process(target=worker_task, args=(i, log_queue))
        for i in range(3)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    logger.info("All workers finished")
    LoggerSingleton.shutdown()
