
import logging
from logging.handlers import RotatingFileHandler
import os
from typing import Optional

class LoggerSingleton:
    """
    A Singleton class designed to encapsulate the standard Python logging
    module, ensuring only one logger instance exists throughout the application
    with a consistent configuration.
    """
    _instance: Optional['LoggerSingleton'] = None
    _is_initialized: bool = False

    # Declare instance attributes with type hints. This resolves the static analysis error
    # by letting type checkers know these attributes exist on the instance.
    _log_file: str
    _level: int
    _max_bytes: int
    _backup_count: int
    logger: logging.Logger

    def __new__(cls, log_file='app.log', level=logging.DEBUG, max_bytes=10485760, backup_count=5):
        """
        Implements the Singleton pattern. This method controls instance creation.
        If an instance already exists, it returns the existing one.
        """
        if cls._instance is None:
            cls._instance = super(LoggerSingleton, cls).__new__(cls)
            # Configuration will happen in __init__ which is called right after __new__
            # We pass the arguments via __new__ and store them here temporarily
            cls._instance._log_file = log_file
            cls._instance._level = level
            cls._instance._max_bytes = max_bytes
            cls._instance._backup_count = backup_count
        return cls._instance

    def __init__(self, *args, **kwargs):
        """
        Initializes the logger configuration only once.
        This signature is updated to accept *args and **kwargs to prevent the TypeError,
        as Python automatically passes instantiation arguments to __init__.
        The _is_initialized flag prevents re-configuration on subsequent calls.
        """
        if LoggerSingleton._is_initialized:
            return

        # 1. Create the main logger object
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self._level)

        # Ensure handlers are not duplicated if the class is accessed multiple times
        if not self.logger.handlers:
            # 2. Define a standard formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(module)s - %(message)s'
            )

            # --- Console Handler (StreamHandler) ---
            # Logs messages to the standard output (console)
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.INFO)  # Console shows INFO level and above
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

            # --- File Handler (RotatingFileHandler) ---
            # Logs messages to a file, rotating logs when they reach a size limit
            try:
                # Ensure the log file directory exists if needed
                log_dir = os.path.dirname(self._log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir)

                file_handler = RotatingFileHandler(
                    filename=self._log_file,
                    maxBytes=self._max_bytes, # 10MB
                    backupCount=self._backup_count, # Keep 5 backup files
                    encoding='utf-8'
                )
                file_handler.setLevel(self._level)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                # Fallback to console logging if file setup fails
                self.logger.error(f"Failed to set up file logging: {e}")


        LoggerSingleton._is_initialized = True
        self.logger.info("LoggerSingleton initialized successfully.")

    # --- Wrapper methods for convenience ---

    def debug(self, msg, *args, **kwargs):
        """Log 'msg % args' with severity 'DEBUG'."""
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """Log 'msg % args' with severity 'INFO'."""
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """Log 'msg % args' with severity 'WARNING'."""
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """Log 'msg % args' with severity 'ERROR'."""
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """Log 'msg % args' with severity 'CRITICAL'."""
        self.logger.critical(msg, *args, **kwargs)

# --- Example Usage ---

def module_a_function():
    """Function demonstrating logging usage in Module A."""
    logger_a = LoggerSingleton()
    logger_a.info("Module A: Starting processing.")
    logger_a.debug("Module A: This is a detailed debug message.")
    try:
        result = 10 / 0
    except ZeroDivisionError:
        logger_a.error("Module A: An arithmetic error occurred.", exc_info=True)

def module_b_function():
    """Function demonstrating logging usage in Module B."""
    logger_b = LoggerSingleton()
    logger_b.warning("Module B: A non-critical event happened.")
    logger_b.critical("Module B: System shutdown imminent.")


if __name__ == "__main__":
    # The first call to LoggerSingleton() creates and configures the instance.
    print("--- Initializing LoggerSingleton (First Call) ---")
    log_file_path = 'my_application.log'
    logger1 = LoggerSingleton(log_file=log_file_path, level=logging.DEBUG)

    # Subsequent calls return the exact same instance, avoiding re-configuration.
    print("\n--- Retrieving LoggerSingleton (Second Call) ---")
    logger2 = LoggerSingleton()

    # Verify that both variables point to the same instance
    print(f"Is logger1 the same object as logger2? {logger1 is logger2}")
    print(f"logger1 object ID: {id(logger1)}")
    print(f"logger2 object ID: {id(logger2)}")

    print("\n--- Testing Log Messages (Will write to console and file) ---")

    logger1.info("Main App: Application started successfully.")

    # Call functions from different conceptual modules
    module_a_function()
    module_b_function()

    logger2.info("Main App: Application finished.")

    # Check the log file location
    print(f"\nLog messages saved to: {os.path.abspath(log_file_path)}")
    print("Check the file contents to see all messages recorded.")

