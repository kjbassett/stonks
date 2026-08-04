"""
Centralized logging configuration for the stonks application.

Call setup_logging() once at process startup (e.g. in main.py or the live trading entry point).
Tests do NOT call this; they use unittest's assertLogs() instead.

Log routing
-----------
  trading.*  ->  logs/trading_logs/trading.log   (daily rotation, 30 days retained)
  everything ->  logs/app.log                     (rotating file handler)
  INFO+      ->  stderr console

All file handlers are wrapped in QueueHandler + QueueListener so logging calls
return immediately without waiting for disk I/O (the Listener drains the queue
in a background daemon thread).
"""

import logging
import logging.handlers
import os
import queue


_listener: logging.handlers.QueueListener | None = None


def setup_logging(log_dir: str | None = None) -> None:
    """
    Configure application-wide logging. Safe to call multiple times — subsequent
    calls are no-ops once the listener is running.

    Reads settings from config["logging"] if available:
        log_dir      — directory for log files (default: "logs")
        max_bytes    — max size of app.log before rotation (default: 10 MB)
        backup_count — number of rotated app.log backups to keep (default: 5)
        level        — minimum log level name (default: "INFO")
    """
    global _listener
    if _listener is not None:
        return  # Already configured

    try:
        from src.utils.project_utilities import config as _cfg
        log_cfg = _cfg.get("logging", {})
    except Exception:
        log_cfg = {}

    if log_dir is None:
        log_dir = log_cfg.get("log_dir", "logs")
    max_bytes = int(log_cfg.get("max_bytes", 10 * 1024 * 1024))
    backup_count = int(log_cfg.get("backup_count", 5))
    level_name = log_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    trading_dir = os.path.join(log_dir, "trading_logs")
    os.makedirs(trading_dir, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- File handlers ---
    trading_file = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(trading_dir, "trading.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    trading_file.setLevel(logging.DEBUG)
    trading_file.setFormatter(fmt)

    app_file = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    app_file.setLevel(logging.DEBUG)
    app_file.setFormatter(fmt)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(fmt)

    # --- Queue-backed non-blocking listener ---
    log_queue: queue.Queue = queue.Queue(maxsize=-1)
    _listener = logging.handlers.QueueListener(
        log_queue, trading_file, app_file, console, respect_handler_level=True
    )
    _listener.start()

    queue_handler = logging.handlers.QueueHandler(log_queue)

    # Root logger: receives everything
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(queue_handler)

    # trading.* logger: routed separately, does not propagate to root
    trading_logger = logging.getLogger("trading")
    trading_logger.setLevel(logging.DEBUG)
    trading_logger.propagate = False
    trading_logger.addHandler(queue_handler)


def shutdown_logging() -> None:
    """Stop the queue listener gracefully (call at process exit if needed)."""
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None
