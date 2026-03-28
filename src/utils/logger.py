
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

# ── Log file location ─────────────────────────────────────────────────────────
# Writes to logs/ folder in project root
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "pipeline.log"


# ── Color codes for terminal output ──────────────────────────────────────────
class _Colors:
    GREY    = "\x1b[38;20m"
    BLUE    = "\x1b[34;20m"
    YELLOW  = "\x1b[33;20m"
    RED     = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET   = "\x1b[0m"


class _ColorFormatter(logging.Formatter):
    """Adds colors to terminal log output based on log level."""

    FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    DATEFMT = "%Y-%m-%d %H:%M:%S"

    LEVEL_COLORS = {
        logging.DEBUG:    _Colors.GREY,
        logging.INFO:     _Colors.BLUE,
        logging.WARNING:  _Colors.YELLOW,
        logging.ERROR:    _Colors.RED,
        logging.CRITICAL: _Colors.BOLD_RED,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, _Colors.RESET)
        formatter = logging.Formatter(
            fmt=f"{color}{self.FORMAT}{_Colors.RESET}",
            datefmt=self.DATEFMT
        )
        return formatter.format(record)


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger for the given module name.
    Creates handlers only once — safe to call multiple times.
    """
    logger = logging.getLogger(name)

    # Don't add handlers if they already exist (prevents duplicate logs)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # ── Terminal handler (colored) ────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # INFO and above in terminal
    console_handler.setFormatter(_ColorFormatter())

    # ── File handler (rotating, no colors) ───────────────────
    # Max 5MB per file, keeps last 3 files
    file_handler = RotatingFileHandler(
        _LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)  # DEBUG and above in file
    file_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Prevent logs bubbling up to root logger (avoids duplicates)
    logger.propagate = False

    return logger
