import logging
import os

from rich.logging import RichHandler

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

log_file_path = os.path.join(log_dir, "app.log")

log_format = "%(asctime)s %(levelname)s %(message)s"
log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=log_level,
    format=log_format,
    datefmt="[ %Y-%m-%d %H:%M:%S ]",
)

console_handler = RichHandler()
console_handler.setLevel(log_level)
console_handler.setFormatter(logging.Formatter("%(message)s"))

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(log_level)
file_handler.setFormatter(logging.Formatter(log_format))

logger = logging.getLogger("autoperf_logger")
logger.setLevel(log_level)
logger.addHandler(console_handler)
logger.addHandler(file_handler)
