import sys
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Only add a handler if there isn't one already
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)