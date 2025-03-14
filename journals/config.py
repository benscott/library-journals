from pathlib import Path
import logging
import colorlog

ROOT_DIR = Path(__file__).parent.parent.resolve()

DATA_DIR = ROOT_DIR / 'data'

PROCESSING_DATA_DIR = Path(DATA_DIR / 'processing')
PROCESSING_DATA_DIR.mkdir(parents=True, exist_ok=True)

INPUT_DATA_DIR = Path(DATA_DIR / 'input')
INPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DATA_DIR = Path(DATA_DIR / 'output')
OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)

ASSETS_DIR = Path(DATA_DIR / 'assets')

JAR_DIR = ASSETS_DIR / 'python-sutime/sutime/jars'

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)

# Create a handler
handler = logging.StreamHandler()

# Define colorized formatter
formatter = colorlog.ColoredFormatter(
    "%(log_color)s[%(levelname)s]%(reset)s %(message)s",
    log_colors={
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_red",
    }
)

handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)