from enum import Enum
from ml_final_project.config import logger


class Color(Enum):
    """Enum for colors used in the project.

    Primary colors are inspired by the accessible color palette from
    https://github.com/mpetroff/accessible-color-cycles
    """

    # Primary
    BLUE = "#5790fc"
    ORANGE = "#f89c20"
    RED = "#e42536"
    PURPLE = "#964a8b"
    GRAY = "#9c9ca1"
    VIOLET = "#7a21dd"

    # Secondary
    DARK_GRAY = "#3b3b3b"
    LIGHT_GRAY = "#BFBFBF"

    DARK_BLUE = "#072ea5"
    LIGHT_BLUE = "#91c2ed"

    DARK_ORANGE = "#ff3e06"
    LIGHT_ORANGE = "#fbb181"

    DARK_RED = "#a50000"
    LIGHT_RED = "#ea6466"

    SOFT_BLACK = "#333333"


logger.info(
    "Accessible color palette inspired by https://github.com/mpetroff/accessible-color-cycles."
)
