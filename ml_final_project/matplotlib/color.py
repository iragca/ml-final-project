from enum import Enum
from ml_final_project.config import logger


class Color(Enum):
    """Enum for colors used in the project.

    Inspired by the accessible color palette from
    https://github.com/mpetroff/accessible-color-cycles
    """

    BLUE = "#5790fc"
    ORANGE = "#f89c20"
    RED = "#e42536"
    PURPLE = "#964a8b"
    GRAY = "#9c9ca1"
    VIOLET = "#7a21dd"


color_str = "\n"
for color in Color:
    color_str += f"{color.name}: {color.value}\n"

logger.info(
    f"Accessible colors by https://github.com/mpetroff/accessible-color-cycles. {color_str}",
)
