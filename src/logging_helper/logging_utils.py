from basic_imports import *
from loguru import logger
from loguru import _logger
from typing import Any, Union


class LogFilters:
    """Class that will be used to filter logs based on the `where` and `inside` fields"""

    def __init__(self):
        self._where_inside: dict[str, list[str]] = {}
        """Dictionary containing info about `where` (in which class)
        amd `inside` which function in the class, we want to see logs"""
        # self.level: Union[str, int] = "DEBUG"
        # """Level of the logs that we want to see"""

    def add_filter(self, where: str, inside: list[str]):
        """Add a filter to the logger"""
        res = self._where_inside.get(where, [])
        # Add only the elements that are not already in the list
        self._where_inside[where] = list(set(res + inside))

    def __call__(self, record: dict[str, Any]):
        """Filter function that will be called by loguru"""
        if record["extra"].get("where") in self._where_inside:
            if (
                record["extra"].get("inside")
                in self._where_inside[record["extra"].get("where")]
            ):
                return True
        return False

def custom_log_format(record):
    time_format = (
        "<blue>{time:MMM D, YYYY -> HH:mm:ss}</blue>"
        # + f" % {elapsed_time.total_seconds():.4f} sec</blue>"
    )
    time_format = f"{time_format:<35}"
    file_name = record["file"].name
    thread_name = record["thread"].name
    # process_info = record["process"]
    function_name = record["function"]
    if function_name[0] == "<":
        function_name = f"\\{function_name}"
    line_num = record["line"]
    where_str = f"<cyan>{file_name}|thread={thread_name}|function={function_name}:{line_num}</cyan>"
    where_str = f"{where_str:<50}"
    if record["extra"]:
        return (
            f"[{time_format}]"
            + " | <yellow>{level}</yellow> | "
            + f"{where_str}"
            + " | <bold>{message}</bold> | <magenta>{extra}</magenta>\n\n"
        )
    return (
        f"[{time_format}]"
        + " | <yellow>{level}</yellow> | "
        + f"{where_str}"
        + " | <bold>{message}</bold>\n\n"
    )
