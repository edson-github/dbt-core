from dbt import ui
from dbt.node_types import NodeType
from typing import Optional, Union
from datetime import datetime


def format_fancy_output_line(
    msg: str,
    status: str,
    index: Optional[int],
    total: Optional[int],
    execution_time: Optional[float] = None,
    truncate: bool = False,
) -> str:
    progress = "" if index is None or total is None else f"{index} of {total} "
    prefix = "{progress}{message} ".format(progress=progress, message=msg)

    truncate_width = ui.printer_width() - 3
    justified = prefix.ljust(ui.printer_width(), ".")
    if truncate and len(justified) > truncate_width:
        justified = f"{justified[:truncate_width]}..."

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(execution_time=execution_time)

    return "{justified} [{status}{status_time}]".format(
        justified=justified, status=status, status_time=status_time
    )


def _pluralize(string: Union[str, NodeType]) -> str:
    try:
        convert = NodeType(string)
    except ValueError:
        return f"{string}s"
    else:
        return convert.pluralize()


def pluralize(count, string: Union[str, NodeType]):
    pluralized = _pluralize(string) if count != 1 else str(string)
    return f"{count} {pluralized}"


def timestamp_to_datetime_string(ts):
    timestamp_dt = datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9)
    return timestamp_dt.strftime("%H:%M:%S.%f")
