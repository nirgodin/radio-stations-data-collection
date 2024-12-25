from enum import Enum


class JobId(str, Enum):
    RADIO_SNAPSHOTS = "radio_snapshots"
    CONSTANT_LOGGER = "constant_logger"
