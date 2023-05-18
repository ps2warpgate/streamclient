# TODO: Move to constants
from pydantic.dataclasses import dataclass

from constants.typings import World, Zone


class Config:
    validate_assignment = True


@dataclass(config=Config)
class MetagameEvent:
    id: str
    event_id: int
    state: str
    world_id: World
    zone_id: Zone
    nc: float
    tr: float
    vs: float
    xp: float
    timestamp: float
