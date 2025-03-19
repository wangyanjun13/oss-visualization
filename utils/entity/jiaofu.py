from dataclasses import dataclass
from typing import Dict, List, Union, Optional


@dataclass
class jiaofu:
    _id: str
    src_path: str
    dst_path: str
    data_type: str
    state: str
    fuser_lidar_count: int
    jpg_count: int
    data_info: Dict
    time_info: Dict
    front_2_lidar_count: Optional[int] = 0
