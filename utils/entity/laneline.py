from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class LaneLineTag:
    _id: str
    parsed_rosbag_id: str
    rhea_version: str
    tag: str
    frame_num: Optional[int] = 0
    rule: Optional[Dict] = None
    ranges: Optional[List[List]] = None
    # err_value: no_tag, frames_under_threshold, unknown, None(success)
    error_info: Optional[str] = None
    map_json_status: Optional[str] = None
    map_json_path: Optional[str] = None


@dataclass
class LanePreprocess:
    _id: str
    tag_type: str
    # [junction, straightway, ramp]
    parsed_rosbag_id: str
    slam_status: str = "not_yet"
    # [not_yet, running, finish, failed]
    rv_laneline_status: str = "not_yet"
    # [not_yet, running, finish, failed]
    rv_stopline_status: str = "skip"
    # [skip, not_yet, running, finish, failed]
    preprocess_state: str = "not_yet"
    # [not_yet, ready, finish], state summary slam rv
