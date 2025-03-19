from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class RosBag:
    _id: str
    car_id: str
    name: str
    oss_path: str
    collect_type: str
    splits: List[str]
    comment: Optional[str] = None
    status: Optional[str] = None
    collect_size: Optional[int] = None
    collect_duration: Optional[int] = None
    collect_time: Optional[str] = None
    meta: "dict | None" = None


@dataclass
class ParsedRosBag:
    _id: str
    rosbag_id: str
    dag_run_id: str
    launch_time: str
    rhea_version: str
    calibrated_sensors: Dict
    parse_path: Optional[str] = None
    status: Optional[str] = None
    tags: Optional[Dict[str, bool]] = None
    visualization_path: Optional[str] = None
    # 生成新的nori同步到brain++ path
    save_bpp_path: Optional[str] = None
    failed_in: Optional[str] = None
    extra_info: Optional[Dict] = None


@dataclass
class ErrorRosBag:
    _id: str
    car_id: str
    name: str
    oss_path: str
    collect_type: str
    splits: List[str]


@dataclass
class EventRosBag:
    _id: str
    uuid: str
    car_id: str
    name: str
    oss_path: str
    collect_type: str
    collect_date: str
    collect_duration: str
    start_timestamp: str
    end_timestamp: str
    splits: List[str]
    status: str
    meta_data: Dict
    aliyun_key: str
    aliyun_bucket: str
    aliyun_endpoint: str
    oss_bucket_prefix: str
    comment: Optional[str] = None
    tags: Optional[List[str]] = None
    route: Optional[str] = None
    location: Optional[str] = None
    operator: Optional[str] = None
    # 通过邮寄上传的oss目录
    input_oss_path: Optional[str] = None
    # 该bag里event被收藏次数
    stars: Optional[int] = None


@dataclass
class CategoriedParsedRosBag:
    _id: str
    parsed_rosbag_id: str
    rhea_version: str
    ramp: Optional[int] = 0
    junction: Optional[int] = 0
    straightway: Optional[int] = 0
    # err_key: ramp, junction, straightway
    # err_value: no_tag, frames_under_threshold, unknown, None(success)
    error_info: Optional[Dict[str, bool]] = None


@dataclass
class StatisticParsedRosBag:
    _id: str
    parsed_rosbag_id: str
    rhea_version: str
    # err_key: ramp, junction, straightway
    type: str
    frame_num: Optional[int] = 0
    # err_value: no_tag, frames_under_threshold, unknown, None(success)
    error_info: Optional[str] = None
