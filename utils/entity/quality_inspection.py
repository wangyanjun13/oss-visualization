from typing import Dict

from dataclasses import dataclass


@dataclass
class QualityInspectionRes:
    _id: str
    car_id: str
    num_split: int
    parsed_rosbag_id: str
    camera: Dict
    lidar: Dict
    corr_imu: Dict
    gps: Dict
    ins: Dict
    raw_imu: Dict
    radar: Dict
    vehicle_report: Dict
    tag: Dict
    frame: Dict
    clips: Dict
    det_annotask: Dict
