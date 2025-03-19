from dataclasses import dataclass
from typing import Dict, List, Union, Optional

from utils.entity.ros_message import Camera
from utils.entity.ros_message import CorrImu
from utils.entity.ros_message import Gps
from utils.entity.ros_message import Ins
from utils.entity.ros_message import Lidar
from utils.entity.ros_message import LidarSlamPose
from utils.entity.ros_message import Radar
from utils.entity.ros_message import RawImu
from utils.entity.ros_message import Tag
from utils.entity.ros_message import VehicleReportCommon
from utils.entity.ros_message import CameraExtraInfo


@dataclass
class Frame:
    _id: str
    parsed_rosbag_id: str
    timestamp: str
    fuser_lidar: Lidar
    lidar: Dict[str, Union[Lidar, None]]
    radar: Dict[str, Union[Radar, None]]
    camera: Dict[str, Union[Camera, None]]
    ins: Union[Ins, None]
    gps: Union[Gps, None]
    lidar_slam_pose: Union[LidarSlamPose, None]
    tag: Union[Tag, None]
    vehicle_report: Union[VehicleReportCommon, None]
    corr_imu: Optional[Union[CorrImu, None]] = None
    raw_imu: Optional[Union[RawImu, None]] = None
    adjacent_camera: Optional[Dict[str, List[Dict]]] = None
    camera_extra_info: Optional[Dict[str, Union[CameraExtraInfo, None]]] = None
    is_key_frame: Optional[Union[bool, None]] = None 

    def as_dict(self):
        return {
            "_id": self._id,
            "parsed_rosbag_id": self.parsed_rosbag_id,
            "timestamp": self.timestamp,
            "fuser_lidar": self.fuser_lidar.as_dict() if self.fuser_lidar is not None else None,
            "lidar": {key: lidar.as_dict() if lidar is not None else None for key, lidar in self.lidar.items()},
            "radar": {key: radar.as_dict() if radar is not None else None for key, radar in self.radar.items()},
            "camera": {key: camera.as_dict() if camera is not None else None for key, camera in self.camera.items()},
            "adjacent_camera": self.adjacent_camera,
            "ins": self.ins.as_dict() if self.ins is not None else None,
            "gps": self.gps.as_dict() if self.gps is not None else None,
            "lidar_slam_pose": self.lidar_slam_pose.as_dict() if self.lidar_slam_pose is not None else None,
            "tag": self.tag.as_dict() if self.tag is not None else None,
            "vehicle_report": self.vehicle_report.as_dict() if self.vehicle_report is not None else None,
            "corr_imu": self.corr_imu.as_dict() if self.corr_imu is not None else None,
            "raw_imu": self.raw_imu.as_dict() if self.raw_imu is not None else None,
            "camera_extra_info": {
                key: camera.as_dict() if camera else None for key, camera in self.camera_extra_info.items()
            }
            if self.camera_extra_info is not None
            else None,
        }


@dataclass
class Clip:
    _id: str
    parsed_rosbag_id: str
    # {"天气_晴天": true, "数据类型_训练数据": true}
    tags: Dict[str, bool]
    start_time: str
    end_time: str
    origin_data: Dict[str, str]
    aligned_frame_list: List[Frame]
    fps: int
    idx_in_ppl_bag: int
    available: Optional[Union[bool, None]] = None
    oss_output_path: Optional[Union[str, None]] = None
    is_key_frame: Optional[Union[bool, None]] = None
    """
    origin_data: {
        "origin_camera_data_id": "633565de686e2f1840068bac",
        "origin_corr_imu_data_id": "633565de686e2f1840068bad",
        "origin_ins_data_id": "633565df686e2f1840068bae",
        "origin_gps_data_id": "633565e0686e2f1840068baf",
        "origin_radar_data_id": "633565e0686e2f1840068bb0",
        "origin_lidar_data_id": "633565e0686e2f1840068bb1",
        "origin_raw_imu_data_id": "633565e0686e2f1840068bb2",
        "origin_tag_data_id": "633565e0686e2f1840068bb3",
        "origin_ultrasonic_radar_data_id": "633565e1686e2f1840068bb4",
        "origin_vehicle_report_data_id": "633565e1686e2f1840068bb5"
    }
    """


@dataclass
class Dataset:
    _id: str
    parsed_rosbag_id: str
    clip_ids: List[str]
    dag_run_id: str
    oss_output_path: str


@dataclass
class OriginLidarData:
    _id: str
    lidar_list: List[Dict]


@dataclass
class OriginCameraData:
    _id: str
    camera_list: List[Dict]


@dataclass
class OriginRadarData:
    _id: str
    radar_list: List[Dict]


@dataclass
class OriginUltrasonicRadarData:
    _id: str
    radar_list: List[Dict]


@dataclass
class OriginRawImuData:
    _id: str
    raw_imu_list: List[Dict]


@dataclass
class OriginCorrImuData:
    _id: str
    corr_imu_list: List[Dict]


@dataclass
class OriginInsData:
    _id: str
    ins_list: List[Dict]


@dataclass
class OriginGpsData:
    _id: str
    gps_list: List[Dict]


@dataclass
class OriginTagData:
    _id: str
    tag_list: List[Dict]


@dataclass
class OriginVehicleReportData:
    """兼容vehicle_report_info 和 vehicle_report_common"""

    _id: str
    vehicle_report_list: List[Dict]
