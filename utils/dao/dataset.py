from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.dao.ros_message import CameraRosMessageDAO
from utils.dao.ros_message import CorrImuRosMessageDAO
from utils.dao.ros_message import GpsRosMessageDAO
from utils.dao.ros_message import InsRosMessageDAO
from utils.dao.ros_message import LidarRosMessageDAO
from utils.dao.ros_message import RadarRosMessageDAO
from utils.dao.ros_message import RawImuRosMessageDAO
from utils.dao.ros_message import TagRosMessageDAO
from utils.dao.ros_message import VehicleReportCommonRosMessageDAO
from utils.dao.ros_message import VehicleReportInfoRosMessageDAO
from utils.dao.ros_message import CameraExtraInfoRosMessageDAO
from utils.entity import Clip
from utils.entity import Dataset
from utils.entity import Frame
from utils.entity import LidarSlamPose
from utils.entity import OriginCameraData
from utils.entity import OriginCorrImuData
from utils.entity import OriginGpsData
from utils.entity import OriginInsData
from utils.entity import OriginLidarData
from utils.entity import OriginRadarData
from utils.entity import OriginRawImuData
from utils.entity import OriginTagData
from utils.entity import OriginUltrasonicRadarData
from utils.entity import OriginVehicleReportData


class ClipDAO(RheaDAOBase):
    _collection = db.clip_collection
    _dataclass = Clip

    @classmethod
    def convert_document_to_dataclass(cls, document):
        document["_id"] = str(document["_id"])
        document.pop("write_time", None)
        document.pop("update_time", None)
        document.pop("git_commit", None)
        document["aligned_frame_list"] = [
            Frameutils.dao.convert_document_to_dataclass(frame) for frame in document["aligned_frame_list"]
        ]
        if "idx_in_ppl_bag" not in document:
            document["idx_in_ppl_bag"] = None

        return cls._dataclass(**document)


class DatasetDAO(RheaDAOBase):
    _collection = db.dataset_collection
    _dataclass = Dataset


class FrameDAO(RheaDAOBase):
    _collection = db.frame_collection
    _dataclass = Frame

    @classmethod
    def convert_document_to_dataclass(cls, document):
        document["_id"] = str(document["_id"])
        document.pop("write_time", None)
        document.pop("update_time", None)
        document.pop("git_commit", None)
        if document["lidar_slam_pose"] is not None:
            document["lidar_slam_pose"] = LidarSlamPose(**document["lidar_slam_pose"])

        cls._convert_subdocument_to_dataclass(document, "fuser_lidar", LidarRosMessageDAO)
        cls._convert_subdocument_to_dataclass(document, "ins", InsRosMessageDAO)
        cls._convert_subdocument_to_dataclass(document, "gps", GpsRosMessageDAO)
        cls._convert_subdocument_to_dataclass(document, "tag", TagRosMessageDAO)
        cls._convert_subdocument_to_dataclass(document, "corr_imu", CorrImuRosMessageDAO)
        cls._convert_subdocument_to_dataclass(document, "raw_imu", RawImuRosMessageDAO)
        try:
            cls._convert_subdocument_to_dataclass(document, "vehicle_report", VehicleReportInfoRosMessageDAO)
        except:
            cls._convert_subdocument_to_dataclass(document, "vehicle_report", VehicleReportCommonRosMessageDAO)

        for key in document["lidar"]:
            cls._convert_subdocument_to_dataclass(document["lidar"], key, LidarRosMessageDAO)
        for key in document["camera"]:
            cls._convert_subdocument_to_dataclass(document["camera"], key, CameraRosMessageDAO)
        if "radar" in document:
            for key in document["radar"]:
                cls._convert_subdocument_to_dataclass(document["radar"], key, RadarRosMessageDAO)
        else:
            document["radar"] = {}
        if "camera_extra_info" in document and document["camera_extra_info"]:
            for key in document["camera_extra_info"]:
                cls._convert_subdocument_to_dataclass(document["camera_extra_info"], key, CameraExtraInfoRosMessageDAO)

        return cls._dataclass(**document)

    @classmethod
    def _convert_subdocument_to_dataclass(cls, document, key, DAO):
        if document.get(key, None):
            document[key] = utils.dao.convert_document_to_dataclass(document[key])


class OriginLidarDataDAO(RheaDAOBase):
    _collection = db.origin_lidar_data_collection
    _dataclass = OriginLidarData


class OriginCameraDataDAO(RheaDAOBase):
    _collection = db.origin_camera_data_collection
    _dataclass = OriginCameraData


class OriginRadarDataDAO(RheaDAOBase):
    _collection = db.origin_radar_data_collection
    _dataclass = OriginRadarData


class OriginUltrasonicRadarDataDAO(RheaDAOBase):
    _collection = db.origin_ultrasonic_radar_data_collection
    _dataclass = OriginUltrasonicRadarData


class OriginRawImuDataDAO(RheaDAOBase):
    _collection = db.origin_raw_imu_collection
    _dataclass = OriginRawImuData


class OriginCorrImuDataDAO(RheaDAOBase):
    _collection = db.origin_corr_imu_collection
    _dataclass = OriginCorrImuData


class OriginInsDataDAO(RheaDAOBase):
    _collection = db.origin_ins_collection
    _dataclass = OriginInsData


class OriginGpsDataDAO(RheaDAOBase):
    _collection = db.origin_ins_collection
    _dataclass = OriginGpsData


class OriginTagDataDAO(RheaDAOBase):
    _collection = db.origin_tag_collection
    _dataclass = OriginTagData


class OriginVehicleReportDataDAO(RheaDAOBase):
    _collection = db.origin_vehicle_report_collection
    _dataclass = OriginVehicleReportData
