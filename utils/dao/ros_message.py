from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import Camera
from utils.entity import CorrImu
from utils.entity import Gps
from utils.entity import Ins
from utils.entity import Lidar
from utils.entity import Localization
from utils.entity import Odometry
from utils.entity import Radar
from utils.entity import RawImu
from utils.entity import Tag
from utils.entity import UltrasonicRadar
from utils.entity import UltrasonicRadarInfo
from utils.entity import VehicleReportCommon
from utils.entity import VehicleReportInfo
from utils.entity import CameraExtraInfo


class CameraRosMessageDAO(RheaDAOBase):
    _collection = db.camera_collection
    _dataclass = Camera


class CorrImuRosMessageDAO(RheaDAOBase):
    _collection = db.corr_imu_collection
    _dataclass = CorrImu


class RawImuRosMessageDAO(RheaDAOBase):
    _collection = db.raw_imu_collection
    _dataclass = RawImu


class GpsRosMessageDAO(RheaDAOBase):
    _collection = db.gps_collection
    _dataclass = Gps


class UltrasonicRadarDAO(RheaDAOBase):
    _collection = db.ultrasonic_radar_collection
    _dataclass = UltrasonicRadar

    @classmethod
    def convert_document_to_dataclass(cls, document):
        document["_id"] = str(document["_id"])
        document.pop("write_time", None)
        document.pop("update_time", None)
        document.pop("git_commit", None)
        document["ultra_radar_array"] = [UltrasonicRadarInfo(**info) for info in document["ultra_radar_array"]]
        return cls._dataclass(**document)


class InsRosMessageDAO(RheaDAOBase):
    _collection = db.ins_collection
    _dataclass = Ins

    @classmethod
    def convert_document_to_dataclass(cls, document):
        document["_id"] = str(document["_id"])
        document.pop("write_time", None)
        document.pop("update_time", None)
        document.pop("git_commit", None)
        document["localization"] = Localization(**document["localization"])
        return cls._dataclass(**document)


class LidarRosMessageDAO(RheaDAOBase):
    _collection = db.lidar_collection
    _dataclass = Lidar


class OdometryRosMessageDAO(RheaDAOBase):
    _collection = db.odometry_collection
    _dataclass = Odometry


class RadarRosMessageDAO(RheaDAOBase):
    _collection = db.radar_collection
    _dataclass = Radar


class TagRosMessageDAO(RheaDAOBase):
    _collection = db.tag_collection
    _dataclass = Tag


class VehicleReportCommonRosMessageDAO(RheaDAOBase):
    _collection = db.vehicle_report_common_collection
    _dataclass = VehicleReportCommon


class VehicleReportInfoRosMessageDAO(RheaDAOBase):
    _collection = db.vehicle_report_info_collection
    _dataclass = VehicleReportInfo


class CameraExtraInfoRosMessageDAO(RheaDAOBase):
    _collection = db.camera_extra_info_collection
    _dataclass = CameraExtraInfo


Camera_Ros = "CameraRos"
Corr_Imu_Ros = "CorrImuRos"
Gps_Ros = "GpsRos"
Ins_Ros = "InsRos"
Lidar_Ros = "LidarRos"
Radar_Ros = "RadarRos"
Raw_Imu_Ros = "RawImuRos"
Tag_Ros = "TagRos"
Ultrasonic_Radar_Ros = "UltrasonicRadarRos"
Vehicle_Report_Common_Ros = "VehicleReportCommonRos"
Vehicle_Report_Info_Ros = "VehicleReportInfoRos"
Camera_Extra_Info_Ros = "CameraExtraInfoRos"

ROS_DAO_DICT = {
    Camera_Ros: CameraRosMessageDAO,
    Corr_Imu_Ros: CorrImuRosMessageDAO,
    Gps_Ros: GpsRosMessageDAO,
    Ins_Ros: InsRosMessageDAO,
    Lidar_Ros: LidarRosMessageDAO,
    Radar_Ros: RadarRosMessageDAO,
    Raw_Imu_Ros: RawImuRosMessageDAO,
    Tag_Ros: TagRosMessageDAO,
    Ultrasonic_Radar_Ros: UltrasonicRadarDAO,
    Vehicle_Report_Common_Ros: VehicleReportCommonRosMessageDAO,
    Vehicle_Report_Info_Ros: VehicleReportInfoRosMessageDAO,
    Camera_Extra_Info_Ros: CameraExtraInfoRosMessageDAO,
}

ROS_MESSAGE_DICT = {
    Camera_Ros: Camera,
    Corr_Imu_Ros: CorrImu,
    Ins_Ros: Ins,
    Lidar_Ros: Lidar,
    Radar_Ros: Radar,
    Raw_Imu_Ros: RawImu,
    Tag_Ros: Tag,
    Ultrasonic_Radar_Ros: UltrasonicRadar,
    Vehicle_Report_Common_Ros: VehicleReportCommon,
    Vehicle_Report_Info_Ros: VehicleReportInfo,
    Camera_Extra_Info_Ros: CameraExtraInfo,
}
