from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import AvpObstacleManuLabel
from utils.entity import AvpParkSpaceManuLabel
from utils.entity import ObstacleDetAutoLabel
from utils.entity import ObstacleDetManuLabel
from utils.entity import ObstacleBmkManuLabel
from utils.entity import ObstacleTrackAutoLabel
from utils.entity import ObstacleTrackManuLabel
from utils.entity import ObstacleManuLabelProject
from utils.entity import CodaDetLabel
from utils.entity import ObstacleDetBoxesToWbf
from utils.entity import ManuLabelProject
from utils.entity import LaneLineManuLabelProject


class AvpObstacleManuLabelDAO(RheaDAOBase):
    _collection = db.avp_obstacle_manu_label_collection
    _dataclass = AvpObstacleManuLabel


class AvpParkSpaceManuLabelDAO(RheaDAOBase):
    _collection = db.avp_park_space_manu_label_collection
    _dataclass = AvpParkSpaceManuLabel


class ObstacleDetAutoLabelDAO(RheaDAOBase):
    _collection = db.obstacle_det_auto_label_collection
    _dataclass = ObstacleDetAutoLabel


class ObstacleDetManuLabelDAO(RheaDAOBase):
    _collection = db.obstacle_det_manu_label_collection
    _dataclass = ObstacleDetManuLabel


class ObstacleDetBoxesToWbfDAO(RheaDAOBase):
    _collection = db.obstacle_det_boxes_to_wbf_collection
    _dataclass = ObstacleDetBoxesToWbf


class ObstacleBmkManuLabelDAO(RheaDAOBase):
    _collection = db.obstacle_det_manu_label_collection
    _dataclass = ObstacleBmkManuLabel


class ObstacleTrackAutoLabelDAO(RheaDAOBase):
    _collection = db.obstacle_track_auto_label_collection
    _dataclass = ObstacleTrackAutoLabel


class ObstacleTrackManuLabelDAO(RheaDAOBase):
    _collection = db.obstacle_track_manu_label_collection
    _dataclass = ObstacleTrackManuLabel


class CodaDetLabelDAO(RheaDAOBase):
    _collection = db.coda_det_label_collection
    _dataclass = CodaDetLabel


config = {
    "ObstacleDet": {"Auto": ObstacleDetAutoLabelDAO, "Manual": ObstacleDetManuLabelDAO},
    "ObstacleTrack": {"Auto": ObstacleTrackAutoLabelDAO, "Manual": ObstacleTrackManuLabelDAO},
    "AvpObstacle": {"Manual": AvpObstacleManuLabelDAO},
    "AvpParkSpace": {"Manual": AvpParkSpaceManuLabelDAO},
}


class LabelDAOFactory:
    @staticmethod
    def get(anno_task_type: str, label_type="Auto"):
        if anno_task_type not in config:
            return KeyError()
        return config[anno_task_type][label_type]


class ObstacleManuLabelProjectDao(RheaDAOBase):
    _collection = db.obstacle_manu_label_project_collection
    _dataclass = ObstacleManuLabelProject

class ManuLabelProjectDAO(RheaDAOBase):
    _collection = db.manu_label_project_collection
    _dataclass = ManuLabelProject

class LaneLineManuLabelProjectDAO(RheaDAOBase):
    _collection = db.laneline_manu_label_project_collection
    _dataclass = LaneLineManuLabelProject