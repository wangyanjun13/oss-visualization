from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import LaneLineTag, LanePreprocess


class LaneLineTagDAO(RheaDAOBase):
    _collection = db.lane_line_tag_collection
    _dataclass = LaneLineTag


class LanePreprocessDAO(RheaDAOBase):
    _collection = db.lane_preprocess_collection
    _dataclass = LanePreprocess
