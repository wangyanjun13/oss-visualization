from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import RoadTestUploadRes


class RoadTestUploadDAO(RheaDAOBase):
    _collection = db.road_test_upload_res_collection
    _dataclass = RoadTestUploadRes
