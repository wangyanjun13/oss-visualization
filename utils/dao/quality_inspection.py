from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import QualityInspectionRes


class QualityInspectionResDAO(RheaDAOBase):
    _collection = db.quality_inspection_res_collection
    _dataclass = QualityInspectionRes
