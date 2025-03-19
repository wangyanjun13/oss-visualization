from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import AnnoTask
from utils.entity import AnnoBatch


class AnnoTaskDAO(RheaDAOBase):
    _collection = db.anno_task_collection
    _dataclass = AnnoTask


class AnnoBatchDAO(RheaDAOBase):
    _collection = db.anno_batch_collection
    _dataclass = AnnoBatch