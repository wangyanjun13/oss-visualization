from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import DumpTask


class DumpTaskDAO(RheaDAOBase):
    _collection = db.dump_task_collection
    _dataclass = DumpTask

