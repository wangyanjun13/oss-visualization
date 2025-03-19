from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import jiaofu


class JiaoFuDAO(RheaDAOBase):
    _collection = db.jiaofu_task_collection
    _dataclass = jiaofu
