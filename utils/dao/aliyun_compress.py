from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from entity import aliyun_compress


class AliyunCompressDAO(RheaDAOBase):
    _collection = db.aliyun_compress_task_collection
    _dataclass = aliyun_compress
