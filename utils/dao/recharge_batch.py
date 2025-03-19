from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import RechargeBatch

class RechargeBatchDAO(RheaDAOBase):
    _collection = db.recharge_batch_collection
    _dataclass = RechargeBatch
