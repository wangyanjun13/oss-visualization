from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import PredictionTask, PredictionMergeTask


class PredictionTaskDAO(RheaDAOBase):
    _collection = db.prediction_task_collection
    _dataclass = PredictionTask


class PredictionMergeTaskDAO(RheaDAOBase):
    _collection = db.prediction_merge_task_collection
    _dataclass = PredictionMergeTask
