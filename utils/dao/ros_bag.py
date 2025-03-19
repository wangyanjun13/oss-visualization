from utils.dao.base import db
from utils.dao.base import RheaDAOBase
from utils.entity import ParsedRosBag
from utils.entity import RosBag
from utils.entity import EventRosBag
from utils.entity import CategoriedParsedRosBag
from utils.entity import StatisticParsedRosBag


class RosBagDAO(RheaDAOBase):
    _collection = db.ros_bag_collection
    _dataclass = RosBag


class ParsedRosBagDAO(RheaDAOBase):
    _collection = db.parsed_ros_bag_collection
    _dataclass = ParsedRosBag


class EventRosBagDAO(RheaDAOBase):
    _collection = db.event_ros_bag_collection
    _dataclass = EventRosBag


class CategoriedParsedRosBagDAO(RheaDAOBase):
    _collection = db.categoried_parsed_ros_bag_collection
    _dataclass = CategoriedParsedRosBag


class StatisticParsedRosBagDAO(RheaDAOBase):
    _collection = db.statistic_parsed_ros_bag_collection
    _dataclass = StatisticParsedRosBag
