from dataclasses import dataclass
from typing import Dict, List

from utils.dao.base import db
from utils.dao.base import RheaDAOBase


@dataclass
class LightSignSampleFrame:
    _id: str
    project_id: str
    sample_date: str
    tag: str
    ppl_bag: str
    origin_nori_path: str
    csv_path: str
    frames: List[Dict]
    car_id: str
    car_args: dict
    to_label: bool
    

@dataclass
class LightSignToLabel:
    _id: str
    project_id: str
    project_type: str
    ori_nori_id: str
    ori_nori_path: str
    ppl_root: str  
    csv_path: str
    frame_id: str
    nori_id: str
    nori_path: str
    batch_id: int
    batch_name: str


@dataclass
class LightSignToRecycle:
    _id: str
    nori_key: str
    nori_path: str
    project_id: str 
    ori_nori_key: str
    ori_nori_path: str 
    image_path: str 
    frame_id: str 
    labels: list
    unit_id: str 
    batch_id: str 
    image_width: int 
    image_height: int 
    recycle_path: str
    mask_path: str 
    ramp_path: str 
    ramp_neg_path: str 
    instruction_path: str 
    warning_path: str 
    forbidden_path: str
    insert_nori_path: str


@dataclass
class SignClassToLabel:
    _id: str
    nori_key: str
    nori_path: str
    image_path: str
    insert_nori_key: str  
    insert_nori_path: str
    sign_type: str 
    gtbox: str
    project_id: str 
    project_type: str
    batch_id: int 
    batch_name: str
    file_path: str
    image_height: int 
    image_width: int 
    

@dataclass
class SignClassToRecycle:
    _id: str 
    nori_key: str 
    extra: dict 
    image_height: int 
    image_width: int 
    traffic_signs: list 
    recycle_path: str 
    

class LightSignSampleFrameDAO(RheaDAOBase):
    _collection = db.light_sign_sample_frame
    _dataclass = LightSignSampleFrame

    @classmethod
    def update_one(cls, filter_condition, update_fields):
        cls._collection.update_one(filter_condition, {"$set": update_fields}, upsert=True)


class LightSignToLabelDAO(RheaDAOBase):
    _collection = db.light_sign_to_label
    _dataclass = LightSignToLabel

    @classmethod
    def update_one(cls, filter_condition, update_fields):
        cls._collection.update_one(filter_condition, {"$set": update_fields}, upsert=True)


class LightSignToRecycleDAO(RheaDAOBase):
    _collection = db.light_sign_to_recycle
    _dataclass = LightSignToRecycle 
    
    @classmethod
    def update_one(cls, filter_condition, update_fields):
        cls._collection.update_one(filter_condition, {"$set": update_fields}, upsert=True)


class SignClassToLabelDAO(RheaDAOBase):
    _collection = db.sign_class_to_label 
    _dataclass = SignClassToLabel
    @classmethod
    def update_one(cls, filter_condition, update_fields):
        cls._collection.update_one(filter_condition, {"$set": update_fields}, upsert=True)



class SignClassToRecycleDAO(RheaDAOBase):
    _collection = db.sign_class_to_recycle 
    _dataclass = SignClassToRecycle

    @classmethod
    def update_one(cls, filter_condition, update_fields):
        cls._collection.update_one(filter_condition, {"$set": update_fields}, upsert=True)
