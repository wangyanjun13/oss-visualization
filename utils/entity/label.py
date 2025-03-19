from dataclasses import dataclass
from typing import List, Optional


# TODO(chenlei): Label 的抽象基类
@dataclass
class ObstacleDetAutoLabel:
    # TODO(chenlei): 设计文档不完善
    _id: str
    dataset_id: str
    clip_id: str
    label_timestamp: str
    model_version: str
    frame_label_results: List = None
    obstacle_det_boxes_to_wbf_id: str = ""  # 用于兼容老数据的read_by_condition
    frame_label_results_oss_path: str = None  # 用于兼容frame_label_results超过16mb的情况，存储数据到oss


@dataclass
class ObstacleDetBoxesToWbf:
    _id: str
    dataset_id: str
    clip_id: str
    label_timestamp: str
    model_version: str
    det_tta_result_to_wbf_json_path: str


@dataclass
class CodaDetLabel:
    _id: str
    anno_task_id: str
    frame_label_results: dict


@dataclass
class ObstacleDetManuLabel:
    # TODO(chenlei): 设计文档不完善
    _id: str
    clip_id: str
    frame_label_results: dict


@dataclass
class ObstacleBmkManuLabel:
    # TODO(chenlei): 设计文档不完善
    _id: str
    clip_id: str
    frame_label_results: dict


@dataclass
class ObstacleTrackAutoLabel:
    _id: str
    dataset_id: str
    clip_id: str
    label_timestamp: str
    model_version: str
    frame_label_results: List = None
    frame_label_results_oss_path: str = None  # 用于兼容frame_label_results超过16mb的情况，存储数据到oss


@dataclass
class ObstacleTrackManuLabel(ObstacleTrackAutoLabel):
    _id: str
    clip_id: str
    frame_label_results: dict


@dataclass
class AvpParkSpaceManuLabel:
    # TODO(chenlei): 设计文档不完善
    pass


@dataclass
class AvpObstacleManuLabel:
    # TODO(chenlei): 设计文档不完善
    pass


@dataclass
class ObstacleManuLabelProject:
    _id: str
    project_name: str
    project_id: str
    # [no_notice, notified, fetched_back]
    status: str
    anno_task_ids: List[str]
    # anno_task_id: frame_num
    frame_nums: Optional[dict] = None
    rect_nums: Optional[dict] = None


@dataclass
class LaneLineManuLabelProject:
    _id: str
    requirement_id: str
    anno_task_ids: List[str]
    status: Optional[str] = "ready"
    title_name: Optional[str] = None
    laneline_type: Optional[str] = None


@dataclass
class ManuLabelProject:
    _id: str
    requirement_id: str
    project_id: str
    recycle_tag: str
    input_path: str
    anno_task_id: str
    status: Optional[str] = "ready"
    slice_interval: Optional[int] = 5
    title_name: Optional[str] = None
    task_type: Optional[str] = None
    import_label_root: Optional[str] = None
    export_label_root: Optional[str] = None
    recycle_path: Optional[str] = None
