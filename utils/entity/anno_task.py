from dataclasses import dataclass
from typing import Dict, List, Optional, Any


@dataclass
class AnnoTask:
    _id: str
    anno_task_type: str
    anno_platform: str
    anno_platform_task_id: str
    dataset_id: str
    anno_frames: List[Dict[str, List]]
    key_frame_interval: int
    oss_output_path: str
    # exported ， labeling， fetched
    status: str
    buried_points_result: Optional[Dict] = None
    # batchs, requirement_id
    to_label_info: Optional[Dict] = None
    to_label_retry_times: Optional[int] = None
    # 以下两个为3dbmk类型设计，scene_sections为按tag切分的片段，一个AnnoTask对应一个tag，json_details为最后生成的json文件与相对于的片段
    scene_sections: Dict = None
    json_details: Dict[str, List] = None
    # 回收s3 path
    labeled_path: Optional[str] = None
    # 生成新的nori同步到brain++ path
    save_bpp_path: Optional[str] = None
    # 生成新的nori同步到brain++的状态, ready, success
    bpp_status: Optional[str] = None
    # 车道线3d转2d的process类型
    cdx_3d_2d_process_type: Optional[str] = None
    trigger_cdx_2d_tolabel: Optional[bool] = None

@dataclass
class AnnoBatch:
    _id: str
    batch_id: str
    job_name: str
    tar_s3_path: str
    frame_num: int
    anno_task_id: str
    status: str # labeling、fetched、fetch_failed
    
    final_save_path_pass: str = None
    final_save_path_nopass: str = None
    fetched_task_ids: Optional[List] = None
    file_name: Optional[str] = None
    labeled_path: Optional[str] = None
    # 生成新的nori同步到brain++ path
    save_bpp_path: Optional[str] = None
    # 生成新的nori同步到brain++的状态, ready, success
    bpp_status: Optional[str] = None
    # 回收时间
    fetched_time: Optional[Any] = None
    pass_len: Optional[int] = None
    nopass_len: Optional[int] = None
    requirement_id: Optional[str] = None
    # yunhuii_info 2024.5.9 适配之前的旧数据
    yunhui_info: Optional[Any] = None
