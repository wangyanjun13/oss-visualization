from dataclasses import dataclass
from typing import Dict, List, Optional

# 回灌
@dataclass
class RechargeBatch:
    _id: str
    # 原始batch信息
    src_project_id: int # 原始项目id
    src_batch_id: int # 原始批次id
    s3_tar_path: str # 原始tar包路径
    src_batch_name: str
    src_prelabel_json_path: str # 原始预标文件路径
    # 回灌batch信息
    dst_project_id: int # 回灌项目id
    dst_batch_id: int # 回灌批次id
    s3_batch_json_path: str # 回灌批次文件
    dst_batch_name: str # 回灌批次名
    dst_data_json_path: str
    # meta信息
    upload_rect_nums: int # 回灌框数
    delete_rect_nums: int # 去除的框数, 超roi范围
    upload_frame_nums: int # 回灌帧数
    delete_frame_nums: int # 去除的帧数, 跳过帧数量
    skip_task_ids: Optional[List[int]] = None  # 跳过任务ids
    upload_task_info:  Optional[Dict] = None