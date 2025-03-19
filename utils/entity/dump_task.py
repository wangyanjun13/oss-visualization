from dataclasses import dataclass
from typing import List, Optional, Dict

@dataclass
class DumpTask:
    _id: str
    dump_task_type: str # labeled_xxx, source
    source_path: str
    save_path: str
    parsed_rosbag_id: str
    dump_status: str
    desensitization_status: str
    
    # 3d labeled 任务 tar 路径
    tar_s3_path: Optional[int] = None
    # 2d labeled 送标路径
    import_label_root: Optional[int] = None
    
    all_frame_count: Optional[int] = None
    
    # ======= labeled_xxx ===== 才有
    anno_task_id: Optional[str] = None
    save_batch_info: Optional[Dict] = None
    
    dump_use_time: Optional[Dict] = None
    desensitization_use_time: Optional[int] = None
    


    