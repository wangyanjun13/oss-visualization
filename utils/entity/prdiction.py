from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class PredictionTask:
    _id: str
    anno_task_id: str
    path_pattern: str
    prelabel_dir: str
    scenario_dir: str
    tensor_dir: str
    dataset_dir: str
    map_dataset_dir: str
    centerline_dataset_dir: str
    final_dataset_dir: str
    date_keys: List


@dataclass
class PredictionMergeTask:
    _id: str
    path_pattern: str
    prelabel_dir: str
    scenario_dir: str
    tensor_dir: str
    dataset_dir: str
    map_dataset_dir: str
    centerline_dataset_dir: str
    final_dataset_dir: str
    final_dataset_path: str
    date_keys: List
