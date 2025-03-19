from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class RoadTestUploadRes:
    _id: str
    ppl_bag_name: str
    car_id: str
    collect_date: str
    tar_bag_path: str
    ppl_bag_path: str
    status: str
