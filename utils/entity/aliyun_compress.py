from dataclasses import dataclass
from typing import Dict, List, Union, Optional


@dataclass
class aliyun_compress:
    _id: str
    car_id: str
    ppl_bag: str
    status: str
    all_ppl_bag_json: Optional[List] = None
