from typing import List, Optional

from pydantic import BaseModel


class SensorsRequest(BaseModel):
    s3_paths: List[str]


class SensorReouces(BaseModel):
    s3_path: str = ""
    sensors: Optional[List[str]]
    is_project: bool = False
    view: str = ""

class SensorResourcesRequest(BaseModel):
    s3_paths: List[SensorReouces]
    