import requests
from json import dumps
from re import sub
import numpy as np 
from PIL import Image
from io import BytesIO
from loguru import logger


def get_aliyun_pcd(nori_id,nori_path):
    request_body = {
        "resource_nori": dumps({
            "fuser_lidar": {
                "nori_id": nori_id,
                "oss_path": f'broadside-transform/dev/nova/oss_visualization/{nori_id}.pcd',
            },
            "sensor":[]
        }),
        "parse_path": sub("all_nori/fuser_lidar.nori","",nori_path)
    }

    resp = requests.post(
        url="http://10.236.242.11:9999/obstacle_vis",
        json=request_body
    ).json()
    if resp["code"]==200:
        return True
    else:
        return False

def get_aliyun_jpg(sensor,nori_path):
    request_body = {
        "resource_nori": dumps({
            "fuser_lidar": {},
            "sensor":sensor
        }),
        "parse_path": sub("all_nori/image.nori","",nori_path)
    }
    # print(request_body)

    resp = requests.post(
        url="http://10.236.242.11:9999/obstacle_vis",
        json=request_body
    ).json()
    if resp["code"]==200:
        return True
    else:
        return False

def conver_oss_to_url(oss_path):
    resp = requests.post(
        url="http://10.236.242.11:9999/convert_oss_to_url",
        json={
            "oss_path": oss_path
        }
    ).json()
    oss_url=resp["data"].get("oss_url", "")
    return oss_url

def convert_oss_to_url_multi(sensors):
    resp = requests.post(
        # url="http://10.236.242.11:9999/convert_oss_to_url_multi",
        url="http://10.236.246.210:9999/convert_oss_to_url_multi", # test
        json={
            "sensors": sensors
        }
    ).json()
    return resp["data"]["sensors"]

def get_aliyun_sensor(request_body):
    resp = requests.post(
        # url="http://10.236.242.11:9999/get_sensor_data",
        url="http://10.236.246.210:9999/get_sensor_data", # test 
        json=request_body
    )
    try:
        json_data = resp.json()
        if json_data["code"]==200:
            return True
    except Exception as e:
        logger.error(f"error: {repr(e)}")
        return False 
    