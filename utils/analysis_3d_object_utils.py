import json
from refile import smart_open
from json import loads,dumps
import refile
from tqdm import tqdm
import os 
import traceback
from datetime import datetime
import sys 

from factory import insert_many_data,update_one_data
from pickle import load
import pickle
import torch
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from utils.pcd_aliyun_utils import get_aliyun_pcd,get_aliyun_jpg,conver_oss_to_url, \
    get_aliyun_other_lidar, get_aliyun_radar, get_aliyun_sensor
# import oss2
import os
import traceback
import nori2
import cv2
from utils.pcd_writer_utils import to_pcd, to_pcd_nori_reader
import requests
import numpy as np 
from PIL import Image
from io import BytesIO
from loguru import logger

from config import IMAGE_DIR, PCD_DIR, RADAR_DIR, BASE_URL

oss_ak = ''
oss_sk = ''
oss_endpoint = os.getenv('oss_endpoint', 'https://oss-cn-beijing.aliyuncs.com')
GET_PCD_URI = "api/v1/pcds/get_3d_pcd"
GET_RADAR_URI = "api/v1/radars/get_3d_radar"
GET_IMAGE_URI = "api/v1/images/get_3d_image"
ALIYUN_DATA_ROOT = "broadside-transform/dev/nova/oss_visualization"


def get_radar_data(nori_id, nori_path, save_path):
    try:
        # with nori2.open(nori_path) as nr:
            # byte_data = nr.get(nori_id)
        try:
            byte_data = nori2.Fetcher().get(nori_id,retry=3)
        except:
            with nori2.open(nori_path) as nr:
                byte_data = nr.get(nori_id)
                
        radar_info = pickle.loads(byte_data)
        objects = radar_info["objects"]
        with refile.smart_open(save_path, "w") as f:
            f.write(json.dumps(objects))
        return True
    except Exception as e:
        logger.error(f"get radar error: {repr(e)}")
        return False


def get_img_shape(oss_url):
    try:
        resp=requests.get(url=oss_url)
        image = Image.open(BytesIO(resp.content))
        image_array = np.array(image)
        height, width = image_array.shape[:2]
        return height,width
    except:
        return None, None

def image_undistort(img, cam_intrin):
    h, w = img.shape[:2]
    k = np.array(cam_intrin['K']).reshape((3, 3))
    d = np.array(cam_intrin['D'])
    mode = cam_intrin['distortion_model']
    if mode == 'pinhole':
        mapx, mapy = cv2.initUndistortRectifyMap(k, d, None, k, (w, h), 5)
    elif mode == 'fisheye':
        mapx, mapy = cv2.fisheye.initUndistortRectifyMap(k, d, None, k, (w, h), 5)
    return cv2.remap(img, mapx, mapy, cv2.INTER_LINEAR)


def nori_path_and_nori_id_get_img(nori_path: str, nori_id: str,save_path,cam_intrin):
    try:
        nori_data = nori2.open(nori_path)
        byte_img = nori_data.get(nori_id)
        byte_img=image_undistort(byte_img,cam_intrin)
    except:
        return None
    with refile.smart_open(save_path, "wb") as f:
        f.write(byte_img)
    return save_path

def generate_pcd_img(k,v,file_path,trans_matrix,intrinsic):
    if k=="fuser_lidar":
        nori_id=v.get("nori_id")
        nori_path=v.get("nori_path")
        save_path=f"{PCD_DIR}/{nori_id}.pcd"
        if not refile.smart_exists(save_path):
            result=to_pcd(nori_id=nori_id,nori_path=nori_path,save_path=save_path)
            if result is not None:
                oss_path=f"{BASE_URL}/api/v1/pcds/get_3d_pcd/{nori_id}"
                update_one_data("3d_data_source", {"source_path":file_path}, {"source_path":file_path,"data_source":"oss"},True)   
                return {"oss_path":oss_path,"ext":"pcd","meta":None,"size":{},"oss_url":oss_path+".pcd", "key": k}
            else:
                # flag=get_aliyun_pcd(nori_id,nori_path)
                flag=get_aliyun_other_lidar(nori_id,k,nori_path)
                if flag is True:
                    oss_url=conver_oss_to_url(f"oss://broadside-transform/dev/nova/oss_visualization/{nori_id}.pcd")
                    return {"oss_path":oss_url,"ext":"pcd","meta":None,"size":{},"oss_url":oss_url, "key": k}
                else:
                    oss_url=None
                    return {"oss_path":oss_url,"ext":"pcd","meta":None,"size":{},"oss_url":oss_url, "key": k}
                update_one_data("3d_data_source", {"source_path":file_path}, {"source_path":file_path,"data_source":"aliyun"},True)
            
        else:
            oss_url=f"{BASE_URL}/api/v1/pcds/get_3d_pcd/{nori_id}.pcd"
            oss_path=f"{BASE_URL}/api/v1/pcds/get_3d_pcd/{nori_id}"
            return {"oss_path":oss_path,"ext":"pcd","meta":None,"size":{},"oss_url":oss_url, "key": k}
                    
    elif 'cam_' in k and k!="cam_extra_info":
        nori_id=v.get("nori_id")
        nori_path=v["nori_path"]
        timestamp=v["timestamp"]
        camera=k
        # img_path = f"/data/oss_visualization_fastapi/static/3d_images/{nori_id}.jpg"
        img_path = f"{IMAGE_DIR}/{nori_id}.jpg"
        oss_path=f"{BASE_URL}/api/v1/images/get_3d_image/{nori_id}"
        # if not os.path.exists(img_path):
        if not refile.smart_exists(img_path):
            try:
                ori_img = nori2.Fetcher().get(nori_id,retry=3)
                # with nori2.open(nori_path) as nr:
                #     ori_img = nr.get(nori_id)
                ns = np.frombuffer(ori_img, dtype=np.uint8)
                img = cv2.imdecode(ns, cv2.IMREAD_COLOR)
                byte_img=image_undistort(img,intrinsic.get(k))
                
                if byte_img is not None:
                    # with open(img_path, "wb") as f:
                    _, img_bytes = cv2.imencode('.jpg', byte_img)

                    # 将字节流写入文件
                    with smart_open(img_path, "wb") as f:
                        f.write(img_bytes)
                        
                    # imwrite(img_path, byte_img)
                    # # img = cv2.imread(img_path)
                    # image = Image.open(BytesIO(byte_img))
                    # 将 PIL 图像转换为 numpy 数组
                    # img = np.array(image)
                    img = byte_img
                    if img is None:
                        logger.error(f"{file_path}读取图片失败")
                        # raise FileNotFoundError("Failed to read the image.")
                        return {"oss_path":oss_path,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_path+".jpg","size":{}}
                    else:
                        h, w, g = img.shape
                        return {"oss_path":oss_path,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_path+".jpg","size":{"height": h, "width": w}}

            except:
                img_path=nori_path_and_nori_id_get_img(nori_path,nori_id,img_path,intrinsic.get(k))
                if img_path is None:
                    sensor=[{"nori_id":nori_id,"oss_path":f"broadside-transform/dev/nova/oss_visualization/{nori_id}.jpg","ext":"jpg","camera":camera,"intrinsic":intrinsic.get(k)}]
                    flag=get_aliyun_jpg(sensor,nori_path)
                    if flag is True:
                        oss_url=conver_oss_to_url(f"oss://broadside-transform/dev/nova/oss_visualization/{nori_id}.jpg")
                        # print(oss_url)
                        if oss_url is not None:
                            h,w=get_img_shape(oss_url)
                            return {"oss_path":oss_url,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_url,"size":{"height": h, "width": w}}
                        else:
                            return {"oss_path":None,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":None,"size":{}}
                    else:
                        oss_url=None
                        return {"oss_path":oss_url,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_url,"size":{}}
                else:
                    with refile.smart_open(img_path, "rb") as f:
                        byte_img = f.read()
                    image = Image.open(BytesIO(byte_img))
                    # 将 PIL 图像转换为 numpy 数组
                    img = np.array(image)
                    if img is None:
                        logger.error(f"{file_path}读取图片失败")
                        # raise FileNotFoundError("Failed to read the image.")
                        return {"oss_path":oss_path,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_path+".jpg","size":{}}
                    else:
                        h, w, g = img.shape
                        return {"oss_path":oss_path,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_path+".jpg","size":{"height": h, "width": w}}
        else:
            # img = cv2.imread(img_path)
            with refile.smart_open(img_path, "rb") as f:
                byte_img = f.read()
            image = Image.open(BytesIO(byte_img))
            # 将 PIL 图像转换为 numpy 数组
            img = np.array(image)
            # img = imread(img_path)
            if img is None:
                logger.error(f"{file_path}读取图片失败")
                # raise FileNotFoundError("Failed to read the image.")
                return {"oss_path":oss_path,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_path+".jpg","size":{}}
            else:
                h, w, g = img.shape
                return {"oss_path":oss_path,"ext":"jpg","meta":{camera:{"timeStamp":timestamp,"transMatrix":trans_matrix.get(k)}},"oss_url":oss_path+".jpg","size":{"height": h, "width": w}}

    elif "radar" in k:
        nori_id=v.get("nori_id")
        nori_path=v.get("nori_path")
        save_path=f"{RADAR_DIR}/{nori_id}.json"
        if not refile.smart_exists(save_path):
            result = get_radar_data(nori_id, nori_path, save_path)
            if result:
                oss_path=f"{BASE_URL}/api/v1/radars/get_3d_radar/{nori_id}"
                update_one_data("3d_data_source", {"source_path":file_path}, {"source_path":file_path,"data_source":"oss"},True)   
                return {"oss_path":oss_path,"ext":"json","meta":None,"size":{},"oss_url":oss_path+".json","key": k}
            else:
                flag=get_aliyun_radar(nori_id,k,nori_path)
                if flag is True:
                    oss_url=conver_oss_to_url(f"oss://broadside-transform/dev/nova/oss_visualization/{nori_id}.json")
                    resp = requests.get(url=oss_url)
                    data = resp.content.decode()
                    return {"oss_path":oss_url,"ext":"json","meta":None,"size":{},"oss_url":oss_url,"key": k, "data": data}
                else:
                    oss_url=None
                    return {"oss_path":oss_url,"ext":"json","meta":None,"size":{},"oss_url":oss_url,"key": k, "data": None}
        else:
            oss_url=f"{BASE_URL}/api/v1/radars/get_3d_radar/{nori_id}.json"
            oss_path=f"{BASE_URL}/api/v1/radars/get_3d_radar/{nori_id}"
            with refile.smart_open(save_path) as f:
                data = json.loads(f.read())
            return {"oss_path":oss_path,"ext":"json","meta":None,"size":{},"oss_url":oss_url, "data": data, "key": k}
    
    elif k != "fuser_lidar" and "lidar" in k:
        nori_id=v.get("nori_id")
        nori_path=v.get("nori_path")
        save_path=f"{PCD_DIR}/{nori_id}.pcd"
        if not refile.smart_exists(save_path):
            result=to_pcd(nori_id=nori_id,nori_path=nori_path,save_path=save_path)
            if result is not None:
                oss_path=f"{BASE_URL}/api/v1/pcds/get_3d_pcd/{nori_id}"
                update_one_data("3d_data_source", {"source_path":file_path}, {"source_path":file_path,"data_source":"oss"},True)   
                return {"oss_path":oss_path,"ext":"pcd","meta":None,"size":{},"oss_url":oss_path+".pcd","key": k}
            else:
                flag=get_aliyun_other_lidar(nori_id,k,nori_path)
                if flag is True:
                    oss_url=conver_oss_to_url(f"oss://broadside-transform/dev/nova/oss_visualization/{nori_id}.pcd")
                    return {"oss_path":oss_url,"ext":"pcd","meta":None,"size":{},"oss_url":oss_url,"key": k}
                else:
                    oss_url=None
                    return {"oss_path":oss_url,"ext":"pcd","meta":None,"size":{},"oss_url":oss_url,"key": k}
        else:
            oss_url=f"{BASE_URL}/api/v1/pcds/get_3d_pcd/{nori_id}.pcd"
            oss_path=f"{BASE_URL}/api/v1/pcds/get_3d_pcd/{nori_id}"
            return {"oss_path":oss_path,"ext":"pcd","meta":None,"size":{},"oss_url":oss_url, "key": k}

def get_frame_info(_id, frames,file_path,trans_matrix,calibrated_sensors,intrinsic):
    all_resources = generate_sensor_data(frames, trans_matrix, intrinsic)
    print(f"len all_resources: {len(all_resources)}")
    for frame in tqdm(frames, desc="insert frame"):
        try:
            labels=frame["labels"]
        except:
            labels=None
        frame_id=frame["frame_id"]
        ins_data=frame["ins_data"]
        origin_frame_id=frame["origin_frame_id"]
        preds=frame.get("preds")
        if preds is None:
            preds=[]
        pre_labels=frame["pre_labels"]            
        roi_info=frame.get("roi_info",{})
        clip_id = frame["clip_id"] + "_" +str(frame["frame_id"])

        resources = all_resources[clip_id]
        result_dict = {"id": _id,"source_path":file_path,"origin_frame_id":origin_frame_id,"frame_id":frame_id,"data":{"labels":labels,"pre_labels":pre_labels,"preds":preds},"resources":resources,"ins_data":ins_data,"roi_info":roi_info,"calibrated_sensors":calibrated_sensors}
        update_one_data("3d_object", {"id":_id,"source_path":file_path}, result_dict,True)
        _id += 1
    
    return _id
    
def generate_sensor_data(frames, trans_matrix, intrinsic):
    def contains_any(key, substrings):
        return any(substring in key for substring in substrings)
    
    def f_data(oss_url, key, ext, data=None):
        if data:
            return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key,"data": data}
        else:
            return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key}
    
    def convert_url(sensor, sensor_type):
        oss_url = conver_oss_to_url(sensor["oss_path"])
        sensor["oss_url"] = oss_url
        if sensor_type == "radar":
            try:
                data = json.loads(requests.get(url=oss_url).content.decode())
                sensor["data"] = data
            except:
                sensor["data"] = []
    
    all_resources = {}
    sensor_data_format = {}
    # roi_keys = set([
    #     'fuser_lidar', 'front_lidar', 'left_lidar', 'right_lidar', 'back_lidar', 'front_2_lidar', 
    #     'cam_front_left_100', 'cam_back_right_100', 'cam_back_left_100', 'cam_back_100', 'cam_front_right_100', 'cam_front_120',
    #     'radar0', 'radar1', 'radar2', 'radar3', 'radar4', 
    # ])
    print(f"len frames: {len(frames)}")
    for frame in frames:
        clip_id = frame["clip_id"] + "_" +str(frame["frame_id"])
        all_resources[clip_id] = []
        sensor_data = frame["sensor_data"]
        for key, value in sensor_data.items():
            if key == "cam_extra_info":
                continue 
            if not contains_any(key, ["lidar", "radar", "cam_"]):
                continue 
            # if key not in roi_keys:
            #     continue 
            nori_id = value.get("nori_id")
            if not nori_id:
                continue 
            nori_path = value["nori_path"]
            timestamp = value["timestamp"]
            if "lidar" in key:
                sensor_data_format.setdefault(nori_path, []).append({
                    "key": key,
                    "clip_id": clip_id,
                    "nori_id": nori_id,
                    "timestamp": timestamp,
                    "oss_path": refile.smart_path_join(PCD_DIR, nori_id+".pcd")
                })
            elif "radar" in key:
                sensor_data_format.setdefault(nori_path, []).append({
                    "key": key,
                    "clip_id": clip_id,
                    "nori_id": nori_id,
                    "timestamp": timestamp,
                    "oss_path": refile.smart_path_join(RADAR_DIR, nori_id+".json")
                })
            elif 'cam_' in key and key != "cam_extra_info":
                sensor_data_format.setdefault(nori_path, []).append({
                    "key": key,
                    "clip_id": clip_id,
                    "nori_id": nori_id,
                    "timestamp": timestamp,
                    "oss_path": refile.smart_path_join(IMAGE_DIR, nori_id+".jpg")
                })
        
    for nori_path, nori_info in tqdm(sensor_data_format.items(), desc="process sensor"):
        logger.info(f"nori_path: {nori_path}")
        try: 
            refile.smart_exists(nori_path)
        except Exception as e:
            logger.error(f"nori_path: {nori_path}, error: {repr(e)}")
            lidar_sensors = {}
            radar_sensors = {}
            image_sensors = {}
            for info in nori_info:
                sensor_type = "" 
                key = info["key"]
                if "lidar" in key:
                    sensor_type = "lidar"
                    info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".pcd")
                    lidar_sensors.setdefault("lidar", []).append(info)
                elif "radar" in key:
                    sensor_type = "radar"
                    info["oss_path"] =  os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".json")
                    lidar_sensors.setdefault("radar", []).append(info)
                elif "cam_" in key:
                    sensor_type = "image"
                    info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".jpg")
                    info["intrinsic"] = intrinsic.get(info["key"])
                    image_sensors.setdefault("image", []).append(info)

            try:
                sensors_list = [lidar_sensors, radar_sensors, image_sensors]
                sensors_list = [item for item in sensors_list if item]
                for one_sensors in sensors_list:
                    sensor_type = list(one_sensors.keys())[0]
                    print(f"sensor_type: {sensor_type}")
                    sensors = one_sensors[sensor_type]
                    request_body = {
                        "sensor_type": sensor_type,
                        "sensors": sensors,
                        "nori_path": nori_path,
                    }
                    get_aliyun_sensor(request_body)
                    for sensor in tqdm(request_body["sensors"], desc="process aliyun nori"):
                        convert_url(sensor, sensor_type)
                    
                    for sensor in tqdm(request_body["sensors"], desc="process aliyun nori"):
                        clip_id = sensor["clip_id"]
                        # oss_url = conver_oss_to_url(sensor["oss_path"])
                        oss_url = sensor["oss_url"]
                        key = sensor["key"]
                        if sensor_type == "lidar":
                            resource = f_data(oss_url, key, "pcd")
                            all_resources[clip_id].append(resource)
                        elif sensor_type == "radar":
                            data = sensor["data"]
                            resource = f_data(oss_url, key, "json", data)
                            all_resources[clip_id].append(resource)
                        elif sensor_type == "image":
                            h, w=get_img_shape(oss_url)
                            resource = f_data(oss_url, key, "jpg")
                            resource["meta"] = {
                                key: {
                                    "timeStamp":timestamp,
                                    "transMatrix":trans_matrix.get(key)
                                }
                            }
                            resource["size"] = {"height": h, "width": w}
                            resource.pop("key")
                            
                            all_resources[clip_id].append(resource)
            except Exception as e:
                logger.error(f"aliyun error: {repr(e)}")
                traceback.print_exc()
            continue 
        if not refile.smart_exists(nori_path):
            lidar_sensors = {}
            radar_sensors = {}
            image_sensors = {}
            for info in nori_info:
                sensor_type = "" 
                key = info["key"]
                if "lidar" in key:
                    sensor_type = "lidar"
                    info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".pcd")
                    lidar_sensors.setdefault("lidar", []).append(info)
                elif "radar" in key:
                    sensor_type = "radar"
                    info["oss_path"] =  os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".json")
                    lidar_sensors.setdefault("radar", []).append(info)
                elif "cam_" in key:
                    sensor_type = "image"
                    info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".jpg")
                    info["intrinsic"] = intrinsic.get(info["key"])
                    image_sensors.setdefault("image", []).append(info)

            try:
                sensors_list = [lidar_sensors, radar_sensors, image_sensors]
                sensors_list = [item for item in sensors_list if item]
                for one_sensors in sensors_list:
                    sensor_type = list(one_sensors.keys())[0]
                    print(f"sensor_type: {sensor_type}")
                    sensors = one_sensors[sensor_type]
                    logger.info(f"sensors: {sensors}")
                    request_body = {
                        "sensor_type": sensor_type,
                        "sensors": sensors,
                        "nori_path": nori_path,
                    }
                    get_aliyun_sensor(request_body)
                    for sensor in tqdm(request_body["sensors"], desc="process aliyun nori"):
                        convert_url(sensor, sensor_type)
                    
                    for sensor in tqdm(request_body["sensors"], desc="process aliyun nori"):
                        clip_id = sensor["clip_id"]
                        # oss_url = conver_oss_to_url(sensor["oss_path"])
                        oss_url = sensor["oss_url"]
                        key = sensor["key"]
                        if sensor_type == "lidar":
                            resource = f_data(oss_url, key, "pcd")
                            all_resources[clip_id].append(resource)
                        elif sensor_type == "radar":
                            data = sensor["data"]
                            resource = f_data(oss_url, key, "json", data)
                            all_resources[clip_id].append(resource)
                        elif sensor_type == "image":
                            h, w=get_img_shape(oss_url)
                            resource = f_data(oss_url, key, "jpg")
                            resource["meta"] = {
                                key: {
                                    "timeStamp":timestamp,
                                    "transMatrix":trans_matrix.get(key)
                                }
                            }
                            resource["size"] = {"height": h, "width": w}
                            resource.pop("key")
                            
                            all_resources[clip_id].append(resource)
            except Exception as e:
                logger.error(f"aliyun error: {repr(e)}")
                traceback.print_exc()
            except Exception as e:
                logger.error(f"aliyun error: {repr(e)}")
        else:
            nr = nori2.open(nori_path)
            for info in tqdm(nori_info, desc="process nori_info"):
                nori_id = info["nori_id"]
                clip_id = info["clip_id"]
                timestamp = info["timestamp"]
                key = info["key"]
                oss_path = info["oss_path"]
                if refile.smart_exists(oss_path):
                    if oss_path.endswith(".pcd"):
                        oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                        resource = f_data(oss_url, key, "pcd")
                        all_resources[clip_id].append(resource)

                    elif oss_path.endswith(".json"):
                        oss_url=f"{BASE_URL}/{GET_RADAR_URI}/{nori_id}.json"
                        with refile.smart_open(oss_path) as f:
                            data = json.loads(f.read())
                        resource = f_data(oss_url, key, "json", data)                        
                        all_resources[clip_id].append(resource)
                        
                    elif oss_path.endswith(".jpg"):
                        with refile.smart_open(oss_path, "rb") as f:
                            image = Image.open(BytesIO(f.read()))
                            # 将 PIL 图像转换为 numpy 数组
                            img = np.array(image)
                            h, w, g = img.shape
                            oss_url = f"{BASE_URL}/{GET_IMAGE_URI}/{nori_id}.jpg"
                            resource = f_data(oss_url, key, "jpg")
                            resource["meta"] = {
                                key: {
                                    "timeStamp":timestamp,
                                    "transMatrix":trans_matrix.get(key)
                                }
                            }
                            resource["size"] = {"height": h, "width": w}
                            resource.pop("key")
                            
                            all_resources[clip_id].append(resource)

                else:
                    if oss_path.endswith(".pcd"):
                        pcd_flag = to_pcd_nori_reader(
                            nori_id=nori_id,
                            nr=nr,
                            save_path=oss_path)
                        if pcd_flag:
                            oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                            resource = f_data(oss_url, key, "pcd")
                            all_resources[clip_id].append(resource)
                        else:
                            oss_url = ""
                            resource = f_data(oss_url, key, "pcd")
                            all_resources[clip_id].append(resource)

                    elif oss_path.endswith(".json"):
                        byte_data = nr.get(nori_id)
                        radar_info = pickle.loads(byte_data)
                        objects = radar_info["objects"]
                        with refile.smart_open(oss_path, "w") as f:
                            f.write(json.dumps(objects))
                        oss_url=f"{BASE_URL}/{GET_RADAR_URI}/{nori_id}.json"
                        resource = f_data(oss_url, key, "json", objects)                        
                        all_resources[clip_id].append(resource)
                        
                    elif oss_path.endswith(".jpg"):
                        byte_data = nr.get(nori_id)
                        ns = np.frombuffer(byte_data, dtype=np.uint8)
                        img = cv2.imdecode(ns, cv2.IMREAD_COLOR)
                        img_array = image_undistort(img,intrinsic.get(key))
                        _, img_bytes = cv2.imencode('.jpg', img_array)
                        with smart_open(oss_path, "wb") as f:
                            f.write(img_bytes)
                        h, w, g = img.shape
                        oss_url = f"{BASE_URL}/{GET_IMAGE_URI}/{nori_id}.jpg"
                        resource = f_data(oss_url, key, "jpg")
                        resource["meta"] = {
                            key: {
                                "timeStamp":timestamp,
                                "transMatrix":trans_matrix.get(key)
                            }
                        }
                        resource["size"] = {"height": h, "width": w}
                        resource.pop("key")

                        
                        all_resources[clip_id].append(resource)
                       
            nr.close()

    return all_resources

def analysis_pkl_file(file_path):
    data=None
    update_one_data("3d_data_source", {"source_path":file_path}, {"status":"processing"},True)
    trans_matrix={}
    intrinsic={}
    obj=[]
    _id=1
    # cam_keys=["cam_front_left_120","cam_back_right_120","cam_back_left_120","cam_front_right_120","cam_front_120","cam_back_120"]
    try:
        with smart_open(file_path,"rb") as f:
            data=load(f)
    except Exception as e:
        update_one_data("3d_data_source", {"source_path":file_path}, {"status":"failed","message":f"{file_path}解析失败,原因{e}"})
    if data is not None:
        try:
            for item in tqdm(data, desc="process data"):
                result=[]
                frames=item["frames"]
                calibrated_sensors=item["calibrated_sensors"]
                for k,v in calibrated_sensors.items():
                    if "cam_" in k:
                        trans_matrix[k]=[ i for ll in v["T_lidar_to_pixel"] for i in ll]
                        intrinsic[k]=v["intrinsic"]
                        
                get_frame_info(_id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic)
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"success","message":"解析数据成功"})
        except Exception as e:
            logger.info(f"error: {repr(e)}")
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"failed","message":f"解析失败 {str(e)}"})


def analysis_json_file(file_path):
    data=None
    update_one_data("3d_data_source", {"source_path":file_path}, {"status":"processing"},True)
    trans_matrix={}
    intrinsic={}
    _id=1
    result=[]
    try:
        with smart_open(file_path,"r") as f:
            data=loads(f.read())
    except Exception as e:
        update_one_data("3d_data_source", {"source_path":file_path}, {"status":"failed","message":f"{file_path}解析失败,原因{e}"})
    if data is not None:
        try:
            frames=data["frames"]
            calibrated_sensors=data["calibrated_sensors"]
            for k,v in calibrated_sensors.items():
                if 'cam_' in k:
                    trans_matrix[k]=[ i for ll in v["T_lidar_to_pixel"] for i in ll]
                    intrinsic[k]=v["intrinsic"]
                   
            get_frame_info(_id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic)
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"success","message":"解析数据成功"})
        except Exception as e:
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"failed","message":f"解析失败 {str(e)}"})
            traceback.print_exc()


def analysis(file_path):
    if ".pkl" in file_path:
        analysis_pkl_file(file_path)
    elif ".json" in file_path:
        analysis_json_file(file_path)


if __name__ == '__main__':    
    # file_path = "s3://tf-rhea-data-bpp/170km-track-all-frame-pack-nori/labeled_data/car_505/20231116_dp-track_yueying_checked/ppl_bag_20231116_133205-partical_partial_track/v0_231206_033505/0001.json"
    # 线上debug
    file_path = "s3://xiangtian/release_model/lidar3d/bmk/tfboard_fix.pkl"
    analysis_pkl_file(file_path)
