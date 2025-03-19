from concurrent.futures import ThreadPoolExecutor
import json
import pickle
import sys
import traceback
import os 
from io import BytesIO
import time 

from loguru import logger
import refile
import tqdm
import requests
from PIL import Image
import numpy as np
import nori2 
import cv2 
# sys.path.insert(0, "/data/oss_visualization_fastapi")
# os.environ["env"] = "dev" # dev or pro

from factory import update_one_data
from utils.pcd_writer_utils import to_pcd_nori_reader
from utils.pcd_aliyun_utils import conver_oss_to_url, get_aliyun_sensor
from config import IMAGE_DIR, PCD_DIR, RADAR_DIR, BASE_URL

GET_PCD_URI = "api/v1/pcds/get_3d_pcd"
GET_RADAR_URI = "api/v1/radars/get_3d_radar"
GET_IMAGE_URI = "api/v1/images/get_3d_image"
ALIYUN_DATA_ROOT = "broadside-transform/dev/nova/oss_visualization"



class Analysis(object):
    def __init__(self):
        pass
    
    def process_aliyun_sensor_data(self, nori_infos, intrinsic, nori_path, trans_matrix, all_resources):
        def f_data(oss_url, key, ext, data=None):
            if data:
                return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key,"data": data}
            else:
                return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key}

        def get_img_shape(oss_url):
            try:
                resp=requests.get(url=oss_url)
                image = Image.open(BytesIO(resp.content))
                image_array = np.array(image)
                height, width = image_array.shape[:2]
                return height,width
            except:
                return None, None
        
        def convert_url(sensor, sensor_type):
            oss_url = conver_oss_to_url(sensor["oss_path"])
            sensor["oss_url"] = oss_url
            if sensor_type == "radar":
                try:
                    data = json.loads(requests.get(url=oss_url).content.decode())
                    sensor["data"] = data
                except Exception as e:
                    logger.info(f"get radar error: {repr(e)}")
                    sensor["data"] = []
            if sensor_type == "image":
                try:
                    h, w = get_img_shape(oss_url)
                    sensor["size"] = {"height": h, "width": w}
                except:
                    sensor["size"] = {}
                
        lidar_sensors = {}
        radar_sensors = {}
        image_sensors = {}
        for info in nori_infos:
            sensor_type = "" 
            key = info["key"]
            if "lidar" in key:
                sensor_type = "lidar"
                info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".pcd")
                lidar_sensors.setdefault("lidar", []).append(info)
            elif "radar" in key:
                sensor_type = "radar"
                info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".json")
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
                sensors = one_sensors[sensor_type]
                request_body = {
                    "sensor_type": sensor_type,
                    "sensors": sensors,
                    "nori_path": nori_path,
                }
                get_aliyun_sensor(request_body)
                # with ThreadPoolExecutor(5) as exector:
                #     for sensor in tqdm.tqdm(request_body["sensors"], desc="process aliyun nori"):
                #         exector.submit(convert_url, sensor, sensor_type)
                # debug
                for sensor in tqdm.tqdm(request_body["sensors"], desc="process aliyun nori"):
                    convert_url(sensor, sensor_type)
                
                for sensor in tqdm.tqdm(request_body["sensors"], desc="process aliyun nori"):
                    clip_id = sensor["clip_id"]
                    oss_url = sensor["oss_url"]
                    timestamp = sensor["timestamp"]
                    key = sensor["key"]
                    if sensor_type == "lidar":
                        resource = f_data(oss_url, key, "pcd")
                        all_resources[clip_id].append(resource)
                    elif sensor_type == "radar":
                        data = sensor["data"]
                        resource = f_data(oss_url, key, "json", data)
                        all_resources[clip_id].append(resource)
                    elif sensor_type == "image":
                        resource = f_data(oss_url, key, "jpg")
                        resource["meta"] = {
                            key: {
                                "timeStamp": timestamp,
                                "transMatrix": trans_matrix.get(key)
                            }
                        }
                        resource["size"] = sensor["size"] 
                        resource.pop("key")
                        all_resources[clip_id].append(resource)
        
        except Exception as e:
            logger.error(f"get sensor from aliyun error: {repr(e)}")
            traceback.print_exc()
    
    def generate_sensor_data(self, frames, trans_matrix, intrinsic):
        def contains_any(key, substrings):
            return any(substring in key for substring in substrings)
        
        def f_data(oss_url, key, ext, data=None):
            if data:
                return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key,"data": data}
            else:
                return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key}
        
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

        def read_nori(nr, nori_id, clip_id, timestamp, key, oss_path, all_resources):
            if refile.smart_exists(oss_path):
                if oss_path.endswith(".json"):
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
                if oss_path.endswith(".json"):
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
                    with refile.smart_open(oss_path, "wb") as f:
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
        
        all_resources = {}
        sensor_data_format = {}
        logger.info(f"len frames: {len(frames)}")
        for frame in frames: 
            clip_id = frame["clip_id"] + "_" +str(frame["frame_id"])
            all_resources[clip_id] = []
            sensor_data = frame["sensor_data"]
            for key, value in sensor_data.items():
                if key == "cam_extra_info":
                    continue 
                if not contains_any(key, ["lidar", "radar", "cam_"]):
                    continue 
                nori_id = value.get("nori_id")
                if not nori_id:
                    continue 
                nori_path = value["nori_path"]
                if nori_path.endswith("/"):
                    nori_path = nori_path[:-1]
                timestamp = value["timestamp"]
                nori_info = {
                    "key": key,
                    "clip_id": clip_id,
                    "nori_id": nori_id,
                    "timestamp": timestamp,
                }
                if "lidar" in key:
                    nori_info["oss_path"] = refile.smart_path_join(PCD_DIR, nori_id+".pcd")
                elif "radar" in key:
                    nori_info["oss_path"] = refile.smart_path_join(RADAR_DIR, nori_id+".json")
                elif 'cam_' in key and key != "cam_extra_info":
                    nori_info["oss_path"] = refile.smart_path_join(IMAGE_DIR, nori_id+".jpg")
                
                sensor_data_format.setdefault(nori_path, []).append(nori_info)

        for nori_path, nori_infos in tqdm.tqdm(sensor_data_format.items(), desc="process sensor data"):
            logger.info(f"nori_path: {nori_path}, nori_id length: {len(nori_infos)}")
            
            try:
                refile.smart_exists(nori_path)
            except Exception as e:
                # 数据不存在brain++，且没有权限，要从阿里云获取
                logger.error(f"nori_path: {nori_path}, error: {repr(e)}")
                self.process_aliyun_sensor_data(nori_infos, intrinsic, nori_path, trans_matrix, all_resources)
                continue 
            
            if not refile.smart_exists(nori_path):
                self.process_aliyun_sensor_data(nori_infos, intrinsic, nori_path, trans_matrix, all_resources)
            else:
                nr = nori2.open(nori_path)
                with ThreadPoolExecutor(10) as exector:
                    for info in tqdm.tqdm(nori_infos, desc="process nori_info"):
                        nori_id = info["nori_id"]
                        clip_id = info["clip_id"]
                        timestamp = info["timestamp"]
                        key = info["key"]
                        oss_path = info["oss_path"]
                        if "lidar" not in key:
                            exector.submit(read_nori, nr, nori_id, clip_id, timestamp, key, oss_path, all_resources)
                        # elif not refile.smart_exists(oss_path):
                        else:
                            if not refile.smart_exists(oss_path):
                                if oss_path.endswith(".pcd"):
                                    pcd_flag = None
                                    try:
                                        pcd_flag = to_pcd_nori_reader(
                                            nori_id=nori_id,
                                            nr=nr,
                                            save_path=oss_path)
                                    except Exception as e:
                                        pcd_flag = None 
                                        logger.error(f"error: {repr(e)}")
                                    if pcd_flag:
                                        oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                                        resource = f_data(oss_url, key, "pcd")
                                        all_resources[clip_id].append(resource)
                                    else:
                                        oss_url = ""
                                        resource = f_data(oss_url, key, "pcd")
                                        all_resources[clip_id].append(resource)
                            else:
                                if oss_path.endswith(".pcd"):
                                    oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                                    resource = f_data(oss_url, key, "pcd")
                                    all_resources[clip_id].append(resource)
                            
                nr.close()
        
        return all_resources
            
    def process_frames(self, _id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic):
        all_resources = self.generate_sensor_data(frames, trans_matrix, intrinsic)
        for frame in tqdm.tqdm(frames, desc="insert frame"):
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
    
    def analysis_file(self, file_path: str):
        """
        解析pkl或者json文件
        @file_path 文件路几个
        return None
        """
        try:
            t1 = time.time()
            if not file_path.endswith((".pkl", ".json")):
                error_msg = f"file_path: {file_path} is illegal"
                raise Exception(f"error_msg: {error_msg}")

            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"processing"},True)
            
            if file_path.endswith(".pkl"):
                _id = 1
                trans_matrix = {}
                intrinsic = {}
                with refile.smart_open(file_path, "rb") as f:
                    data = pickle.load(f)
                if data:
                    for item in tqdm.tqdm(data, desc="process pkl data"):
                        frames = item["frames"]
                        calibrated_sensors = item["calibrated_sensors"]
                        for k, v in calibrated_sensors.items():
                            if "cam_" in k:
                                trans_matrix[k]=[ i for ll in v["T_lidar_to_pixel"] for i in ll]
                                intrinsic[k]=v["intrinsic"]
                        
                        self.process_frames(_id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic)
                
            elif file_path.endswith(".json"):
                _id = 1
                trans_matrix = {}
                intrinsic = {}
                with refile.smart_open(file_path) as f:
                    data = json.loads(f.read())
                if data:
                    frames = data["frames"]
                    calibrated_sensors=data["calibrated_sensors"]
                    for k,v in calibrated_sensors.items():
                        if 'cam_' in k:
                            trans_matrix[k]=[ i for ll in v["T_lidar_to_pixel"] for i in ll]
                            intrinsic[k]=v["intrinsic"]
                        
                    self.process_frames(_id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic)
            
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"success","message":"解析数据成功"})
            t2 = time.time()
            logger.info(f"process file_path: {file_path} spend time: {t2 - t1}")
        except Exception as e:
            traceback.print_exc()
            logger.error(f"analysis file error: {repr(e)}")
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"failed","message":f"解析失败 {str(e)}"})

analysis = Analysis()

if __name__ == '__main__':
    # file_path = "s3://tf-rhea-data-bpp/swim_labeled_data/suidaokoudierci_data_2024-08-14-17_31_43/car_505/20240729_dp-det_yueying_checked/ppl_bag_20240729_020618_det/v0_240803_210133/0022.json"
    # file_path = "/data/oss_visualization_fastapi/test/roi_test.json"
    file_path = "s3://tf-rhea-data-bpp/swim_labeled_data/suidaokoudierci_data_2024-08-14-17_31_43/car_505/20240729_dp-det_yueying_checked/ppl_bag_20240729_020618_det/v0_240803_210133/0022.json"
    analysis.analysis_file(file_path)
    