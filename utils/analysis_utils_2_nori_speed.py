from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import copy
from importlib import resources
import json
import pickle
import sys
import threading
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
sys.path.insert(0, "/data/zhailipu/code/oss_visualization_fastapi")
# os.environ["env"] = "pro" # dev or pro

from factory import update_one_data, get_one_frame_resouces
from utils.pcd_writer_utils import to_pcd_nori_reader
from utils.pcd_aliyun_utils import conver_oss_to_url, get_aliyun_sensor, convert_oss_to_url_multi
from config import IMAGE_DIR, PCD_DIR, RADAR_DIR, BASE_URL
from utils import file_utils 
from utils import pcd2cam
import tasks 

GET_PCD_URI = "api/v1/pcds/get_3d_pcd"
GET_RADAR_URI = "api/v1/radars/get_3d_radar"
GET_IMAGE_URI = "api/v1/images/get_3d_image"
ALIYUN_DATA_ROOT = "broadside-transform/dev/nova/oss_visualization"

fetcher = nori2.Fetcher()

def speed_nori_path(nori_path):
    print(f'nori speedup --replica 2 --on {nori_path}')
    os.system(f'nori speedup --replica 2 --on {nori_path}')


class Analysis(object):
    def __init__(self):
        pass
    
    def get_nori_path_source(self, nori_path):
        aliyun = False 
        brainpp = False
        # renori 或者 bpp 在nori_path字符串里面为brain++
        if "renori" in nori_path or "bpp" in nori_path:
            brainpp = True 
        try:
            # 路径存在则为brain++
            if refile.smart_exists(nori_path):
                brainpp = True  
            else:
                # 判断路径是否存在于阿里云
                if file_utils.oss_dir_is_exist(nori_path):
                    aliyun = True 
                else:
                    raise Exception(f"nori_path: {nori_path} is not exists")
        except:
            # 异常说明没法使用refile判断nori_path的位置
            if file_utils.oss_dir_is_exist(nori_path):
                aliyun = True 
            else:
                raise Exception(f"nori_path: {nori_path} is not exists") 

        if aliyun:
            return False
        elif brainpp:
            return True 
        if not aliyun and not brainpp:
            raise Exception(f"nori_path: {nori_path} is not exists") 
    
    def fuser_lidar_project_image(self, all_resources, calibrated_sensors):   
        # logger.info(f"process fuser lidar project image start")  
        # with ThreadPoolExecutor(20) as exector:
        #     for _, resouces in tqdm.tqdm(all_resources.items(), desc="process project"):
        #         # pcd2cam.fuser_lidar_project_images(resouces, calibrated_sensors)
        #         exector.submit(pcd2cam.fuser_lidar_project_images, resouces, calibrated_sensors)
        # logger.info(f"process fuser lidar project image end!!!")

        logger.info(f"process fuser lidar project image start")  
        task_args = []
        for clip_id, resources in all_resources.items():
            task_args.append((clip_id, resources, calibrated_sensors))

        with ProcessPoolExecutor(max_workers = 8) as executor:
            processed_sensors = list(tqdm.tqdm(
                executor.map(pcd2cam.fuser_lidar_project_images, task_args),
                total=len(all_resources),
                desc="Processing project"
            ))
        logger.info(f"process fuser lidar project image end!!!")
        return {item[0]: item[1] for item in processed_sensors}
    
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
                    logger.info(f"get radar error: {repr(e)}, oss_url: {oss_url}")
                    sensor["data"] = []
            if sensor_type == "image":
                sensor['ori_oss_url'] = conver_oss_to_url(sensor["ori_oss_path"])
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
                info["ori_oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+"_ori.pcd")
                lidar_sensors.setdefault("lidar", []).append(info)
            elif "radar" in key:
                sensor_type = "radar"
                info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".json")
                lidar_sensors.setdefault("radar", []).append(info)
            elif "cam_" in key:
                sensor_type = "image"
                info["oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+".jpg")
                info["ori_oss_path"] = os.path.join(ALIYUN_DATA_ROOT, info["nori_id"]+"_ori.jpg")
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
                
                if sensor_type == "radar":
                    new_sensors = []
                    chunk_size = 100
                    for i in range(0, len(sensors), chunk_size):
                        sub_sensors = sensors[i: i + chunk_size]
                        sub_sensors = convert_oss_to_url_multi(sub_sensors) 
                        new_sensors.extend(sub_sensors)
                    
                    sensors = copy.deepcopy(new_sensors)
                else:
                    sensors = convert_oss_to_url_multi(sensors)

                request_body["sensors"] = sensors
                # debug 
                # import time
                # with open(f"{str(time.time())}.json", "w") as f:
                #     f.write(json.dumps(request_body["sensors"]))
                # for sensor in tqdm.tqdm(request_body["sensors"], desc="process aliyun nori"):
                #     convert_url(sensor, sensor_type)
                for sensor in tqdm.tqdm(request_body["sensors"], desc="process aliyun nori"):
                    clip_id = sensor["clip_id"]
                    # oss_url = sensor["oss_url"]
                    timestamp = sensor["timestamp"]
                    key = sensor["key"]
                    if sensor_type == "lidar":
                        oss_url = sensor["oss_url"]
                        resource = f_data(oss_url, key, "pcd")
                        resource["ori_oss_url"] = sensor["ori_oss_url"]
                        all_resources[clip_id].append(resource)
                    elif sensor_type == "radar":
                        oss_url = ""
                        data = sensor["data"]
                        resource = f_data(oss_url, key, "json", data)
                        all_resources[clip_id].append(resource)
                    elif sensor_type == "image":
                        oss_url = sensor["oss_url"]
                        resource = f_data(oss_url, key, "jpg")
                        resource["meta"] = {
                            key: {
                                "timeStamp": timestamp,
                                "transMatrix": trans_matrix.get(key)
                            }
                        }
                        # resource["size"] = sensor["size"]
                        resource["size"] = sensor.get("size") # 可能会出错
                        resource['ori_oss_url'] = sensor['ori_oss_url']
                        # resource.pop("key")
                        all_resources[clip_id].append(resource)
        
        except Exception as e:
            logger.error(f"get sensor from aliyun error: {repr(e)}")
            traceback.print_exc()
    
    def generate_sensor_data(self, frames, trans_matrix, intrinsic, sensors):
        def contains_any(key, substrings):
            return any(substring in key for substring in substrings)
        
        def f_data(oss_url, key, ext, data=None):
            if data:
                return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key,"data": data}
            else:
                return {"oss_path": oss_url.replace(f".{ext}", ""),"ext": ext,"meta":None,"size":{},"oss_url":oss_url,"key": key}
        
        def image_undistort(img, cam_intrin):
            # 如果去畸变的图片尺寸和resolution中的尺寸不一致，则resize成一致的
            h, w = img.shape[:2]
            resolution = cam_intrin["resolution"]
            intrin_width, intrin_height = resolution[0], resolution[1]
            if h != intrin_height or w != intrin_width:
                img = cv2.resize(img, (intrin_width, intrin_height))
                h, w = intrin_height, intrin_width
                
            k = np.array(cam_intrin['K']).reshape((3, 3))
            d = np.array(cam_intrin['D'])
            mode = cam_intrin['distortion_model']
            if mode == 'pinhole':
                mapx, mapy = cv2.initUndistortRectifyMap(k, d, None, k, (w, h), 5)
            elif mode == 'fisheye':
                mapx, mapy = cv2.fisheye.initUndistortRectifyMap(k, d, None, k, (w, h), 5)
            return cv2.remap(img, mapx, mapy, cv2.INTER_LINEAR)

        def read_nori(nr, nori_id, clip_id, timestamp, key, oss_path):
            # nr, nori_id, clip_id, timestamp, key, oss_path = args
            # if refile.smart_exists(oss_path):
            #     if oss_path.endswith(".json"):
            #         oss_url=f"{BASE_URL}/{GET_RADAR_URI}/{nori_id}.json"
            #         with refile.smart_open(oss_path) as f:
            #             data = json.loads(f.read())
            #         resource = f_data(oss_url, key, "json", data)                        
            #         # all_resources[clip_id].append(resource)
            #         return clip_id, resource
                    
            #     elif oss_path.endswith(".jpg"):
            #         with refile.smart_open(oss_path, "rb") as f:
            #             image = Image.open(BytesIO(f.read()))
            #             # 将 PIL 图像转换为 numpy 数组
            #         img = np.array(image)
            #         h, w, g = img.shape
            #         oss_url = f"{BASE_URL}/{GET_IMAGE_URI}/{nori_id}.jpg"
                    
            #         ori_img_path = oss_path.replace(".jpg", "_ori.jpg")
            #         if not refile.smart_exists(ori_img_path):
            #             with lock:
            #                 byte_data = nr.get(nori_id)
                        
            #             with refile.smart_open(ori_img_path, "wb") as f:
            #                 f.write(byte_data)
                    
            #         ori_oss_url = f"{BASE_URL}/{GET_IMAGE_URI}/{nori_id}_ori.jpg"
            #         resource = f_data(oss_url, key, "jpg")
            #         resource["meta"] = {
            #             key: {
            #                 "timeStamp":timestamp,
            #                 "transMatrix":trans_matrix.get(key)
            #             }
            #         }
            #         resource['ori_oss_url'] = ori_oss_url
            #         resource["size"] = {"height": h, "width": w}
            #         # resource.pop("key")
                    
            #         # all_resources[clip_id].append(resource)
            #         return clip_id, resource
            # else:
                if oss_path.endswith(".json"):
                    # with lock:
                    byte_data = nr.get(nori_id)
                    radar_info = pickle.loads(byte_data)
                    objects = radar_info["objects"]
                    with refile.smart_open(oss_path, "w") as f:
                        f.write(json.dumps(objects))
                    oss_url=f"{BASE_URL}/{GET_RADAR_URI}/{nori_id}.json"
                    resource = f_data(oss_url, key, "json", objects)                        
                    # all_resources[clip_id].append(resource)
                    return clip_id, resource
                    
                elif oss_path.endswith(".jpg"):
                    # with lock:
                    byte_data = nr.get(nori_id)
                    ns = np.frombuffer(byte_data, dtype=np.uint8)
                    img = cv2.imdecode(ns, cv2.IMREAD_COLOR)

                    oss_path_ori = f'{oss_path.replace(".jpg", "")}_ori.jpg'
                    _ori, img_ori_bytes = cv2.imencode('.jpg', img)
                    with refile.smart_open(oss_path_ori, 'wb') as fso:
                        fso.write(img_ori_bytes)
                    ori_oss_url = f"{BASE_URL}/{GET_IMAGE_URI}/{nori_id}_ori.jpg"

                    try:
                        img_array = image_undistort(img,intrinsic.get(key))
                    except Exception as e:
                        logger.error(f"undistort error: params: {intrinsic.get(key)}")
                        img_array = img
                    _, img_bytes = cv2.imencode('.jpg', img_array)
                    with refile.smart_open(oss_path, "wb") as f:
                        f.write(img_bytes)
                    h, w, g = img_array.shape
                    oss_url = f"{BASE_URL}/{GET_IMAGE_URI}/{nori_id}.jpg"
                    resource = f_data(oss_url, key, "jpg")
                    resource["meta"] = {
                        key: {
                            "timeStamp":timestamp,
                            "transMatrix":trans_matrix.get(key)
                        }
                    }
                    resource['ori_oss_url'] = ori_oss_url
                    resource['project_path'] = refile.smart_path_join(IMAGE_DIR, nori_id+"_project.jpg")
                    resource["size"] = {"height": h, "width": w}
                    # resource.pop("key")
                    # all_resources[clip_id].append(resource)
                    return clip_id, resource

                elif oss_path.endswith(".pcd"):
                    if oss_path.endswith(".pcd"):
                        ori_oss_path = oss_path.replace(".pcd", "_ori.pcd")
                        pcd_flag = None
                        try:
                            pcd_flag = to_pcd_nori_reader(
                                nori_id=nori_id,
                                nr=nr,
                                save_path=oss_path,
                                ori_oss_path=ori_oss_path)
                        except Exception as e:
                            pcd_flag = None 
                            logger.error(f"error: {repr(e)}")
                        if pcd_flag:
                            oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                            resource = f_data(oss_url, key, "pcd")
                            resource["ori_oss_url"] = f"{BASE_URL}/{GET_PCD_URI}/{nori_id}_ori.pcd"
                            # all_resources[clip_id].append(resource)
                        else:
                            oss_url = ""
                            resource = f_data(oss_url, key, "pcd")
                            # all_resources[clip_id].append(resource)
                        return clip_id, resource

        all_resources = {}
        sensor_data_format = {}
        logger.info(f"len frames: {len(frames)}")
        for frame in frames: 
            # clip_id = frame["clip_id"] + "_" +str(frame["frame_id"])
            clip_id = frame.get("clip_id", "") + "_" +str(frame["frame_id"])
            all_resources[clip_id] = []
            sensor_data = frame["sensor_data"]
            for key, value in sensor_data.items():
                if sensors and key not in sensors: # key不在所选sensor中跳过
                    continue 
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
            # try:
            #     # 处理aliyun和brain++不同bucket的问题
            #     refile.smart_exists(nori_path)
            # except Exception as e:
            #     # 数据不存在brain++，且没有权限，要从阿里云获取
            #     logger.error(f"nori_path: {nori_path}, error: {repr(e)}")
            #     self.process_aliyun_sensor_data(nori_infos, intrinsic, nori_path, trans_matrix, all_resources)
            #     continue 
            
            # # e2e-rhea-data 这个bucket aliyun有，brain++也有，所以要加双层判断
            # if not refile.smart_exists(nori_path) and not file_utils.oss_dir_is_exist(nori_path):
            #     # 报错：路径不存在错误
            #     raise Exception(f"nori_path: {nori_path} is not exists")
                # self.process_aliyun_sensor_data(nori_infos, intrinsic, nori_path, trans_matrix, all_resources)
            # elif not refile.smart_exists(nori_path) and file_utils.oss_dir_is_exist(nori_path):
            #     self.process_aliyun_sensor_data(nori_infos, intrinsic, nori_path, trans_matrix, all_resources)
            # else:
            
            
            brainpp = self.get_nori_path_source(nori_path)
            # aliyun
            if not brainpp:
                logger.info(f"aliyun: nori_path: {nori_path}, nori_id length: {len(nori_infos)}")
                self.process_aliyun_sensor_data(nori_infos, intrinsic, nori_path, trans_matrix, all_resources)

            # brain++的nori信息
            if brainpp:
                logger.info(f"brain++: nori_path: {nori_path}, nori_id length: {len(nori_infos)}")
                # nr = nori2.open(nori_path)
                speed_nori_path(nori_path)
                nr = fetcher

                others_nori_infos = []
                for info in tqdm.tqdm(nori_infos, desc="process lidar info"):
                    nori_id = info["nori_id"]
                    clip_id = info["clip_id"]
                    timestamp = info["timestamp"]
                    key = info["key"]
                    oss_path = info["oss_path"]
                    # if "lidar" in key:
                    #     # if not refile.smart_exists(oss_path):
                    #         if oss_path.endswith(".pcd"):
                    #             ori_oss_path = oss_path.replace(".pcd", "_ori.pcd")
                    #             pcd_flag = None
                    #             try:
                    #                 pcd_flag = to_pcd_nori_reader(
                    #                     nori_id=nori_id,
                    #                     nr=nr,
                    #                     save_path=oss_path,
                    #                     ori_oss_path=ori_oss_path)
                    #             except Exception as e:
                    #                 pcd_flag = None 
                    #                 logger.error(f"error: {repr(e)}")
                    #             if pcd_flag:
                    #                 oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                    #                 resource = f_data(oss_url, key, "pcd")
                    #                 resource["ori_oss_url"] = f"{BASE_URL}/{GET_PCD_URI}/{nori_id}_ori.pcd"
                    #                 all_resources[clip_id].append(resource)
                    #             else:
                    #                 oss_url = ""
                    #                 resource = f_data(oss_url, key, "pcd")
                    #                 all_resources[clip_id].append(resource)
                        # else:
                            # if oss_path.endswith(".pcd"):
                            #     oss_url=f"{BASE_URL}/{GET_PCD_URI}/{nori_id}.pcd"
                            #     resource = f_data(oss_url, key, "pcd")
                            #     all_resources[clip_id].append(resource)

                    # else:
                    others_nori_infos.append(info)


                # task_list = []
                # for info in tqdm.tqdm(others_nori_infos, desc="process radar and image"):
                #     nori_id = info["nori_id"]
                #     clip_id = info["clip_id"]
                #     timestamp = info["timestamp"]
                #     key = info["key"]
                #     oss_path = info["oss_path"]
                #     task_list.append((nr, nori_id, clip_id, timestamp, key, oss_path))

                lock = threading.Lock()
                with ThreadPoolExecutor(15) as exector:
                    futures = []
                    for info in others_nori_infos:
                        nori_id = info["nori_id"]
                        clip_id = info["clip_id"]
                        timestamp = info["timestamp"]
                        key = info["key"]
                        oss_path = info["oss_path"]
                    # result = exector.map(read_nori, task_list)
                        future = exector.submit(read_nori, nr, nori_id, clip_id, timestamp, key, oss_path)
                        futures.append(future)
                    
                    for future in as_completed(tqdm.tqdm(futures, desc="process radar and image")):
                        try:
                            clip_id, resource = future.result() 
                            all_resources[clip_id].append(resource)
                        except Exception as e:
                            logger.error(f"error: {repr(e)}")
                     
                # nr.close()
        
        return all_resources
            
    def process_frames(self, _id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic, sensors, is_project, is_last=False):
        all_resources = self.generate_sensor_data(frames, trans_matrix, intrinsic, sensors)
        # all_resources = self.fuser_lidar_project_image(all_resources, calibrated_sensors)
        if is_project:
            tasks.fuser_lidar_project_image.apply_async(kwargs={
                "_id": _id,
                "frames": frames,
                "file_path": file_path,
                "calibrated_sensors": calibrated_sensors,
                "sensors": sensors,
                "all_resources": all_resources,
                "is_last": is_last,
            })
        else:
            for frame in tqdm.tqdm(frames, desc="insert frame"):
                try:
                    labels=frame["labels"]
                except:
                    labels=None
                frame_id=frame.get("frame_id", "")
                ins_data=frame["ins_data"]
                origin_frame_id=frame.get("origin_frame_id", "")
                preds=frame.get("preds")
                if preds is None:
                    preds=[]
                pre_labels=frame.get("pre_labels", [])            
                roi_info=frame.get("roi_info",{})
                # clip_id = frame["clip_id"] + "_" +str(frame["frame_id"])
                clip_id = frame.get("clip_id", "") + "_" +str(frame["frame_id"])

                resources = all_resources[clip_id]            
                final_resouces = []
                final_sensors = set()
                # 更新已经存在的resources
                old_resources = get_one_frame_resouces({"id":_id,"source_path":file_path})
                if old_resources:
                    old_resources_d = {item["key"]: item for item in old_resources}
                    new_resources_d = {item["key"]: item for item in resources}
                    for key in new_resources_d:
                        old_resources_d[key] = new_resources_d[key]
                        final_sensors.add(key)
                
                    for key, item in old_resources_d.items():
                        final_resouces.append(item)
                        final_sensors.add(key)
                else:
                    final_resouces = copy.deepcopy(resources)
                    sensors = [item["key"] for item in resources]
                    final_sensors = set(sensors)
                
                final_sensors = list(final_sensors)
                if sensors and len(sensors) != len(final_resouces):
                    raise Exception(f"error: 加载sensor失败, {set(sensors) - set(final_sensors)}, frame_id: {frame_id}")
                result_dict = {"id": _id,"source_path":file_path,"origin_frame_id":origin_frame_id,"frame_id":frame_id,"data":{"labels":labels,"pre_labels":pre_labels,"preds":preds},"resources":final_resouces,"sensors": final_sensors,"ins_data":ins_data,"roi_info":roi_info,"calibrated_sensors":calibrated_sensors}
                update_one_data("3d_object", {"id":_id,"source_path":file_path}, result_dict,True)
                _id += 1
    
    def analysis_file(self, file_path: str, sensors: list, is_project: bool = False):
        """
        解析pkl或者json文件
        @file_path 文件路径
        @sensors 选择的sensor列表
        @is_project 是否生成图片投影
        return None
        """
        logger.info(f"file_path: {file_path}, sensors: {sensors}, is_project: {is_project}")
        try:
            t1 = time.time()
            if not file_path.endswith((".pkl", ".json")):
                error_msg = f"file_path: {file_path} is illegal"
                raise Exception(f"error_msg: {error_msg}")

            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"processing"},True)
            
            if file_path.endswith(".pkl"):
                trans_matrix = {}
                intrinsic = {}
                try:
                    with refile.smart_open(file_path, "rb") as f:
                        data = pickle.load(f)
                except Exception as e:
                    logger.error(f"read file: {file_path} content error: {repr(e)}")
                    try:
                        data = file_utils.get_file_content(file_path)
                    except Exception as e:
                        logger.error(f"read file from aliyun error: {repr(e)}")
                        data = []
                
                if data:
                    _id = 1
                    for index, item in enumerate(data):
                        frames = item["frames"]
                        calibrated_sensors = item["calibrated_sensors"]
                        for k, v in calibrated_sensors.items():
                            if "cam_" in k:
                                trans_matrix[k]=[ i for ll in v["T_lidar_to_pixel"] for i in ll]
                                intrinsic[k]=v["intrinsic"]
                                
                        if index + 1 == len(data):
                            is_last = True
                        else:
                            is_last = False
                        self.process_frames(_id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic, sensors, is_project, is_last=is_last)
                        _id += len(frames)
                        
            elif file_path.endswith(".json"):
                _id = 1
                trans_matrix = {}
                intrinsic = {}
                try:
                    with refile.smart_open(file_path) as f:
                        data = json.loads(f.read())
                except Exception as e:
                    logger.error(f"read file: {file_path} content error: {repr(e)}")
                    try:
                        data = file_utils.get_file_content(file_path)
                    except Exception as e:
                        logger.error(f"read file from aliyun error: {repr(e)}")
                        data = []
                    
                if data:
                    frames = data["frames"]
                    calibrated_sensors=data["calibrated_sensors"]
                    for k,v in calibrated_sensors.items():
                        if 'cam_' in k:
                            trans_matrix[k]=[ i for ll in v["T_lidar_to_pixel"] for i in ll]
                            intrinsic[k]=v["intrinsic"]
                        
                    self.process_frames(_id, frames, file_path, trans_matrix, calibrated_sensors, intrinsic, sensors, is_project, is_last=True)
            
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"success","message":"解析数据成功"})
            t2 = time.time()
            logger.info(f"process file_path: {file_path} spend time: {t2 - t1}")
        except Exception as e:
            traceback.print_exc()
            logger.error(f"analysis file error: {repr(e)}")
            update_one_data("3d_data_source", {"source_path":file_path}, {"status":"failed","message":f"解析失败 {str(e)}"})

    def get_sensors(self, frames):
        def contains_any(key, substrings):
            return any(substring in key for substring in substrings)
        
        sensors = set()
        for frame in frames: 
            sensor_data = frame["sensor_data"]
            for key in sensor_data:
                if key == "cam_extra_info":
                    continue 
                if not contains_any(key, ["lidar", "radar", "cam_"]):
                    continue 
                
                sensors.add(key)
                 
        return sensors
    
    def get_sensor_list(self, file_path: str):
        t1 = time.time()
        if not file_path.endswith((".pkl", ".json")):
            error_msg = f"file_path: {file_path} is illegal"
            raise Exception(f"error_msg: {error_msg}")

        if file_path.endswith(".pkl"):
            try:
                with refile.smart_open(file_path, "rb") as f:
                    data = pickle.load(f)
            except Exception as e:
                logger.error(f"read file: {file_path} content error: {repr(e)}")
                try:
                    data = file_utils.get_file_content(file_path)
                except Exception as e:
                    logger.error(f"read file from aliyun error: {repr(e)}")
                    data = []        
            full_sensors = set()
            if data:
                for item in tqdm.tqdm(data, desc="process pkl data"):
                    frames = item["frames"]
                    sensors = self.get_sensors(frames)
                    for sensor in sensors:
                        full_sensors.add(sensor)
            
        elif file_path.endswith(".json"):
            try:
                with refile.smart_open(file_path) as f:
                    data = json.loads(f.read())
            except Exception as e:
                logger.error(f"read file: {file_path} content error: {repr(e)}")
                try:
                    data = file_utils.get_file_content(file_path)
                except Exception as e:
                    logger.error(f"read file from aliyun error: {repr(e)}")
                    data = []
                
            full_sensors = set()
            if data:
                frames = data["frames"]
                sensors = self.get_sensors(frames)
                for sensor in sensors:
                    full_sensors.add(sensor)
        
        else:
            raise Exception(f"file path: {file_path} is illegal")
        
        t2 = time.time()
        logger.info(f"get sensor list from file_path: {file_path} spend time: {t2 - t1}")
        return list(full_sensors)

    
    
analysis = Analysis()

if __name__ == '__main__':

    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_11/20241111_dp-track/ppl_bag_20241111_225447_det/v0_241118_144858/splited_video_prelabels_tracking/0019.json"
    # file_path = "s3://zhailipu-data/test/test_2frame_20241212.json"
    file_path = "s3://zhailipu-data/test/test_2frame_20241212.json"
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_16/20241117_dp-track/ppl_bag_20241117_130625_det/v0_241120_163307/splited_video_prelabels_tracking/0008.json"
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_16/20241117_dp-track/ppl_bag_20241117_125110_det/v0_241120_162333/splited_video_prelabels_tracking/0006.json"

    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_13/20240925_dp-track/ppl_bag_20240925_011227_det/v0_241004_203203/splited_video_prelabels_tracking/0077.json"
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_11/20241111_dp-track/ppl_bag_20241111_225447_det/v0_241118_144858/splited_video_prelabels_tracking/0002.json"

    # todo 
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_11/20241111_dp-track/ppl_bag_20241111_225447_det/v0_241118_144858/splited_video_prelabels_tracking/0019.json"
    # file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_13/20240925_dp-track/ppl_bag_20240925_011227_det/v0_241004_203203/splited_video_prelabels_tracking/0011.json"
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_13/20240925_dp-track/ppl_bag_20240925_011227_det/v0_241004_203203/splited_video_prelabels_tracking/0077.json"

    # todo test
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_16/20241117_dp-track/ppl_bag_20241117_125110_det/v0_241120_162333/splited_video_prelabels_tracking/0006.json"
    file_path = "s3://tf-rhea-ci-data/new_track_gjh/prelabeled_data/car_z02/20241202_dp-det/ppl_bag_20241202_163210_det/v0_241204_111543/splited_video_prelabels_tracking/0006.json"
    file_path = "s3://zhailipu-data/test/oss_vis/20241220_test_1.json"
    file_path = "s3://zhailipu-data/test/tfboard_fix_20241220.pkl"
    file_path = "s3://zhailipu-data/test/oss_vis/20241220_test_2.json"
    file_path = "s3://zhailipu-data/test/test_2frame_20241212.json"
    file_path = "s3://tf-rhea-ci-data/new_track_gjh/prelabeled_data/car_z02/20241202_dp-det/ppl_bag_20241202_163210_det/v0_241204_111543/splited_video_prelabels_tracking/0006.json"
    file_path = "s3://megsim/task-plan/19907/1541/car_504__reinjection__20231119__093140__260993/evaluate_result/fusion/tfboard_ppl_bag_20231119_092139_20_record_recovered_cipv_static.pkl"
    file_path = "s3://megsim/reinjection/GT/OD_VEL/car_504/20231119/ppl_bag_20231119_092139_20.json"
    file_path = "s3://zhailipu-data/test/oss_vis/20241220_test_2.json"
    file_path = "s3://zhailipu-data/test/oss_vis/test_frame_20241225_103_105.json"
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z06/20241224_dp-track/ppl_bag_20241224_204547_det/v0_241230_163005/splited_video_prelabels_tracking/0001.json"
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z09/20241225_dp-track/ppl_bag_20241225_183241_det/v0_241227_202719/splited_video_prelabels_tracking/0006.json"
    # sensor_list = analysis.get_sensor_list(file_path)
    
    # file_paths = [
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z10/20241227_dp-track/ppl_bag_20241227_101847_det/v0_241230_101544/splited_video_prelabels_tracking/0113.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z11/20241227_dp-track/ppl_bag_20241227_183635_det/v0_241230_212925/splited_video_prelabels_tracking/0002.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z11/20241227_dp-track/ppl_bag_20241227_172239_det/v0_241231_165809/splited_video_prelabels_tracking/0047.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z08/20241227_dp-track/ppl_bag_20241227_111643_det/v0_241230_230611/splited_video_prelabels_tracking/0086.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z06/20241224_dp-track/ppl_bag_20241224_230042_det/v0_241231_031634/splited_video_prelabels_tracking/0004.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z05/20241224_dp-track/ppl_bag_20241224_214720_det/v0_241230_155348/splited_video_prelabels_tracking/0073.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z04/20241227_dp-track/ppl_bag_20241227_123451_det/v0_241231_012845/splited_video_prelabels_tracking/0008.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z03/20241225_dp-track/ppl_bag_20241225_220442_det/v0_241230_190034/splited_video_prelabels_tracking/0020.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z02/20241227_dp-track/ppl_bag_20241227_225338_det/v0_241231_071552/splited_video_prelabels_tracking/0086.json",
    #     "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z01/20241224_dp-track/ppl_bag_20241224_125913_det/v0_241230_162212/splited_video_prelabels_tracking/0065.json"
    # ]
    # with ProcessPoolExecutor(10) as exector:
    #     for file_path in file_paths:
    #         exector.submit(analysis.analysis_file, file_path, [], is_project=False)

    # file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z04/20241227_dp-track/ppl_bag_20241227_123451_det/v0_241231_012845/splited_video_prelabels_tracking/0008.json"
    # sensors = analysis.get_sensor_list(file_path)
    # print(sensors)
    file_path = "s3://tf-rhea-data-bpp/new_track_gjh/prelabeled_data/car_z02/20250110_dp-track/ppl_bag_20250110_153457_det/v0_250114_062254/splited_video_prelabels_tracking/0013.json"
    sensors = ["radar0","radar4","radar3","radar1","radar2","front_lidar","fuser_lidar","front_2_lidar","middle_lidar","cam_front_left_100","cam_front_right_100","cam_back_left_100","cam_back_right_100","cam_front_120","cam_front_30","cam_back_70"]
    analysis.analysis_file(file_path, sensors, is_project=False)