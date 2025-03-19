import json
import refile 
import cv2 
import numpy as np 
import os 
from loguru import logger
import tqdm 
import nori2
import sys 
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/data/oss_visualization_fastapi")
from utils.pcd_writer_utils import to_pcd_nori_reader


def generate_sensor_data(frames, trans_matrix, intrinsic):
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

    def read_nori(nr, nori_id):
        save_root = "/data/oss_visualization_fastapi/test/data"
        oss_path = os.path.join(save_root, nori_id+".pcd")
        pcd_flag = to_pcd_nori_reader(
            nori_id=nori_id,
            nr=nr,
            save_path=oss_path)

            
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
            
            sensor_data_format.setdefault(nori_path, []).append(nori_info)

    for nori_path, nori_infos in tqdm.tqdm(sensor_data_format.items(), desc="process sensor data"):
        logger.info(f"nori_path: {nori_path}, nori_id length: {len(nori_infos)}")
        nr = nori2.open(nori_path)
        with ThreadPoolExecutor(10) as exector:
            for info in tqdm.tqdm(nori_infos, desc="process nori_info"):
                nori_id = info["nori_id"]

                exector.submit(read_nori, nr, nori_id)
        
        # for info in tqdm.tqdm(nori_infos, desc="process nori_info"):
        #     nori_id = info["nori_id"]

        #     read_nori(nr, nori_id)
        
        nr.close()
    
    return all_resources

file_path = "s3://tf-rhea-data-bpp/swim_labeled_data/dynamic_obstacle_ghost_data_part2/car_504/20240319_dp-det_yueying_checked/ppl_bag_20240319_101311_det/v0_240323_040723/0027.json"
with refile.smart_open(file_path) as f:
    frames = json.loads(f.read())["frames"]
    generate_sensor_data(frames, {}, {})