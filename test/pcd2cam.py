import json
import os 
import io 
from io import BytesIO

from IPython import embed
import cv2 as cv
import nori2
import refile 
import numpy as np 
import requests
from PIL import Image
from pyquaternion import Quaternion


def transform_matrix(
    translation: np.ndarray = np.array([0, 0, 0]),
    rotation: Quaternion = Quaternion([1, 0, 0, 0]),
    inverse: bool = False,
) -> np.ndarray:
    """
    Convert pose to transformation matrix.
    :param translation: <np.float32: 3>. Translation in x, y, z.
    :param rotation: Rotation in quaternions (w ri rj rk).
    :param inverse: Whether to compute inverse transform matrix.
    :return: <np.float32: 4, 4>. Transformation matrix.
    """
    tm = np.eye(4)
    if inverse:
        rot_inv = rotation.rotation_matrix.T
        trans = np.transpose(-np.array(translation))
        tm[:3, :3] = rot_inv
        tm[:3, 3] = rot_inv.dot(trans)
    else:
        tm[:3, :3] = rotation.rotation_matrix
        tm[:3, 3] = np.transpose(np.array(translation))
    return tm

def get_map(h, w, K, D, mode):
    K = np.array(K).reshape(3, 3)
    D = np.array(D)
    if mode == 'pinhole':
        mapx, mapy = cv.initUndistortRectifyMap(K, D, None, K, (w, h), 5)
    elif mode == 'fisheye':
        mapx, mapy = cv.fisheye.initUndistortRectifyMap(K, D, None, K, (w, h), 5)
    return mapx, mapy


def get_camera_caliresult(extrinsic_camera, intrinsics):
    calib_params = {}
    extrinsics = extrinsic_camera['transform']
    T_lidar2cam =  transform_matrix(np.array(list(extrinsics['translation'].values())),  
                             Quaternion(list(extrinsics['rotation'].values())), inverse=False)
    w, h = intrinsics['resolution']
    K = np.array(intrinsics['K']).reshape(3,3)
    D = np.array(intrinsics['D'])
    dist_mode = intrinsics['distortion_model']
    mapx,mapy = get_map(h, w, K, D, dist_mode)
    calib_params['h'] = h
    calib_params['w'] = w
    calib_params['K'] = K
    calib_params['mapx'] = mapx
    calib_params['mapy'] = mapy
    calib_params['T_lidar2cam'] = T_lidar2cam
    return calib_params



def pcd2cam(pcd, img, cam_calib_params, image_path):
    itensity = pcd[:, 3]
    T_lidar2cam = cam_calib_params['T_lidar2cam'] 
    K_cam = cam_calib_params['K']
    pcd_current = pcd.copy()
    pcd_xyz = (T_lidar2cam[:3,:3]@pcd_current[:, :3].T).T+T_lidar2cam[:3,3]
    pcd_uvd = (K_cam@pcd_xyz.T).T
    pcd_uvd[:,0]/=pcd_uvd[:,2]
    pcd_uvd[:,1]/=pcd_uvd[:,2]
    
    img = draw_points_on_image(img, pcd_uvd, image_path, cam_calib_params['w'], cam_calib_params['h'], itensity)
    return img


def draw_points_on_image(img, points, output_path, w, h, itensity):
    
    # 创建一个空的与原图同样尺寸的掩模，用于标记点的位置
    overlay = img.copy()
    # cv.imwrite(output_path + f'/{name}_origin.jpg', img)
    white_image = np.full((h, w, 3), 255, dtype=np.uint8)
    save_img = img.copy()
    
    # 设置点的颜色和大小
    point_radius = 3

    # 绘制每个点
    for index, point in enumerate(points):
        u = point[0]
        v = point[1]
        if (u < 0 and u > w and v < 0 and v > h) or point[2] < 0.25 or np.isnan(u) or np.isnan(v):
            continue
        sem_data = itensity[index]
        
        if sem_data > 10:
            color = (0, 0, 255)
            # color = (255, 0, 0)
        else:
            continue
            color = (0, 30, 0)
        cv.circle(overlay, (int(u), int(v)), point_radius, color, -1)
        cv.circle(white_image, (int(u), int(v)), point_radius, color, -1)
    
    # 将带有点的掩模与原始图像合并
    alpha = 0.9  # 控制点的透明度
    img = cv.addWeighted(overlay, alpha, save_img, 1 - alpha, 0)
    color = (0, 0, 255)
    
    start_point = (w // 4, h // 4)  # 左上角的点
    end_point = (3 * w // 4, 3 * h// 4)  # 右下角的点

    color = (255, 0, 0)

    # 定义框的线条粗细
    thickness = 2

    # 在图像中间绘制矩形框
    cv.rectangle(img, start_point, end_point, color, thickness)
    
    cv.imwrite(output_path, img)
    return img


def get_pcd_by_nori(data):
    data = io.BytesIO(data)
    data = np.load(data)
    time_min = np.min(data['t'])
    pcd = np.concatenate((
        data['x'].reshape(-1,1),
        data['y'].reshape(-1,1),
        data['z'].reshape(-1,1),
        data['i'].reshape(-1,1)
        ),
        axis=1
    )
    return pcd, time_min


if __name__ == '__main__':
    import time 
    t1 = time.time()
    resources_path = "/data/oss_visualization_fastapi/test/resources.json"
    with open(resources_path) as f:
        origin_data = json.loads(f.read())
    
    resources = origin_data["resources"]
    calibrated_sensors = origin_data["calibrated_sensors"]
    resources_map = {resource["key"]:resource for resource in resources if resource.get("key")}

    fuser_lidar_url = resources_map["fuser_lidar"]["oss_url"].replace("https://rigel-dev.iap.hh-d.brainpp.cn", "http://localhost:5001")
    response = requests.get(fuser_lidar_url)
    fuser_lidar = response.content
    
    
    nori_id = "116441491,100079f19670"
    fuser_lidar = nori2.Fetcher().get(nori_id)
    
    pcd, time_min = get_pcd_by_nori(fuser_lidar)
    
    for index, resource in enumerate(resources):
        ext = resource["ext"]
        if ext != "jpg":
            continue 
        oss_url = resource["oss_url"].replace("https://rigel-dev.iap.hh-d.brainpp.cn", "http://localhost:5001")
        meta = resource["meta"]
        cam = list(meta.keys())[0]
        
        extrinsic_camera = calibrated_sensors[cam]["extrinsic"]
        intrinsics = calibrated_sensors[cam]["intrinsic"]
        cam_calib_params = get_camera_caliresult(extrinsic_camera, intrinsics)
        
        response = requests.get(oss_url)
        image = Image.open(BytesIO(response.content)) 
        image_array = np.array(image)
        image_path = f"/data/oss_visualization_fastapi/test/data/{cam}.jpg"
        img = pcd2cam(pcd, image_array, cam_calib_params, image_path)
    
    t2 = time.time()
    print(t2 - t1)