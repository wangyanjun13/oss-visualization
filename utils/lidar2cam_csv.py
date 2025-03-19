import os
import refile
import pandas as pd
import numpy as np
import io
import nori2 as nori
# from pypcd4 import PointCloud
from loguru import logger
import json
from pyquaternion import Quaternion
import cv2 as cv
from tqdm import tqdm
import argparse
import subprocess

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

def get_map(h, w, K, D, mode):
    K = np.array(K).reshape(3, 3)
    D = np.array(D)
    if mode == 'pinhole':
        mapx, mapy = cv.initUndistortRectifyMap(K, D, None, K, (w, h), 5)
    elif mode == 'fisheye':
        mapx, mapy = cv.fisheye.initUndistortRectifyMap(K, D, None, K, (w, h), 5)
    return mapx, mapy

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

def get_img_by_nori(img, mapx, mapy):
    img = cv.imdecode(np.frombuffer(img, np.uint8), cv.IMREAD_COLOR)
    img = cv.remap(img, mapx, mapy, cv.INTER_LINEAR)
    return img

def get_lidar_caliresult(front_lidar):
    calib_params = {}
    lidar_path = os.path.join(oss_path, f'calibresult/lidar_params/{front_lidar}.json')
    with refile.smart_open(lidar_path, 'r') as f:
        front_lidar = json.load(f)
    extrinsics = front_lidar['transform']
    T_lidar2vehicle =  transform_matrix(np.array(list(extrinsics['translation'].values())),  
                             Quaternion(list(extrinsics['rotation'].values())), inverse=False)
    calib_params['T_lidar2vehicle'] = T_lidar2vehicle
    return calib_params

def draw_points_on_image(img, points, output_path, w, h, name, itensity):
    
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
    
    if refile.smart_exists(output_path) == False:
        os.makedirs(output_path)
    output_path = os.path.join(output_path, name + '.jpg')
    cv.imwrite(output_path, img)
    return img


def pcd2cam(pcd, img, cam_calib_params, index, cam, root_dump_path):
    itensity = pcd[:, 3]
    T_lidar2cam = cam_calib_params['T_lidar2cam'] 
    K_cam = cam_calib_params['K']
    pcd_current = pcd.copy()
    pcd_xyz = (T_lidar2cam[:3,:3]@pcd_current[:, :3].T).T+T_lidar2cam[:3,3]
    pcd_uvd = (K_cam@pcd_xyz.T).T
    pcd_uvd[:,0]/=pcd_uvd[:,2]
    pcd_uvd[:,1]/=pcd_uvd[:,2]
    
    img = draw_points_on_image(img, pcd_uvd, f'{root_dump_path}/{cam}', cam_calib_params['w'], cam_calib_params['h'], str(index), itensity)
    return img


def pcd2cam_frame(index, csv, camera_dict, camera_list, nr_lidar, nr_cam, root_dump_path):
    if csv["fuser_lidar"][index] != "NAN":
        lidar = nr_lidar.get(csv['fuser_lidar'][index].split('#')[-1])
        pcd, time_min = get_pcd_by_nori(lidar)
        # pc = PointCloud.from_xyzi_points(pcd)
        # pc.save(f'output/{index}.pcd')
        logger.info(f'start  {index} frame!')
        for cam in camera_list:
            camere_nori = csv[f'#sensor#{cam}#compressed'][index].split('#')[-1]
            img = nr_cam.get(camere_nori)
            img = get_img_by_nori(img, camera_dict[cam]['cam_calib_params']['mapx'], camera_dict[cam]['cam_calib_params']['mapy'])
            img = pcd2cam(pcd, img, camera_dict[cam]['cam_calib_params'], index, cam, root_dump_path)
        logger.success(f'{index} done')
        return index
    
def extract_number(filename):
    # 使用正则表达式找到数字部分
    match = os.path.basename(filename).split('.')[0]
    if match:
        return int(match)
    return 0 


def save_avi(frames_folder):
    for dir in tqdm(list(os.listdir(frames_folder))):
        if '.avi' in dir or '.pcd' in dir:
            continue
        # 获取图片列表
        image_files = [os.path.join(frames_folder, dir, f) for f in os.listdir(os.path.join(frames_folder, dir)) if f.endswith('.jpg')]
        image_files = sorted(image_files, key=extract_number)  # 确保图片按顺序排列

        # 读取第一张图片以获取尺寸和类型
        first_frame = cv.imread(image_files[0])
        frame_height, frame_width = first_frame.shape[:2]
        video_output_path = f'{frames_folder}/{dir}.mp4'
        fps = 10  # 你可以根据需要设置帧率
        ffmpeg_cmd = [
            'ffmpeg',
            '-y',                   # 覆盖输出文件
            '-f', 'image2pipe',     # 指定输入格式为图片序列
            '-analyzeduration', '100M',  # 增加分析持续时间
            '-probesize', '100M',        # 增加探测大小
            '-r', str(fps),  # 设置帧率
            '-i', '-',              # 从管道读取图片数据
            '-vcodec', 'libx264',   # 使用H.264编码
            '-pix_fmt', 'yuv420p',  # 设置像素格式
            '-crf', '20',           # 设置CRF值
            '-preset', 'ultrafast', # 设置编码预设为ultrafast
            video_output_path               # 输出视频文件
        ]
        ffmpeg_process = subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=10**8)
        # 将图片写入视频
        last_index = -1
        for image_file in image_files:
            index = int(os.path.basename(image_file).split('.')[0])
            if last_index == -1 or index - last_index == 1:
                frame = cv.imread(image_file)
                _, buffer = cv.imencode('.jpg', frame)
                if frame is not None:
                    # out.write(frame)
                    ffmpeg_process.stdin.write(buffer)
                else:
                    print(f"Warning: Could not read image {image_file}")
        last_index = index
        ffmpeg_process.stdin.close()
        ffmpeg_process.wait()
        # refile.smart_copy(video_output_path, f'{oss_path}pcd2cam/{dir}.mp4')
        print("Video has been saved successfully.")
    # refile.smart_remove(frames_folder)
    
def parse_range(value):
    # 解析类似 '1,10' 的输入为 range
    if ',' in value:
        return list(map(int, value.split(',')))
    else:
        raise argparse.ArgumentTypeError("Invalid range or list format")
    
def parse_range_str(value):
    # 解析类似 '1,10' 的输入为 range
    if ',' in value:
        return list(map(str, value.split(',')))
    else:
        return [value]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", "-p", type=str, required=False, help="解包路径", default="s3://e2e-rhea-data/parsed_data/car_15/20241009/ppl_bag_20241009_085159_det/v0_241009_171923/")
    parser.add_argument("--range", "-r", type=parse_range, help="帧数区间，输入参考：--range 1000,3000,5 代表从1000到3000，步长为5进行可视化", default=[0, 100, 10])
    parser.add_argument("--output", "-o", type=str, help="输出路径", default="output")
    parser.add_argument("--camera", "-c", type=parse_range_str, help="可视化的相机list", default=[])
    
    args = parser.parse_args()
    range_list = args.range
    root_dump_path = args.output
    oss_path = args.path
    camera_list = args.camera
    
    logger.info(args)
    
    if not refile.smart_exists(root_dump_path):
        os.mkdir(root_dump_path)
    
    csv_path = os.path.join(oss_path, 'timestamp_align_merged_v2.csv')
    camera_nori_path = os.path.join(oss_path, 'all_nori', 'image.nori')
    
    fuser_lidar_path = os.path.join(oss_path, 'all_nori', 'fuser_lidar.nori')

    with refile.smart_open(csv_path, 'r') as f:
        csv = pd.read_csv(f)
    
    cam_list = refile.smart_glob(oss_path +  "all_db3_data/camera_data/*.txt")
    
    if len(camera_list) == 0:
        for cam in cam_list:
            camera_list.append(os.path.basename(cam).split('.')[0].split('#')[2])

    camera_dict = {}
    logger.info('start load camera params')
    for cam in camera_list:
        extrinsic_path = os.path.join(oss_path, 'calibresult/camera_params/', cam + '_extrinsic.json')
        intrinsics_path = os.path.join(oss_path, 'calibresult/camera_params/', cam + '_intrinsic.json')
        camere_timestamp_path = os.path.join(oss_path, 'all_db3_data/camera_data/', f'#sensor#{cam}#compressed.txt')
        with refile.smart_open(intrinsics_path, 'r') as f:
            intrinsics = json.load(f)
        with refile.smart_open(extrinsic_path, 'r') as f:
            extrinsic_camera = json.load(f)
        camera_dict[cam] = {
            'cam_calib_params': get_camera_caliresult(extrinsic_camera, intrinsics)
        }
        
    import multiprocessing
    logger.info('start pcd2cam')
    with nori.open(fuser_lidar_path, "r") as nr_lidar:
        with nori.open(camera_nori_path, "r") as nr_cam:
            with multiprocessing.Pool(processes=os.cpu_count() - 4) as pool:
                pool.starmap(
                    pcd2cam_frame,
                    [(index, csv, camera_dict, camera_list, nr_lidar, nr_cam, root_dump_path) for index in range(*range_list)]    
                )
    # save_avi(root_dump_path)
    

