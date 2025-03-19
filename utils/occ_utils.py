from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import pickle 
import time 
import os 
import json 

import tqdm
import refile

from utils import file_utils
from config import RADAR_DIR


roi = [
        -15.2, -27.2, -5.0, 
         15.2,  81.6,  3.0
    ]

roi = [i*10 for i in roi]

def multi_thread_compress_occ_data(pkl_paths, max_workers=5):
    path_to_result = {}
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_pkl_path = {
            executor.submit(compress_occ_data, pkl_path): pkl_path
            for pkl_path in pkl_paths
        }
        
        for future in tqdm.tqdm(as_completed(future_to_pkl_path), desc="process pkl_path", total=len(pkl_paths)):
            pkl_path = future_to_pkl_path[future]  # 获取对应的 pkl_path
            try:
                result = future.result()
                # 将 pkl_path 和 result 关联起来
                path_to_result[pkl_path] = result
            except Exception as e:
                # 处理异常
                print(f"Error processing {pkl_path}: {e}")
                path_to_result[pkl_path] = None  # 标记失败的任务
    
    return path_to_result
        

def compress_occ_data(pkl_path):
    data = file_utils.get_file_content(pkl_path)
    target = data["target"].tolist()
    
    center_points_with_values = process_data(target)
    
    file_name = str(time.time())
    oss_path = os.path.join(RADAR_DIR, file_name+".json")
    with refile.smart_open(oss_path, "w") as f:
        f.write(json.dumps(center_points_with_values))
    
    return file_name



def process_data(data):
    center_points_with_values = []

    # 解构 ROI
    min_x, min_y, min_z, max_x, max_y, max_z = roi

    # 计算每个维度的步长
    step_x = (max_x - min_x) / len(data)
    step_y = (max_y - min_y) / len(data[0])
    step_z = (max_z - min_z) / len(data[0][0])

    # 遍历三维数组
    for x in range(len(data)):
        for y in range(len(data[x])):
            for z in range(len(data[x][y])):
                value = data[x][y][z]
                if value != 0:
                    # 计算当前立方体的中心点（映射到 ROI 空间）
                    center_x = min_x + (x + 0.5) * step_x  # x 中心点
                    center_y = min_y + (y + 0.5) * step_y  # y 中心点
                    center_z = min_z + (z + 0.5) * step_z  # z 中心点

                    # 检查中心点是否在 ROI 内
                    if (
                        min_x <= center_x <= max_x
                        and min_y <= center_y <= max_y
                        and min_z <= center_z <= max_z
                    ):
                        # 存储中心点及对应的值
                        center_points_with_values.append(
                            [center_x / 10, center_y / 10, center_z / 10, value]
                        )

    return center_points_with_values
