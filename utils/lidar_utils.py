import numpy as np

from utils.dao import ParsedRosBagDAO
from utils.naco import load_nacos_json_config


def get_car_sensor_config(car_id: str):
    nacos_sensor_config = load_nacos_json_config("sensor_config", "CALIB_SENSORS")
    car_config = nacos_sensor_config.get(str(car_id))
    if car_config == None:
        raise Exception(f"Car {car_id} not supported!")
    
    return car_config


def get_standard_lidar_by_car_id(car_id: str):
    car_config = get_car_sensor_config(car_id)
    standard_lidar = car_config["standard_lidar"]

    return standard_lidar


def get_fisheye_and_others_cam(car_id: str, parsed_rosbag_id):
    car_config = get_car_sensor_config(car_id)
    cam_index_list = car_config["camera_index_list"]
    parsed_ros_bag = ParsedRosBagDAO.read_one(parsed_rosbag_id)
    calibrated_sensors = parsed_ros_bag.calibrated_sensors

    fisheye_cams = []
    others_cams = []
    if len(calibrated_sensors) == 0:
        raise Exception("Calibrated sensors config not exists!!!")
    
    for cam_index in cam_index_list:
        cam_name = "cam_" + cam_index
        distortion_model = calibrated_sensors[cam_name]["intrinsic"]["distortion_model"]
        if distortion_model == "fisheye":
            fisheye_cams.append(cam_name)
        else:
            others_cams.append(cam_name)

    return fisheye_cams, others_cams


# 泊车专用， 顺序有影响，最前是主lidar，后面是需要融合进主lidar的lidar
def get_lidar_keys_by_car_id(car_id):
    if car_id in [1, 2]:
        return ["middle_lidar", "left_lidar", "right_lidar"]
    elif car_id in [4]:
        return ["front_lidar", "left_lidar", "back_lidar", "right_lidar", "left_down_lidar", "right_down_lidar"]
    elif car_id in [3]:
        return ["front_lidar", "left_lidar", "right_lidar", "back_lidar"]
    elif car_id in [
        5,
        7,
        8,
        9,
        10,
        11,
        13,
        14,
        15,
        201,
        202,
        501,
        502,
        503,
        504,
        505,
        506,
        507,
        508,
        509,
        510,
        511,
        512,
        513,
        515,
        516,
        517,
        518,
        519,
    ]:
        return ["front_lidar", "left_lidar", "right_lidar", "back_lidar", "left_down_lidar", "right_down_lidar"]
    elif car_id in [12]:
        return ["front_lidar", "left_lidar", "right_lidar", "back_lidar"]
    elif car_id in [6]:
        return ["front_lidar", "back_lidar"]
    elif car_id in [101]:
        return ["front_lidar"]
    elif car_id in [102]:
        return ["middle_lidar"]
    raise Exception(f"car_id: {car_id} not supported")


def get_lidar_list_by_car_id(car_id: str):
    car_config = get_car_sensor_config(car_id)
    lidar_types = car_config["lidar_list"]

    return lidar_types


def filter_fuser_lidar_by_lidar_ids(fuser_lidar, lidar_ids):
    cond = np.zeros(fuser_lidar.shape, dtype=np.bool)
    for lidar_id in lidar_ids:
        cond = cond | (fuser_lidar["lidar_id"] == lidar_id)
    return fuser_lidar[cond]
