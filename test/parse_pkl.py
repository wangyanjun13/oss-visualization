import json 
import refile 
import pickle


file_path = "s3://megsim/task-plan/19907/1541/car_504__reinjection__20231119__093140__260993/evaluate_result/fusion/tfboard_ppl_bag_20231119_092139_20_record_recovered_cipv_static.pkl"

if file_path.endswith(".pkl"):
    trans_matrix = {}
    intrinsic = {}
    try:
        with refile.smart_open(file_path, "rb") as f:
            data = pickle.load(f)
    except Exception as e:
        pass 

        
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
