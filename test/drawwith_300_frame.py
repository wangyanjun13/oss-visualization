import copy
import json
import os 
import refile 


def drawwith_frames():
    with refile.smart_open(oss_path) as f:
        json_data = json.loads(f.read())
    
    roi_data = copy.deepcopy(json_data)
    roi_data["frames"] = json_data["frames"][120:130]
    
    with refile.smart_open(dest_path, "w") as f:
        f.write(json.dumps(roi_data))

oss_path = "s3://tf-23q3-shared-data/labeled_data/car_9/20230901_dp-det_yueying_checked/ppl_bag_20230901_161016_det/v0_230911_131756/0004.json"
dest_path = "s3://zhailipu-data/test/oss_vis/test_frame_20241225_100_102.json"
dest_path = "s3://zhailipu-data/test/oss_vis/test_frame_20241225_103_105.json"
dest_path = "s3://zhailipu-data/test/oss_vis/test_frame_20241225_110_120.json"
dest_path = "s3://zhailipu-data/test/oss_vis/test_frame_20241225_120_130.json"
drawwith_frames()