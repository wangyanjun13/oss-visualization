import subprocess
import os 
from datetime import datetime
os.system("eval $(curl -s http://deploy.i.brainpp.cn/httpproxy)")

import tqdm

def exec_command(cmd):
    PROXIES = {
        "all_proxy": "httpproxy-headless-mc-upload.kubebrain.svc.hh-d.brainpp.local:3128",
        "http_proxy": "httpproxy-headless-mc-upload.kubebrain.svc.hh-d.brainpp.local:3128",
        "https_proxy": "httpproxy-headless-mc-upload.kubebrain.svc.hh-d.brainpp.local:3128",
    }

    env = {
        **PROXIES
    }
   
    label_cmd = [cmd, ]
    for cmd in label_cmd:
        handler = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, close_fds=True, env=env)
        handler.wait()
        if handler.returncode != 0:
            raise Exception(f'{cmd}失败')
        print(f'{cmd}')

if __name__ == '__main__':
    # sync 会覆盖已有文件，copy不会覆盖
    # 车道线回收数据
    # commands = [
    #     "/data/axyh/rclone/rclone --config /data/axyh/rclone/rclone.conf copy -P bpp-oss://xyh/data/Lane_Line/data/recycle/20240903/ aliyun-oss://broadside-dlf/ods/ods_rc_meta_frame_df/date_sk=20240817/ --transfers 64 --checkers 32 --s3-chunk-size 128M"
    # ]
    # 车道线训练数据
    # commands = [
    #     "/data/axyh/rclone/rclone --config /data/axyh/rclone/rclone.conf copy -P bpp-oss://xyh/data/Lane_Line/data/train/frame/ aliyun-oss://broadside-dlf/ods/ods_tr_meta_frame_df/date_sk=20240817/ --transfers 64 --checkers 32 --s3-chunk-size 128M"
    # ]

    # 回收预览图
    # commands = [
    #     "/data/axyh/rclone/rclone --config /data/axyh/rclone/rclone.conf copy -P bpp-oss://xyh/data/Lane_Line/data/recycle/preview/ aliyun-oss://broadside-transform/prod/preview/laneline/recycle/ --transfers 64 --checkers 32 --s3-chunk-size 128M"
    # ]


    # 训练预览图
    # commands = [
    #     "/data/axyh/rclone/rclone --config /data/axyh/rclone/rclone.conf copy -P bpp-oss://xyh/data/Lane_Line/data/train/preview/ aliyun-oss://broadside-transform/prod/preview/laneline/train/ --transfers 64 --checkers 32 --s3-chunk-size 128M"
    # ]

    commands = [
        "/data/oss_visualization_fastapi/rclone --config /data/oss_visualization_fastapi/rclone/rclone.conf copy -P aliyun-oss://tf-rhea-data/parsed_data/car_507/20231113/ppl_bag_20231113_201812_partial_track/v0_231206_014252/all_nori/fuser_lidar.nori  bpp-oss://mc-trained-data/3d_object_visualization/nori/parsed_data/car_507/20231113/ppl_bag_20231113_201812_partial_track/v0_231206_014252/all_nori/fuser_lidar.nori  --transfers 64 --checkers 32 --s3-chunk-size 128M"
    ]


    # bmk数据
    # commands = [
    #     "/data/axyh/rclone/rclone --config /data/axyh/rclone/rclone.conf sync -P bpp-oss://xyh/data/Lane_Line/data/recycle/bmk/ aliyun-oss://broadside-dlf/ods/ods_rc_meta_frame_df/date_sk=20240814/ --transfers 64 --checkers 32 --s3-chunk-size 128M"
    # ]

    for cmd in tqdm.tqdm(commands):
        exec_command(cmd)
        