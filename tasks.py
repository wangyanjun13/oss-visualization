import time
import os
import sys
import traceback
import copy
import tqdm
from concurrent.futures import ProcessPoolExecutor
import redis

from celery import Celery
from pymongo import MongoClient
from loguru import logger

# 使用 get 方法安全地获取环境变量，提供默认值
env = os.environ.get("env", "dev")

# 远程redis连接不到
# is_local = os.environ.get("IS_LOCAL", "true").lower() == "true"

# # 根据环境配置Redis主机
# if is_local:
#     # 本地开发环境使用localhost
#     redis_host = "localhost"
# elif env == "dev":
#     redis_host = "skyfire-broadside-08.group-megvii-transformer-hound.megvii-transformer.svc"
# elif env == "pro":
#     redis_host = "broadside-new-ait.group-megvii-transformer-hound.megvii-transformer.svc"
# else:
#     # 默认使用localhost
#     redis_host = "localhost"

# 在容器内始终使用localhost作为Redis主机
redis_host = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# 测试Redis连接并输出状态
try:
    redis_client = redis.Redis(host=redis_host, port=REDIS_PORT, db=REDIS_DB)
    if redis_client.ping():
        logger.info(f"✅ 成功连接到Redis服务器: {redis_host}:{REDIS_PORT}")
        # 获取并显示所有键
        keys = redis_client.keys('*')
        logger.info(f"找到 {len(keys)} 个键: {[key.decode('utf-8') for key in keys]}")
    else:
        logger.error(f"❌ 无法ping通Redis服务器: {redis_host}:{REDIS_PORT}")
except Exception as e:
    logger.error(f"❌ 连接Redis失败: {str(e)}")

# 创建 Celery 实例，指定默认的消息代理
app = Celery(
    'tasks',
    broker=f'redis://{redis_host}:{REDIS_PORT}/{REDIS_DB}',
    backend=f'redis://{redis_host}:{REDIS_PORT}/{REDIS_DB}'
)

# 配置 Celery (可选)
app.conf.update(
    task_serializer='json',  # 使用 JSON 序列化任务
    result_serializer='json',  # 使用 JSON 序列化结果
    accept_content=['json'],  # 只接受 JSON 格式
)

MONGO_DATABASE_URL = "mongodb://skyfire-broadside-08.group-megvii-transformer-hound.megvii-transformer.svc:27017"
client = MongoClient(MONGO_DATABASE_URL)
db = client['object_3d_data']


def update_one_data(table, query, value, flag=False):
    db[table].update_one(query, {"$set": value}, upsert=flag)


def get_one_frame_resouces(query):
    result = db["3d_object"].find_one(query, {"resources": 1})
    if result:
        return result["resources"]
    else:
        return []


def _fuser_lidar_project_image(all_resources, calibrated_sensors):
    logger.info(f"process fuser lidar project image start")
    task_args = []
    for clip_id, resources in all_resources.items():
        task_args.append((clip_id, resources, calibrated_sensors))

    with ProcessPoolExecutor(max_workers=8) as executor:
        processed_sensors = list(tqdm.tqdm(
            executor.map(pcd2cam.fuser_lidar_project_images, task_args),
            total=len(all_resources),
            desc="Processing project"
        ))
    logger.info(f"process fuser lidar project image end!!!")
    return {item[0]: item[1] for item in processed_sensors}


@app.task
def fuser_lidar_project_image(_id, frames, file_path, calibrated_sensors, sensors, all_resources, is_last):
    # def fuser_lidar_project_image(**kwargs):
    """
    真正的处理逻辑
    """
    # _id = kwargs["_id"]
    # frames = kwargs["frames"]
    # file_path = kwargs["file_path"]
    # calibrated_sensors = kwargs["calibrated_sensors"]
    # sensors = kwargs["sensors"]
    # all_resources = kwargs["all_resources"]
    # is_last = kwargs["is_last"]
    try:
        all_resources = _fuser_lidar_project_image(all_resources, calibrated_sensors)
        for frame in tqdm.tqdm(frames, desc="insert frame"):
            try:
                labels = frame["labels"]
            except:
                labels = None
            frame_id = frame["frame_id"]
            ins_data = frame["ins_data"]
            origin_frame_id = frame["origin_frame_id"]
            preds = frame.get("preds")
            if preds is None:
                preds = []
            pre_labels = frame["pre_labels"]
            roi_info = frame.get("roi_info", {})
            clip_id = frame["clip_id"] + "_" + str(frame["frame_id"])

            resources = all_resources[clip_id]
            final_resouces = []
            final_sensors = set()
            # 更新已经存在的resources
            old_resources = get_one_frame_resouces({"id": _id, "source_path": file_path})
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
                raise Exception(f"error: 加载sensor失败, {set(sensors) - set(final_sensors)}")
            result_dict = {"id": _id, "source_path": file_path, "origin_frame_id": origin_frame_id,
                           "frame_id": frame_id, "data": {"labels": labels, "pre_labels": pre_labels, "preds": preds},
                           "resources": final_resouces, "sensors": final_sensors, "ins_data": ins_data,
                           "roi_info": roi_info, "calibrated_sensors": calibrated_sensors}
            update_one_data("3d_object", {"id": _id, "source_path": file_path}, result_dict, True)
            _id += 1

        if is_last:
            update_one_data("3d_data_source", {"source_path": file_path},
                            {"status": "success", "message": "解析数据成功"})
    except Exception as e:
        traceback.print_exc()
        logger.error(f"analysis file error: {repr(e)}")
        update_one_data("3d_data_source", {"source_path": file_path},
                        {"status": "failed", "message": f"解析失败 {str(e)}"})


@app.task
def test(id, id2):
    time.sleep(1)
    print(f"id:{id}, id2: {id2}")


if __name__ == '__main__':
    for i in range(10):
        test.apply_async(kwargs={"id": i, "id2": i + 1})