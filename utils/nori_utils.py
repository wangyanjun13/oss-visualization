import datetime
import os
import refile
import nori2 as nori
from loguru import logger
import time

from .dao import ParsedRosBagDAO
from pymongo import MongoClient
import socket
import pymongo
from loguru import logger
import time

def make_nori_path(parsed_rosbag_id, split_name, message_type):
    """根据parsed_rosbag_id, data_type 和 message_type 生成对应的nori存储路径
    生成规则见: https://wiki.megvii-inc.com/pages/viewpage.action?pageId=393026590
    第六层: 生成数据的版本和时间 => dag_run_time + parsed_rosbag_id
    """
    # assert message_type in ("image", "pointcloud", "radar")

    localtime = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    if message_type.startswith("/"):
        message_type = message_type[1:]

    if split_name == "UnitTest":
        bucket = f"s3://tf-rhea-ci-data"
        return f"{bucket}/3_0_unit_test/{localtime[:4]}/{localtime}/{message_type}.nori"
    parsed_rosbag = ParsedRosBagDAO.read_one(parsed_rosbag_id)
    if parsed_rosbag.dag_run_id == "__UnitTest__":
        bucket = f"s3://tf-rhea-ci-data"
        return f"{bucket}/3_0_unit_test/{localtime[:4]}/{localtime}/{message_type}.nori"
    parse_path = parsed_rosbag.parse_path
    nori_path = refile.smart_path_join(parse_path, "all_nori", f"{message_type}.nori")
    return nori_path


def speed_all_fuser_lidar_old(parsed_rosbag_id):
    parsed_rosbag = ParsedRosBagDAO.read_one(parsed_rosbag_id)
    parse_path = parsed_rosbag.parse_path
    fuser_lidar_path = refile.smart_path_join(parse_path, "fuser_lidar")

    if refile.smart_exists(fuser_lidar_path):
        for root, dirs, files in refile.smart_walk(fuser_lidar_path):
            if root.endswith(".nori"):
                with nori.open(root) as f:
                    nori.speedup.on(f)


def speedup(nori_path):

    logger.warning("aliyun can not nori speedup")
    return

    if not refile.smart_exists(nori_path):
        logger.error("nori path {} not exist".format(nori_path))
    try:
        with nori.open(nori_path) as f:
            nori.speedup.on(f, timeout=60 * 60, replica=1)
    except:
        try:
            vids_all = nori.utils.search_volumes_v2(nori_path, 2)

            size = len(vids_all)
            interval = 1000

            for start in range(0, size, interval):
                end = min(start + interval, size)
                with nori.NoriReader(files=[nori_path], vids=vids_all[start:end]) as f:
                    nori.speedup.on(f, timeout=60 * 60, replica=1)
        except Exception as e:
            logger.error("nori speedup {} failed, {}".format(nori_path, e))


def make_parse_path(oss_path: str, car_id: str, collect_type: str, oss_output_path: str = None):
    if collect_type == "UnitTest":
        return
    bagname = os.path.basename(oss_path[:-1])
    collect_date = bagname.split("_")[-2]
    try:
        bucket_type = os.getenv("RHEA_OSS_BUCKET_TYPE", "dev")
        if bucket_type == "prod":
            # 绝大多数情况是能解析出来的
            year = collect_date[2:4]
            quater = (int(collect_date[4:6]) + 2) // 3
            bucket = f"s3://tf-rhea-data-new"
        elif bucket_type == "e2e":
            # 绝大多数情况是能解析出来的
            year = collect_date[2:4]
            quater = (int(collect_date[4:6]) + 2) // 3
            bucket = f"s3://e2e-rhea-data"
        else:
            bucket = f"s3://tf-rhea-ci-data"
    except:
        bucket = f"s3://tf-rhea-ci-data"

    if oss_output_path:
        if oss_output_path.endswith("/"):
            oss_output_path = oss_output_path[:-1]
        base_path = oss_output_path
    else:
        base_path = f"{bucket}/parsed_data/car_{car_id}/{collect_date}/{bagname}_{collect_type}"

    washing_time = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    num = 0
    if refile.smart_exists(base_path):
        num = len(refile.smart_listdir(base_path))
    parse_path = f"{base_path}/v{num}_{washing_time}/"
    return parse_path


def nori_path_replace(s3_path, prefix="s3://"):
    return s3_path.replace("s3://", prefix)


global_nori = {}


class nori_open:
    def __init__(self, s3_path: "str", mode="r"):
        nori_path = s3_path
        if "tf-rhea-data/" in nori_path and refile.smart_exists(nori_path.replace("tf-rhea-data/", "tf-rhea-data-new/")):
            nori_path = nori_path.replace("tf-rhea-data/", "tf-rhea-data-new/")
        self.s3_path = nori_path
        self.mode = mode

    def __enter__(self):
        global global_nori
        if self.s3_path not in global_nori:
            print(f"Create {self.s3_path}")
            a = time.time()
            global_nori[self.s3_path] = nori.open(self.s3_path, self.mode)
            b = time.time()
            logger.info(f"open nori {self.s3_path} cost: {b-a}")
        return global_nori[self.s3_path]

    def __exit__(
        self,
        exc_type: "Type[BaseException] | None",
        exc: "BaseException | None",
        tb: "TracebackType | None",
    ):
        return
    
    def close(s3_path):
        nori_path = s3_path
        if "tf-rhea-data/" in nori_path and refile.smart_exists(nori_path.replace("tf-rhea-data/", "tf-rhea-data-new/")):
            nori_path = nori_path.replace("tf-rhea-data/", "tf-rhea-data-new/")
        s3_path = nori_path
        if s3_path not in global_nori:
            return
        global_nori[s3_path].close()


def clear_nori_temp():
    global global_nori
    for v in global_nori.values():
        v.close()
    global_nori = {}


def clear_nori_temp_by_ppl(ppl):
    global global_nori
    cur_keys = list(global_nori.keys())
    for s3_path in cur_keys:
        if ppl in s3_path:
            print(f'del nori {s3_path}')
            global_nori[s3_path].close()
            del global_nori[s3_path]
            

def get_gpu_count():
    import pynvml, random

    pynvml.nvmlInit()
    gpu_count = pynvml.nvmlDeviceGetCount()
    return gpu_count


def get_gpu_index():
    import pynvml, random

    pynvml.nvmlInit()
    gpu_count = pynvml.nvmlDeviceGetCount()
    result_index = random.randint(0, gpu_count - 1)
    for index in range(gpu_count):
        device_handle = pynvml.nvmlDeviceGetHandleByIndex(index)
        process = pynvml.nvmlDeviceGetComputeRunningProcesses(device_handle)

        if len(process) == 0:
            result_index = index
            break
    logger.info(f"Use gpu: {result_index}/{gpu_count}")
    return result_index


class gpu_allocate:
    def __init__(self):
        self.gpu_id = None
        self.gpu_index = None
        self.db = "test_database"
        self.collection = "node_gpu"
        self.rhea_mongodb_url = os.environ['RHEA_MONGODB_URL']
        self.client = MongoClient(self.rhea_mongodb_url)
        self.node_collection = self.client[self.db][self.collection]
        
    def __enter__(self):
        try:
            retry = 8
            while retry > 0:
                self.gpu_index = get_gpu_index()
                hostname = socket.gethostname()
                try_list = [f"{hostname}_{self.gpu_index}", f"{hostname}_{self.gpu_index}_1"]
                
                allocated = False
                for gpu_id_item in try_list:
                    self.gpu_id = gpu_id_item
                    try:
                        find = self.node_collection.insert_one({
                            "_id": self.gpu_id,
                            "create_time": datetime.datetime.now()
                        })
                        logger.info(f"Allocate gpu {self.gpu_id} success")
                        allocated = True
                        break
                    except pymongo.errors.DuplicateKeyError as e:
                        logger.warning(f"Allocate {self.gpu_id} error: {e}")
                        time.sleep(3)
                if allocated: 
                    break
                retry -= 1
            if retry <= 0:
                self.gpu_index = None
                self.gpu_id = None
                
        except Exception as e:
            logger.warning(f"Allocate error: {e}")
            self.gpu_index = None
            self.gpu_id = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.gpu_id != None:
                self.node_collection.delete_one({"_id": self.gpu_id})
            logger.info(f"Release gpu {self.gpu_id} success")
        except Exception as e:
            logger.error(f"Release gpu {self.gpu_id} error: {e}")
        finally:
            if self.client != None:
                self.client.close()

if __name__ == "__main__":
    parsed_ros_bag_id = "638714f1569909ab5df019c4"

    nori_path = make_nori_path(parsed_ros_bag_id, "aaa", "image")
    print(nori_path)
