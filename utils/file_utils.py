import os
import pickle
import sys
import json
from venv import logger 

import oss2
from starlette.responses import FileResponse

OSS_AK = os.getenv("")
OSS_SK = os.getenv('')
OSS_ENDPOINT = os.getenv('oss_endpoint', '')

if not OSS_AK or not OSS_SK:
    raise Exception("ak/sk is None!")

def read_file(filename, mode='r'):
    """
    读取文件内容
    @filename 文件名
    return string(bin) 若文件不存在，则返回None
    """
    if not os.path.exists(filename):
        return False
    try:
        fp = open(filename, mode)
        content = fp.read()
        fp.close()
    except Exception as ex:
        if sys.version_info[0] != 2:
            try:
                fp = open(filename, mode, encoding="utf-8")
                content = fp.read()
                fp.close()
            except:
                fp = open(filename, mode, encoding="GBK")
                content = fp.read()
                fp.close()
        else:
            return False
    return content


def write_file(filename, content, mode='w+'):
    """
    写入文件内容
    @filename 文件名
    @content 写入的内容
    return bool 若文件不存在则尝试自动创建
    """
    try:
        fp = open(filename, mode)
        fp.write(content)
        fp.close()
        return True
    except:
        try:
            fp = open(filename, mode, encoding="utf-8")
            fp.write(content)
            fp.close()
            return True
        except:
            return False


def clear_file(filename, mode='w+'):
    """
    清空文件内容
    @filename 文件名
    return bool
    """
    try:
        open(filename, mode).close()
        return True
    except:
        try:
            open(filename, mode, encoding="utf-8").close()
            return True
        except:
            return False


def create_xlsx_response(data, filename):
    from pyexcelerate import Workbook
    df = pd.DataFrame(data)
    wb = Workbook()
    wb.new_sheet('Sheet1', data=[df.columns.tolist()] + df.values.tolist())
    wb.save(filename)
    return FileResponse(filename, filename=filename)


# 对文件名非法字符则替换为空
def clean_filename(filename):
    return ''.join(c if c not in '<>:"/\\|?*' and ord(c) >= 32 else '' for c in filename).strip()


# 压缩图片参数
compress_params = {'x-oss-process': 'image/quality,q_70|image/format,avif'}


def get_bucket_path(path):
    # oss://ait-recycle-images/car_light_v2/0.1__101011-101017__142329421,d4d20003cc96802.png
    path = path.split('//', 1)[-1] if '//' in path else path
    # demo:broadside-mir/ods_label_cli_meta_oss_dev/data_sk=240611/308081018772914976,broadside-mir是bucket
    bucket, result = path.split('/', 1)
    return bucket, result


def get_oss_bucket_object(bucket) -> oss2.Bucket:
    # bucket:bucket名称
    return oss2.Bucket(oss2.Auth(OSS_AK, OSS_SK), OSS_ENDPOINT, bucket)


def get_oss_url(path, params={}):
    try:
        if not path: return ''
        bucket, result = get_bucket_path(path)
        print(result)
        if result and result[-1] == "\\":
            result = result[:-1]
        return oss2.Bucket(oss2.Auth(OSS_AK, OSS_SK), OSS_ENDPOINT, bucket).sign_url('GET', result, 3600, params=params)
    except Exception as e:
        print(e)
        return path


# 判断文件路径是否存在
def oss_path_is_exist(path):
    if not path: return False
    # demo:broadside-transform/dev/recycle/4325-1724827703101/traffic_light.json
    bucket, result = get_bucket_path(path)
    bucket_object = get_oss_bucket_object(bucket)
    return bucket_object.object_exists(result)


def test():
    import re
    s = 'broadside-transform/prod/recycle/176636-1724913452656/2292151-1724913452656.json'
    parts = s.rsplit('/', 1)  # 分割字符串，保留最后一个'-'
    print(parts)
    number = parts[-1].split('-')[0]  # 获取最后一个'-'后面的部分，并取第一个'/'前面的内容
    return int(number) if number.isdigit() else None


# 返回oss目录下所有的文件完整路径
def list_dir_oss_path(path):
    bucket, result = get_bucket_path(path)
    bucket_object = get_oss_bucket_object(bucket)
    # 列出指定目录下的所有文件
    files = [f"{bucket}/{obj.key}" for obj in oss2.ObjectIterator(bucket_object, prefix=result)]
    return files


# 文件目录是否存在
def oss_dir_is_exist(path):
    if not path:
        return False
    # demo: broadside-transform/dev/nova/collect/315376412/
    try:
        bucket, result = get_bucket_path(path)
        bucket_object = get_oss_bucket_object(bucket)
        r = bucket_object.list_objects(prefix=result, max_keys=1)
        if r.object_list:
            return True
    except Exception as e:
        print(e)

    return False


# 获取文件内容
def get_file_content(oss_path: str):
    bucket, result = get_bucket_path(oss_path)
    object_result = get_oss_bucket_object(bucket).get_object(result)
    content = object_result.read()
    try:
        if oss_path.endswith(".json"):
            return json.loads(content)
        elif oss_path.endswith(".pkl"):
            return pickle.loads(content)
        
    except Exception as e:
        raise Exception(f"get file content error: {repr(e)}")

if __name__ == '__main__':
    oss_path = "s3://e2e-rhea-data/prelabeled_data/car_z07/20241201_dp-track/ppl_bag_20241201_195516_det/v0_241204_155906/splited_video_prelabels_tracking/0000.json"

    # bucket, result = get_bucket_path(oss_path)
    # object_result = get_oss_bucket_object(bucket).get_object(result)
    # content = object_result.read()
    # print(content)
    oss_path = "s3://e2e-rhea-data/parsed_data/car_11/20241111/ppl_bag_20241111_225447_det/v0_241115_023429/all_nori/pointcloud.nori"

    print(oss_dir_is_exist(oss_path))