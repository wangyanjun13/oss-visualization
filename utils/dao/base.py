import asyncio
from dataclasses import dataclass
import datetime
import os
import subprocess
from typing import Any, Dict, List, Tuple, Union

from bson.objectid import ObjectId
import motor
import motor.motor_asyncio

# import git
# from pathlib import Path

COMMIT_ID = None


def get_commit_id():
    try:
        COMMIT_ID = subprocess.check_output(["git", "rev-parse", "HEAD"], encoding="utf-8").strip()
        print(f"Current commit ID: {COMMIT_ID}")
        return COMMIT_ID
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while getting commit ID: {e}")


COMMIT_ID = get_commit_id()
# try:
#     repo = git.Repo(Path(__file__).parents[2])
#     COMMIT_ID = repo.head.commit.hexsha
# except:
#     raise Exception("Fail to get repo's commit_id")

# Please export RHEA_MONGODB_URL=mongodb://{your_local_ip}:27017 or somewhat like that.
RHEA_MONGODB_URL = os.getenv("RHEA_MONGODB_URL")
client = motor.motor_asyncio.AsyncIOMotorClient(RHEA_MONGODB_URL)
mongo_motor_loop = client.get_io_loop()
RHEA_MONGODB_DATASET = os.getenv("RHEA_MONGODB_DATASET")
if RHEA_MONGODB_DATASET == "rhea":
    db = client.rhea_database
elif RHEA_MONGODB_DATASET == "e171":
    db = client.e171_database
elif RHEA_MONGODB_DATASET == "E2E":
    db = client.e2e_database
else:
    db = client.test_database

# TODO(chenlei): 写入时做检查, 是否满足数据结构(会影响效率?)
class RheaDAOBase(object):
    _collection = None
    _dataclass = None

    @classmethod
    def convert_document_to_dataclass(cls, document):
        document["_id"] = str(document["_id"])
        common_keys = ["write_time", "update_time", "git_commit"]
        common_values = {}
        for key in common_keys:
            common_values[key] = document.pop(key, None)
        obj = cls._dataclass(**document)
        for key in common_keys:
            obj.__setattr__(key, common_values[key])

        return obj

    @classmethod
    async def do_insert_one(cls, data):
        result = await cls._collection.insert_one(data)
        return result

    @classmethod
    async def do_insert_many(cls, many_data):
        result = await cls._collection.insert_many(many_data)
        return result

    @classmethod
    async def do_find_one_by_id(cls, id_):
        result = await cls._collection.find_one({"_id": ObjectId(id_)})
        return result

    @classmethod
    async def do_find_one_by_condition(cls, condition):
        result = await cls._collection.find_one(condition)
        if result is None:
            return result
        else:
            return cls.convert_document_to_dataclass(result)

    @classmethod
    async def do_query_by_condition(cls, condition, length=None):
        cursor = cls._collection.find(condition)
        result = [cls.convert_document_to_dataclass(document) for document in await cursor.to_list(length=length)]
        return result

    @classmethod
    async def do_query_by_condition_and_sort(cls, condition, sort, inverse, length=None):
        cursor = cls._collection.find(condition, sort=[(key, 1 if not inverse else -1) for key in sort])
        result = [cls.convert_document_to_dataclass(document) for document in await cursor.to_list(length=length)]
        return result

    @classmethod
    async def do_query_by_condition_and_skip(cls, condition, skip, length=None):
        cursor = cls._collection.find(condition, skip=skip)
        result = [cls.convert_document_to_dataclass(document) for document in await cursor.to_list(length=length)]
        return result

    @classmethod
    async def do_count_by_condition(cls, condition):
        n = await cls._collection.count_documents(condition)
        return n

    @classmethod
    async def do_delete_one_by_id(cls, id_):
        result = await cls._collection.delete_one({"_id": ObjectId(id_)})
        return result

    @classmethod
    async def do_delete_many_by_condition(cls, condition):
        result = await cls._collection.delete_many(condition)
        return result

    @classmethod
    async def do_update_one_by_id(cls, id_, data):
        result = await cls._collection.update_one({"_id": ObjectId(id_)}, {"$set": data})
        return result

    @classmethod
    async def do_update_many_by_conditon(cls, conditon, set):
        result = await cls._collection.update_many(conditon, set)
        return result

    @classmethod
    def read_one(cls, id_: str) -> Union[None, _dataclass]:
        """用一个 _id 来读取对应的 document, 返回一个数据对象; 如果不存在, 返回None"""
        result = mongo_motor_loop.run_until_complete(cls.do_find_one_by_id(id_))
        if result is not None:
            return cls.convert_document_to_dataclass(result)
        else:
            return None

    @classmethod
    async def async_read_one(cls, id_: str) -> Union[None, _dataclass]:
        """用一个 _id 来读取对应的 document, 返回一个数据对象; 如果不存在, 返回None"""
        result = await cls.do_find_one_by_id(id_)
        if result is not None:
            return cls.convert_document_to_dataclass(result)
        else:
            return None

    @classmethod
    def read_many(cls, ids: List[str]) -> List[_dataclass]:
        """用若干个 _id 来读取对应的 documents, 返回数据对象列表"""
        data_objs = []
        for id_ in ids:
            data_obj = cls.read_one(id_)
            data_objs.append(data_obj)
        return data_objs

    @classmethod
    def read_by_condition(cls, condition: Dict, length=None) -> List[_dataclass]:
        """用条件读取时，返回所有满足条件的 document(s) 对应的数据对象列表"""
        result = mongo_motor_loop.run_until_complete(cls.do_query_by_condition(condition, length))
        return result

    @classmethod
    def read_by_condition_and_sort(cls, condition: Dict, sort: List[str], inverse: bool = False) -> List[_dataclass]:
        """用条件读取时，返回所有满足条件的 document(s) 对应的数据对象列表，并且按照给定的 key 排序"""
        result = mongo_motor_loop.run_until_complete(cls.do_query_by_condition_and_sort(condition, sort, inverse))
        return result

    @classmethod
    def read_by_condition_and_skip(cls, condition: Dict, skip: int) -> List[_dataclass]:
        """用条件读取时，返回所有满足条件的 document(s) 对应的数据对象列表，并且按照给定的 key 排序"""
        result = mongo_motor_loop.run_until_complete(cls.do_query_by_condition_and_skip(condition, skip))
        return result

    @classmethod
    def read_first_by_condition(cls, condition: Dict) -> Union[None, _dataclass]:
        """用条件读取时，返回第一个满足条件的 document 对应的数据对象"""
        result = mongo_motor_loop.run_until_complete(cls.do_find_one_by_condition(condition))
        return result

    @classmethod
    def write_one(cls, data: Any) -> Tuple[bool, str]:
        """向 DAO 对应的 collection 写入一个 document"""
        write_time = datetime.datetime.now()
        data["write_time"] = write_time
        data["update_time"] = write_time
        data["git_commit"] = COMMIT_ID
        result = mongo_motor_loop.run_until_complete(cls.do_insert_one(data))
        success = result.acknowledged
        inserted_id = str(result.inserted_id) if success else ""
        return (success, inserted_id)

    @classmethod
    async def async_write_one(cls, data: Any) -> Tuple[bool, str]:
        """向 DAO 对应的 collection 写入一个 document"""
        write_time = datetime.datetime.now()
        data["write_time"] = write_time
        data["update_time"] = write_time
        data["git_commit"] = COMMIT_ID
        result = await cls.do_insert_one(data)
        success = result.acknowledged
        inserted_id = str(result.inserted_id) if success else ""
        return (success, inserted_id)

    @classmethod
    def write_many(cls, many_data: List[Any]) -> Tuple[bool, List[str]]:
        """向 DAO 对应的 collection 写入若干个 document"""
        write_time = datetime.datetime.now()
        for data in many_data:
            data["write_time"] = write_time
            data["update_time"] = write_time
            data["git_commit"] = COMMIT_ID
        result = mongo_motor_loop.run_until_complete(cls.do_insert_many(many_data))
        success = result.acknowledged
        inserted_ids = [str(inserted_id) for inserted_id in result.inserted_ids] if success else [""]
        return (success, inserted_ids)

    @classmethod
    def update_one(cls, id: str, data: Any) -> bool:
        """更新对应 _id 的 document"""
        update_time = datetime.datetime.now()
        data["update_time"] = update_time
        data["git_commit"] = COMMIT_ID
        result = mongo_motor_loop.run_until_complete(cls.do_update_one_by_id(id, data))
        success = result.acknowledged and result.modified_count == 1
        return success

    @classmethod
    def update_many_by_conditon(cls, condition: Dict, set: Dict) -> bool:
        """更新对应 condition 的 document(s) 的 set"""
        update_time = datetime.datetime.now()
        set["$set"]["update_time"] = update_time
        set["$set"]["git_commit"] = COMMIT_ID
        result = mongo_motor_loop.run_until_complete(cls.do_update_many_by_conditon(condition, set))
        success = result.acknowledged and result.modified_count > 0
        return success

    @classmethod
    def count_by_conditon(cls, condition: Dict) -> int:
        """计数对应 condition 的 document(s)"""
        result = mongo_motor_loop.run_until_complete(cls.do_count_by_condition(condition))
        return result

    @classmethod
    def delete_one(cls, id_: str) -> bool:
        """删除对应 _id 的 document"""
        result = mongo_motor_loop.run_until_complete(cls.do_delete_one_by_id(id_))
        success = result.acknowledged
        return success

    @classmethod
    def delete_by_condition(cls, condition: Dict) -> bool:
        """删除对应 condition 的 document(s)"""
        result = mongo_motor_loop.run_until_complete(cls.do_delete_many_by_condition(condition))
        success = result.acknowledged
        return success

    # @classmethod
    # def async_write_one(cls, data: Dict) -> asyncio.Future:
    #     future = cls._collection.insert_one(data)
    #     # future.add_done_callback(lambda e: print(time(), data, e.result().inserted_id))
    #     return future


@dataclass
class Test:
    _id: str
    name: str
    age: int


class TestDAO(RheaDAOBase):
    _collection = db.test_collection
    _dataclass = Test


if __name__ == "__main__":
    origin_data = {"name": "abc", "age": 123}
    (success, test_id) = Testutils.utils.utils.dao.write_one(origin_data)
    if success:
        read_output = Testutils.utils.utils.dao.read_one(test_id)
        print(read_output)
        success = Testutils.utils.utils.dao.update_one(test_id, {"age": 456})
        if success:
            read_output = Testutils.utils.utils.dao.read_one(test_id)
            print(read_output)
