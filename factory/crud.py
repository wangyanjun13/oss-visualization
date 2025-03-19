import json
from pymongo import MongoClient
from datetime import datetime
from json import dumps
import nori2


MONGO_DATABASE_URL="mongodb://localhost:27017/"
client=MongoClient(MONGO_DATABASE_URL)
db=client['object_3d_data']



def update_one_data(table,query,value,flag=False):
    db[table].update_one(query,{"$set":value},upsert=flag)

def get_one_frame_resouces(query):
    result = db["3d_object"].find_one(query, {"resources": 1})
    if result:
        return result["resources"]
    else:
        return []

def insert_many_data(table,result):
    result=db[table].insert_many(result)
    

def get_total_frame(source_path):
    total_frame=db["3d_object"].count_documents({"source_path":source_path})
    return total_frame


def get_data_source_status(source_path):
    result=db["3d_data_source"].find_one({"source_path":source_path})
    return result

def nori_path_and_nori_id_get_img(nori_path: str, nori_id: str,save_path):
    nori_data = nori2.open(nori_path)
    byte_img = nori_data.get(nori_id)
    with open(save_path, "wb") as f:
        f.write(byte_img)
    return save_path
        

async def get_3dObject_data(page,pageSize,file_path):
    pageNumber = page
    resources=[]
    pre_labels=[]
    oss_url_change=False
    json_data=[]
    skip = (pageNumber - 1) * pageSize
    results=db["3d_object"].find({"source_path":file_path},{"_id":0,"id":0}).skip(skip).limit(pageSize)
    for result in results:
        resources=result.get("resources")
        calibrated_sensors=result.get("calibrated_sensors")
        origin_frame_id=result.get("origin_frame_id")
        labels=result.get("data")["labels"]
        preds=result.get("data")["preds"]
        if labels is not None and labels!=[]:
            if type(labels) == list:
                label=labels
            else:
                label=[labels]
        else:
            label=[]
        if result.get("data")["pre_labels"] !=[]:
            pre_labels=[result.get("data")["pre_labels"]] # pkl
            if isinstance(pre_labels[0], list): # json
                pre_labels = pre_labels[0]
        else:
            pre_labels=[]
        roi_info=result.get("roi_info")
        ins_data=result.get("ins_data")
        json_data.append({
            "label":dumps(label),
            "preds":dumps(preds),
            "extend":dumps({
                "extra_data": {
                    "pre_labels":pre_labels,
                    "roi_info":roi_info,
                    "ins_data":ins_data,
                    "origin_frame_id":origin_frame_id,
                    "calibrated_sensors":calibrated_sensors
                },
            "resources":resources})
        })
    return json_data

def get_model_info():
    result = db["model_index"].find()
    return list(result)

def get_file_path_sensors(source_path, sensors):
    pipeline = [
        {
            '$match': {
                "source_path": source_path
            }
        },
        {
            '$project': {
                'sensors': 1,
                '_id': 0
            }
        },
        {
            '$unwind': '$sensors'
        },
        {
            '$match': {
                'sensors': {'$in': sensors}
            }
        },
        {
            '$group': {
                '_id': '$_id',
                'allKeysMatch': {'$push': '$sensors'}
            }
        },
        {
            '$project': {
                'allKeysMatch': {'$setEquals': ['$allKeysMatch', sensors]},
                '_id': 0
            }
        },
        {
            '$match': {
                'allKeysMatch': True
            }
        }
    ]



    results = db["3d_object"].aggregate(pipeline)
    sensors_exist = list(results)
    print(sensors_exist) 
    if sensors_exist:
        print(f"exists specical sensor: {sensors}")
        return True
    else:
        print(f"not exists special sensor: {sensors}")
        return False 


if __name__ == "__main__":
    pass
    # import tqdm 
    # oss_root = "s3://gongjiahao-share/e2e_public_eval_results/"
    # versions = refile.smart_listdir(oss_root)
    # for version in tqdm.tqdm(versions):
    #     file_path = refile.smart_path_join(oss_root, version, "Metric_summary_e2e_l3_far.json")
    #     with refile.smart_open(file_path) as f:
    #         json_data = json.loads(f.read())     
    #     time_stamp = "2024"+version   
    #     update_one_data(
    #         table="model_index", 
    #         query={"version": time_stamp}, 
    #         value={"type": "", "eval_time": time_stamp, "eval_result": json_data},
    #         flag=True
    #     ) 
    
    file_path = "s3://tf-rhea-data-bpp/170km-track/labeled_data/car_505/20231116_dp-track_yueying_checked/ppl_bag_20231116_133205-partical_partial_track/v0_231206_033505/0003.json"
    results=db["3d_object"].find({"source_path":file_path},{"_id":0,"id":0}).limit(10)
    with open("result.json", "w") as f:
        f.write(json.dumps(list(results)))