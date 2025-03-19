import json
import sys 

from loguru import logger

sys.path.insert(0, "/data/oss_visualization_fastapi")
from factory import crud


class ModelIndexService():
    def __init__(self) -> None:
        pass
    
    def get_model_index(self):
        try:
            AP_dict = {"type": "AP", "data": []}
            max_recall_dict = {"type": "max_recall", "data": []}
            trans_err_dict = {"type": "trans_err", "data": []}
            trans_err_x_dict = {"type": "trans_err_x", "data": []}
            trans_err_y_dict = {"type": "trans_err_y", "data": []}
            scale_err_dict = {"type": "scale_err", "data": []}
            orient_err_dict = {"type": "orient_err", "data": []}

            query_info = crud.get_model_info()
            for info in query_info:
                version = info["version"]
                summary = info["eval_result"]["summary"]
                AP_info = {"version": version}   
                max_recall_info = {"version": version}   
                trans_err_info = {"version": version}   
                trans_err_x_info = {"version": version}   
                trans_err_y_info = {"version": version}   
                scale_err_info = {"version": version}   
                orient_err_info = {"version": version}   
                for category, indexs in summary.items():
                    AP_info[category] = indexs["AP"]
                    max_recall_info[category] = indexs["max_recall"]   
                    trans_err_info[category] = indexs["trans_err"]
                    trans_err_x_info[category] = indexs["trans_err_x"]
                    trans_err_y_info[category] = indexs["trans_err_y"]
                    scale_err_info[category] = indexs["scale_err"]
                    orient_err_info[category] = indexs["orient_err"]
                                
                AP_dict["data"].append(AP_info)
                max_recall_dict["data"].append(max_recall_info)
                trans_err_dict["data"].append(trans_err_info)
                trans_err_x_dict["data"].append(trans_err_x_info)
                trans_err_y_dict["data"].append(trans_err_y_info)
                scale_err_dict["data"].append(scale_err_info)
                orient_err_dict["data"].append(orient_err_info)
            
            return [AP_dict, max_recall_dict, trans_err_dict, trans_err_x_dict, trans_err_y_dict, scale_err_dict, orient_err_dict]             
                        
        except Exception as e:
            logger.error(f"error: {repr(e)}")
            return []
    
model_index_serice = ModelIndexService()


if __name__ == '__main__':
    result = model_index_serice.get_model_index()
    with open("result.json", "w") as f:
        f.write(json.dumps(result))