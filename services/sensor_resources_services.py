
from loguru import logger
from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse

from schemas.sensor_schemas import SensorsRequest, SensorResourcesRequest
from utils.analysis_utils_2 import analysis
# from utils.analysis_utils_2_nori_speed import analysis
from factory import get_data_source_status, get_total_frame, get_file_path_sensors


class SensorsService:
    
    def get_sensor_list(self, req: SensorsRequest):
        sensor_result = {}
        try:
            for path in req.s3_paths:
                sensors = analysis.get_sensor_list(path)
                sensors_d = {}
                for sensor in sensors:
                    if "lidar" in sensor:
                        sensors_d.setdefault("pcd", []).append(sensor)
                    elif "radar" in sensor:
                        sensors_d.setdefault("json", []).append(sensor)
                    elif 'cam_' in sensor and sensor != "cam_extra_info":
                        sensors_d.setdefault("jpg", []).append(sensor)
            
                sensor_result[path] = sensors_d
        except Exception as e:
            logger.error(f"get sensor list error: {repr(e)}")
            return JSONResponse({"result": f"{repr(e)}"})
        return sensor_result


    def analysis_3D_object_optional_sensor(self, req: SensorResourcesRequest, background_tasks: BackgroundTasks):   
        def run(s3_path, sensors, view, is_project, background_tasks: BackgroundTasks):
            result = get_data_source_status(s3_path)
            if result is None:
                occ = False
                if view == "occ":
                    occ = True
                background_tasks.add_task(analysis.analysis_file, s3_path, sensors, occ, is_project)
            else:
                status = result.get("status")
                message = result.get("message")
                if status == "processing":
                    return {"status":status}
                elif status=="success":
                    if sensors: # 有sensor，才判断sensor是否全                 
                        all_sensor_exists = get_file_path_sensors(s3_path, sensors)
                        logger.info(f"all_sensor_exists: {all_sensor_exists}")
                        if all_sensor_exists:
                            total_frame = get_total_frame(s3_path)
                            return {"status":status,"total_frame":total_frame,"s3_path":s3_path}
                        else:
                            background_tasks.add_task(analysis.analysis_file, s3_path, sensors)
                            return {"status": "processing"}
                    else: # 没有sensor，返回数据
                        total_frame = get_total_frame(s3_path)
                        return {"status":status,"total_frame":total_frame,"s3_path":s3_path}
                elif status == "failed": # 对于失败的任务，应该允许重试
                    background_tasks.add_task(analysis.analysis_file, s3_path, sensors)
                    return {"status":"processing"}
             
        result = []
        for resource in req.s3_paths:
            s3_path = resource.s3_path
            sensors = resource.sensors
            is_project = resource.is_project
            view = resource.view
            logger.info(f"s3_path: {s3_path}, sensors: {sensors}, is_project: {is_project}, view: {view}")
            tmp = run(s3_path, sensors, view, is_project, background_tasks)
            result.append(tmp)
        return JSONResponse({"result":result})

    
    def analysis_3D_object_process_status(self, req: SensorResourcesRequest):
        result = []
        for resource in req.s3_paths:
            s3_path = resource.s3_path
            sensors = resource.sensors
            logger.info(f"s3_path: {s3_path}, sensors: {sensors}")
            data_source_record = get_data_source_status(s3_path)
            if data_source_record:
                status = data_source_record.get("status")
                message = data_source_record.get("message")
                if status == "processing":
                    result.append({"status":status, "message": message, "s3_path": s3_path})
                elif status=="success":
                    total_frame = get_total_frame(s3_path)
                    result.append({"status":status,"total_frame":total_frame,"s3_path":s3_path, "message": message})
                elif status == "failed":
                    result.append({"status":"failed", "message": message, "s3_path": s3_path})
            else:
                result.append({"status": "unloaded", "message": "未加载", "s3_path": s3_path})
                
        return JSONResponse({"result":result})

sensors_serivice = SensorsService()
