from fastapi import APIRouter,HTTPException,Depends,BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
# from utils import analysis
from factory import insert_many_data,get_data_source_status,get_total_frame,get_3dObject_data
from concurrent.futures import ProcessPoolExecutor,as_completed
import logging
# from utils.analysis_utils import analysis
from utils.analysis_utils_2 import analysis
from schemas.sensor_schemas import SensorsRequest, SensorResourcesRequest
from services.sensor_resources_services import sensors_serivice


logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/v1/3D_object",
    tags=["3D_object"],
    # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)
# "s3://gongjiahao-share/tmp/front_lidar_only_eval_frames.pkl"

# s3://camera-perceptron/labeled_data/car_501/20231101_dp-det_yueying_checked/ppl_bag_20231101_131139_det/v0_231107_014458/0002.json

async def run(s3_path,background_tasks):
    result=get_data_source_status(s3_path)
    if result is None:
        background_tasks.add_task(analysis.analysis_file,s3_path)
    else:
        status=result.get("status")
        message=result.get("message")
        if status=="processing":
            return {"status":status}
        elif status=="success":
            total_frame=get_total_frame(s3_path)
            # return {"status":status,"total_frame":total_frame,"data":get_3dObject_data(1,total_frame,s3_path)}
            return {"status":status,"total_frame":total_frame,"s3_path":s3_path}
        elif status=="failed":
            # background_tasks.add_task(analysis,s3_path)
            return {"status":"failed", "message": message}




@router.post("/analysis_3D_object")
async def analysis_3D_object(
    s3_path:dict,
    background_tasks:BackgroundTasks
):  
    s3_pathes=s3_path.get("s3_path")
    result=[]
    if type(s3_pathes) == list:
        for s3_path in s3_pathes:
            tmp=await run(s3_path,background_tasks)
            result.append(tmp)
        return JSONResponse({"result":result})
    else:
        raise HTTPException(status_code=500, detail="传递值不是列表")

            
    
@router.get("/get_3d_data")
async def get_3d_object_data(
    page:int,
    pageSize:int,
    s3_path:str
):
    json_data=await get_3dObject_data(page,pageSize,s3_path)
    return JSONResponse(jsonable_encoder({"json_data":json_data}),status_code=200)


@router.post("/get_sensor_list")
def get_sensor_list(
    req: SensorsRequest
):
    return sensors_serivice.get_sensor_list(req)

@router.post("/analysis_3D_object_optional_sensor")
def analysis_3D_object_optional_sensor(
    req: SensorResourcesRequest,
    background_tasks: BackgroundTasks
):
    return sensors_serivice.analysis_3D_object_optional_sensor(req, background_tasks)

@router.post("/analysis_3D_object_process_status")
def analysis_3D_object_process_status(
    req: SensorResourcesRequest
):
    return sensors_serivice.analysis_3D_object_process_status(req)