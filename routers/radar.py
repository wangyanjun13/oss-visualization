import refile
from loguru import logger
from fastapi import APIRouter,HTTPException,Depends,BackgroundTasks
from fastapi.responses import JSONResponse,FileResponse, StreamingResponse

from config import RADAR_DIR

router = APIRouter(
    prefix="/api/v1/radars",
    tags=["radars"],
    # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)


def generate_pcd_data(pcd_path):
    with refile.smart_open(pcd_path, "rb") as f:
        while chunk := f.read(1024):  # 每次读取 1024 字节
            yield chunk

@router.get("/get_3d_radar/{pcd_name}")
async def get_image(pcd_name: str):
    pcd_path = refile.smart_path_join(RADAR_DIR, pcd_name)
    logger.info(f"pcd_path: {pcd_path}")
    if not refile.smart_exists(pcd_path):
        raise HTTPException(status_code=404, detail="Pcd not found")

    response = StreamingResponse(generate_pcd_data(pcd_path), media_type="application/json")
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response



