import refile
from fastapi import APIRouter,HTTPException,Depends,BackgroundTasks
from fastapi.responses import JSONResponse,FileResponse, StreamingResponse

from config import PCD_DIR

router = APIRouter(
    prefix="/api/v1/pcds",
    tags=["pcds"],
    # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)


def generate_pcd_data(pcd_path):
    with refile.smart_open(pcd_path, "rb") as f:
        while chunk := f.read(1024):  # 每次读取 1024 字节
            yield chunk

@router.get("/get_3d_pcd/{pcd_name}")
async def get_image(pcd_name: str):
    pcd_path = refile.smart_path_join(PCD_DIR, pcd_name)

    if not refile.smart_exists(pcd_path):
        raise HTTPException(status_code=404, detail="Pcd not found")

    return StreamingResponse(generate_pcd_data(pcd_path), media_type="application/octet-stream")



