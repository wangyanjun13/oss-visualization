from fastapi import APIRouter,HTTPException,Depends,BackgroundTasks
from fastapi.responses import JSONResponse,FileResponse, StreamingResponse

import refile

from config import IMAGE_DIR

router = APIRouter(
    prefix="/api/v1/images",
    tags=["images"],
    # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)

def generate_image(image_path):
    # 流式读取数据
    with refile.smart_open(image_path, "rb") as f:
        while chunk := f.read(1024):  # 每次读取 1024 字节
            yield chunk    

@router.get("/get_3d_image/{image_name}")
async def get_image(image_name: str):
    image_path = refile.smart_path_join(IMAGE_DIR, image_name)

    if not refile.smart_exists(image_path):
        raise HTTPException(status_code=404, detail="Image not found")

    return StreamingResponse(generate_image(image_path), media_type="image/jpeg")


