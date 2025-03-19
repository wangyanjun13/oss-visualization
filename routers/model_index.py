from fastapi import APIRouter,HTTPException
from fastapi.responses import JSONResponse


from schemas.model_index_schemas import ModelIndexRequest
from services.model_index_service import model_index_serice

router = APIRouter(
    prefix="/api/v1/model",
    tags=["model_index"],
    responses={404: {"description": "Not found"}},
)
 

@router.get("/get_model_index", summary="展示模型不同版本的指标")
async def get_model_index():
    return model_index_serice.get_model_index()
