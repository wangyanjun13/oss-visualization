import os 
# 使用环境变量，而不是硬编码
env = os.environ.get("env", "dev")

from fastapi import FastAPI
from routers import images,process_3D_object,pcds, radar, model_index
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源，或者可以指定具体的域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],   # 允许所有请求头
)

# 添加一个简单的健康检查端点
@app.get("/api/v1/health")
async def health_check():
    return {"status": "This is oss-visualization!!!"}

app.include_router(images.router)
app.include_router(process_3D_object.router)
app.include_router(pcds.router)
app.include_router(radar.router)
app.include_router(model_index.router)


if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=5001,)