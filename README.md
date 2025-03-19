# oss-visualization
这是一个用于3D对象可视化的FastAPI后端项目，主要是处理和分析来自不同传感器（相机、雷达、激光雷达等）的数据，并提供API接口供前端调用。
uvicorn app:app --host 0.0.0.0 --port 5001 --workers 4

前台启动

> 这是一个用于3D对象可视化的FastAPI后端项目，主要是处理和分析来自不同传感器（相机、雷达、激光雷达等）的数据，并提供API接口供前端调用。

项目结构概览：

oss_visualization_fastapi/
├── app.py                 # FastAPI主应用入口
├── appci/                 # 应用配置目录
│   └── app-dev.yaml       # 开发环境配置
├── config.py              # 配置文件
├── Dockerfile             # Docker构建文件
├── entrypoint.sh          # 容器启动脚本
├── factory/               # 工厂模式目录
│   ├── __init__.py
│   └── crud.py            # 数据库CRUD操作
├── Makefile               # 构建脚本
├── rclone/                # rclone配置(用于云存储交互)
├── routers/               # API路由目录
│   ├── images.py          # 图像相关API
│   ├── model_index.py     # 模型索引API
│   ├── pcds.py            # 点云数据API
│   ├── process_3D_object.py # 3D对象处理API
│   └── radar.py           # 雷达数据API
├── schemas/               # 数据模型目录
│   ├── model_index_schemas.py
│   └── sensor_schemas.py
├── services/              # 服务层目录
│   ├── model_index_service.py
│   └── sensor_resources_services.py
├── tasks.py               # Celery任务定义
├── test/                  # 测试目录
└── utils/                 # 工具函数目录
    ├── analysis_3d_object_utils.py
    ├── analysis_utils.py
    ├── analysis_utils_2.py
    ├── analysis_utils_2_nori_speed.py
    ├── dao/                # 数据访问对象目录
    ├── entity/             # 实体模型目录
    └── pcd_aliyun_utils.py # 阿里云PCD工具

主要功能模块：

1. 3D对象分析：处理来自不同传感器的数据，生成3D可视化所需的信息
2. 图像处理：处理和提供图像数据
3. PCD（点云）处理：处理和提供激光雷达点云数据
4. 雷达数据处理：处理和提供雷达数据
5. 后台任务处理：使用Celery进行异步处理

技术栈：

1. FastAPI - Web框架
2. MongoDB - 数据存储
3. Redis - 缓存和Celery消息队列
4. Celery - 异步任务处理
5. OSS/S3 - 对象存储，用于存储大型文件
6. nori2/refile - 可能是内部文件处理库

### 1. 核心模块详解

#### 1.1 路由模块 (routers/)

该模块定义了API端点，主要包括：

* images.py: 处理图像数据的API
* pcds.py: 处理点云数据的API
* radar.py: 处理雷达数据的API
* process_3D_object.py: 处理3D对象分析的API
* model_index.py: 模型指标相关API

#### 1.2 服务模块 (services/)

实现业务逻辑的核心层：

* sensor_resources_services.py: 管理传感器资源，包括获取传感器列表、分析3D对象等功能
* model_index_service.py: 处理模型指标数据

#### 1.3 工具模块 (utils/)

提供各种功能支持的工具类和函数：

* analysis_utils.py: 数据分析工具
* analysis_utils_2.py: 改进版数据分析工具
* file_utils.py: 文件操作工具
* pcd_writer_utils.py: 点云数据写入工具
* pcd2cam.py: 点云到相机投影工具
* lidar_utils.py: 激光雷达数据处理
* occ_utils.py: 占位数据处理

#### 1.4 数据访问层 (factory/)

负责与数据库交互：

* crud.py: 实现CRUD操作
* init_.py: 导出数据访问函数

#### 1.5 配置模块

* config.py: 环境配置，定义了开发和生产环境的不同配置
* rclone/rclone.conf: 对象存储访问配置

#### 1.6 异步任务处理

* tasks.py: 定义Celery任务，特别是处理耗时的数据分析和投影任务
* celery_task/: Celery配置

### 2. 数据流程

1. 数据源：支持从S3兼容存储（如阿里云OSS和Brain++内部存储）读取数据
2. 数据处理：

* 解析PKL或JSON格式的原始数据
* 提取传感器信息和校准数据
* 处理图像、点云和雷达数据
* 将点云投影到图像上

1. 数据存储：

* 处理后的数据存储在MongoDB中
* 大型文件（如图像、点云）存储在对象存储中

1. API服务：

* 提供RESTful API供前端调用
* 支持异步处理大型分析任务

### 3. 核心功能

1. 传感器列表获取：查询可用的传感器列表
2. 3D对象分析：处理和分析3D对象数据
3. 图像处理和展示：处理图像数据并提供API访问
4. 点云处理和展示：处理点云数据并提供API访问
5. 雷达数据处理：处理雷达数据并提供展示
6. 数据同步：在不同存储系统间同步数据

### 4. 依赖组件

* MongoDB: 存储处理结果和元数据
* Redis: 用于Celery任务队列和缓存
* 对象存储: 存储大型文件（阿里云OSS和Brain++内部存储）
* Celery: 处理异步任务

```python

图像相关

GET /api/v1/images/get_3d_image/{image_name}

点云相关

GET /api/v1/pcds/get_3d_pcd/{pcd_name}

雷达相关

GET /api/v1/radars/get_3d_radar/{pcd_name}

3D对象处理

POST /api/v1/3D_object/analysis_3D_object
GET /api/v1/3D_object/get_3d_data
POST /api/v1/3D_object/get_sensor_list
POST /api/v1/3D_object/analysis_3D_object_optional_sensor

模型指标

GET /api/v1/model/get_model_index
```