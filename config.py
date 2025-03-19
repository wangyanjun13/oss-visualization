import os
import boto3

# 获取环境变量，提供默认值为"dev"
env = os.environ.get("env", "dev")

# S3/OSS 存储路径
IMAGE_DIR = "s3://mc-trained-data-qy/3d_object_visualization/3d_images"  # oss_dir
PCD_DIR = "s3://mc-trained-data-qy/3d_object_visualization/pcds"
RADAR_DIR = "s3://mc-trained-data-qy/3d_object_visualization/radars"

# S3/OSS 凭证配置
# 从rclone.conf中提取的阿里云OSS凭证
S3_ACCESS_KEY = ""
S3_SECRET_KEY = ""
S3_ENDPOINT = "oss-cn-beijing.aliyuncs.com"

# 设置环境变量，供refile使用
os.environ['AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = S3_SECRET_KEY
os.environ['AWS_ENDPOINT_URL'] = f"https://{S3_ENDPOINT}"

# 使用boto3.client创建S3客户端
try:
    # 尝试创建S3客户端
    s3_client = boto3.client(
        's3',
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='cn-beijing',
        endpoint_url=f"https://{S3_ENDPOINT}"
    )
except Exception as e:
    print(f"Warning: Failed to create boto3 S3 client: {str(e)}")

# 环境相关配置
if env == "dev":
    # BASE_URL = "https://rigel-oss.iap.hh-d.brainpp.cn"
    BASE_URL = "https://rigel-test.iap.qy.machdrive.cn"
elif env == "pro":
    BASE_URL = "https://rigel-pro.iap.qy.machdrive.cn"
else:
    # 如果env不是dev或pro，默认使用dev环境
    print(f"Warning: Unknown environment '{env}', using 'dev' as default")
    BASE_URL = "https://rigel-test.iap.qy.machdrive.cn"
