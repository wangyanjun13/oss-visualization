import nacos
import json


SERVER_ADDRESSES = "10.236.245.60:8848"
NAMESPACE = "rhea"

NACOS_CLIENT = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE)


def load_nacos_json_config(data_id, group) -> dict:
    config = NACOS_CLIENT.get_config(data_id, group)
    json_config = json.loads(config)

    return json_config


def load_nacos_txt_config(data_id, group) -> str:
    txt_config = NACOS_CLIENT.get_config(data_id, group)

    return txt_config
