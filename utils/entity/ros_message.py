import copy
from dataclasses import dataclass
from typing import Dict, List, Optional


# TODO(chenlei): dataclass的默认值难以继承, 这导致有些时候要写很多空值。
# 见: https://qa.1r1g.com/sf/ask/3610315201/
# 比如设置了 avaliable 默认值为 true, 后面所有的派生类都会报错 args with default value 应该放在最后面。
@dataclass
class RosMessage:
    _id: str
    parsed_rosbag_id: str
    split_name: str
    timestamp: str
    index_in_split: int
    topic_type: str
    topic_name: str
    avaliable: bool

    def as_dict(self):
        return self.__dict__


@dataclass
class Camera(RosMessage):
    camera_name: str
    nori_id: str
    nori_path: str


@dataclass
class Lidar(RosMessage):
    lidar_name: str
    nori_id: str
    nori_path: str


@dataclass
class UltrasonicRadarInfo:
    radar_identify_id: int
    radar_name: str
    radar_type: str
    radar_state: bool
    radar_tx_status: bool
    radar_rx_status: bool
    range: float
    MAX_RADAR_DISTANCE: int
    max_radar_distance: int
    radar_distance: float
    radar_distance_vaild: bool

    def as_dict(self):
        return self.__dict__


@dataclass
class UltrasonicRadar(RosMessage):
    version: str
    ultra_radar_array: List[UltrasonicRadarInfo]

    def as_dict(self):
        _dict = copy.deepcopy(self.__dict__)
        _dict["ultra_radar_array"] = [_.as_dict() for _ in self.ultra_radar_array]
        return _dict


@dataclass
class Radar(RosMessage):
    radar_name: str
    nori_id: str
    nori_path: str


@dataclass
class VehicleReportInfo(RosMessage):
    wire_control_flag: bool  # 线控使能状态

    gear_en: bool  # 当前档位使能状态
    gear: int  # 当前档位值 1:P, 2:R, 3:N, 4:D

    speed: float  # 车速 km/h
    long_acc: float  # 当前纵向加速度
    late_acc: float  # 当前横向加速度
    long_flt: int  # 纵向控制故障码

    long_en: bool  # 当前纵向接管状态，True时标识人工接管
    late_en: bool  # 当前横向控制使能状态

    eps_torque: float  # EPS力矩 Nm
    front_wheel_angle: float  # 前轮角度 deg, 根据方向盘角度和转向比计算
    steer_angle: float  # 方向盘角度 deg， 左转时为负值，右转时为正值
    yaw_rate: float  # 转动速率 deg/s
    late_flt: int  # 横向控制故障码

    turn_signal_status: int  # 转向灯状态
    wiper_status: int  # 雨刷状态
    door_fl: bool  # 左前门状态
    door_fr: bool  # 右前门状态
    door_rl: bool  # 左后门状态
    door_rr: bool  # 右后门状态

    wheelspeed_fl: bool  # 左前轮轮速 km/h
    wheelspeed_fr: bool  # 右前轮轮速 km/h
    wheelspeed_rl: bool  # 左后轮轮速 km/h
    wheelspeed_rr: bool  # 右后轮轮速 km/h

    pdc_dist_front: int  # 前方超声波传感器测量距离 cm
    pdc_dist_rear: int  # 后方超声波传感器测量距离 cm
    pdc_dist_fls: int  # 左前侧超声波传感器测量距离 cm
    pdc_dist_frs: int  # 右前侧超声波传感器测量距离 cm
    pdc_dist_rls: int  # 左后侧超声波传感器测量距离 cm
    pdc_dist_rrs: int  # 右后侧超声波传感器测量距离 cm
    vehicle_report_index: Optional[int] = None


@dataclass
class VehicleReportCommon(RosMessage):
    speed: float  # 车速 km/h
    speed_valid: bool  # 车速是否有效
    speed_directivity: bool  # 车速是否具有方向性

    wheelspeed_directivity: bool  # 轮速是否具有方向性
    wheelspeed_valid: List[bool]  # 轮速是否有效，顺序为[FL,FR,RL,RR]
    wheelspeed_fl_valid: bool
    wheelspeed_fr_valid: bool
    wheelspeed_rl_valid: bool
    wheelspeed_rr_valid: bool
    wheelspeed: List[float]  # 四个轮的轮速 km/h, 顺序为[FL, FR, RL ,RR]
    wheelspeed_fl: bool
    wheelspeed_fr: bool
    wheelspeed_rl: bool
    wheelspeed_rr: bool
    gear_en: bool  # 档位控制使能状态
    gear: int  # 当前档位值 0:无效 1:P, 2:R, 3:N, 4:D

    epb_state: int  # 电子手刹状态： 0：无效，1：释放中，2：制动状态，3：制动生效中，4：释放状态

    late_en: bool  # 横向控制线控使能状态
    late_driveover: bool  # 横向控制人工接管状态
    steer_angle_valid: bool  # 方向盘角度是否有效
    steer_angle: float  # 当前方向盘角度值 deg, 左转时为负值，右转时为正值
    steer_rotate_angle_speed: float  # 方向盘转动角速度
    steer_torque_valid: bool  # 方向盘力矩是否有效
    steer_torque: float  # 方向盘力矩
    steer_rotate_torque_speed: float  # 方向盘转动力矩速度, 预留
    late_flt: int  # 横向控制故障码，预留

    long_en: bool  # 纵向控制线控使能状态
    long_driveover: bool  # 纵向控制人工接管状态
    long_torque_valid: bool  # 纵向控制力矩有效性
    long_torque: float  # 纵向控制实际力矩值
    long_acc_valid: bool  # 纵向控制加速度值有效性
    long_acc: float  # 纵向控制实际加速度
    long_flt: int  # 纵向控制故障码, 预留

    turn_lamp_lever_state: int  # 转向拨杆状态：0：默认，1：左转，2：右转，other：无效
    turn_lamp_left: bool  # 左转向灯点亮状态
    turn_lamp_right: bool  # 右转向灯点亮状态
    hazard_lamp: bool  # 告警灯点亮状态
    wiper_front: bool  # 前雨刷器启动状态
    door_open_state: List[bool]  # 四车门关闭状态，顺序为[FL, FR, RL, RR]
    door_fl: bool
    door_fr: bool
    door_rl: bool
    door_rr: bool

    late_acc: float  # 车辆运行横向加速度
    yaw_rate: float  # yawrate

    pedal_valid: bool  # 踏板数据有效
    pedal: Dict  # 踏板数据

    wheelspeed_rc_valid: bool  # 轮速计数据是否有效
    wheelspeed_rc: Dict  # TODO(chenlei): 这个要不要设计成一个数据类?

    belt_valid: bool  # ASE3特有数据有效
    belt: Dict  # TODO(chenlei): 同 wheelspeed_rc
    vehicle_report_index: Optional[int] = None

    wheelspeed_dir: Optional[List[int]] = None  # 轮速方向，顺序为[FL, FR, RL, RR]。 0:无效，1:静止，2:前向，3:后向

    standstill: Optional[int] = None
    epb_flt: Optional[int] = None  # 电子手刹控制响应故障码：0：无故障，1：下发命令无效，2：多节点控制冲突，3：与挡位冲突，4：车辆未响应，5：车辆反馈错误，6：其他
    protocol_version: Optional[str] = None  # 消息协议版本号，更改消息后需要增加版本号，保持默认值方式
    unix_ts: Optional[float] = None


@dataclass
class Localization:
    # TODO(chenlei): 这些Dict基本是含有x,y,z三个, 需要再实现一个Point3D/Vector3D类? 但是转换函数就复杂了一层
    utm_id: int
    position: Dict
    orientation: Dict
    linear_velocity: Dict
    linear_acceleration: Dict
    angular_velocity: Dict
    heading: float
    linear_acceleration_vrf: Dict
    angular_velocity_vrf: Dict
    euler_angles: Dict

    def as_dict(self):
        return self.__dict__


@dataclass
class Ins(RosMessage):
    ins_status: str
    lat: float
    lon: float  # TODO(chenlei): Ins的浮点都是float64
    height: float
    localization: Localization
    true_north_heading: float
    ins_index: Optional[int] = None

    def as_dict(self):
        dict_ = copy.deepcopy(self.__dict__)
        dict_["localization"] = self.localization.__dict__
        return dict_


@dataclass
class Gps(RosMessage):
    solution_status: str  # 结算状态
    position_type: str  # 位置类型

    lat: float  # 纬度
    lon: float  # 经度
    height_msl: float  # 海平面高度

    undulation: float  # 高程异常值
    datum_id: str  # 坐标系 id

    lat_sigma: float  # 纬度标准差
    lon_sigma: float  # 经度标准差
    height_sigma: float  # 高度标准差
    diff_age: float  # 差分延迟时间 (s)
    solution_age: float  # 计算延迟时间 (s)
    num_satellites_tracked: int  # 跟踪卫星数目
    num_satellites_used_in_solution: int  # 解算卫星数目
    num_gps_and_glonass_l1_used_in_solution: int  # l1/b1/b1 解算卫星数目
    num_gps_and_glonass_l1_and_l2_used_in_solution: int  # 多频计算卫星数目

    extended_solution_status: int  # 扩展结算状态
    gps_glonass_used_mask: int  # GPS和Glonass信号使用标志
    galileo_beidou_used_mask: int  # galileo 和北斗信号使用标志
    linear_velocity: Dict
    velocity_latency: float  # 速度延迟
    gps_index: Optional[int] = None


@dataclass
class CorrImu(RosMessage):
    linear_acceleration_vrf: Dict
    angular_velocity_vrf: Dict
    corr_imu_index: Optional[int] = None


@dataclass
class RawImu(RosMessage):
    measurement_span: Dict
    linear_acceleration: Dict
    angular_velocity: Dict
    raw_imu_index: Optional[int] = None


# TODO(chenlei): deal_with_localizationEstimate 是有的, 设计里没有 LocalizationEstimate 的实体. 确认一下


@dataclass
class Odometry(RosMessage):
    # TODO(chenlei): 这个之前说了需要拍平, 先按我的理解来了。
    # TODO(chenlei): child_frame_id 和 covariance 在设计里有提到, 但是 deal_with_ 代码里没有记录
    child_frame_id: str
    position: Dict  # xyz
    orientation: Dict  # xyzw
    linear_twist: Dict  # xyz
    angular_twist: Dict  # xyz


@dataclass
class Tag(RosMessage):
    # ["晴天","训练数据"]
    tags: List


@dataclass
class LidarSlamPose:
    localization: Dict
    timestamp: str

    def as_dict(self):
        return self.__dict__


@dataclass
class CameraExtraInfo(RosMessage):
    camera_name: str
    time_expo: int
