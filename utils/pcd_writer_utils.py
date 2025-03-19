import copy
import io
import struct
import warnings

import lzf
import nori2 as nori
import numpy as np
from numpy.lib import recfunctions as rfn
from loguru import logger
from refile import smart_open
import refile

from utils import lidar_utils
from .nori_utils import nori_path_replace, nori_open


def _metadata_is_consistent(metadata):
    """Sanity check for metadata. Just some basic checks."""
    checks = []
    required = ("version", "fields", "size", "width", "height", "points", "viewpoint", "data")
    for f in required:
        if f not in metadata:
            print("%s required" % f)
    checks.append((lambda m: all([k in m for k in required]), "missing field"))
    checks.append(
        (
            lambda m: len(m["type"]) == len(m["count"]) == len(m["fields"]),
            "length of type, count and fields must be equal",
        )
    )
    checks.append((lambda m: m["height"] > 0, "height must be greater than 0"))
    checks.append((lambda m: m["width"] > 0, "width must be greater than 0"))
    checks.append((lambda m: m["points"] > 0, "points must be greater than 0"))
    checks.append(
        (
            lambda m: m["data"].lower() in ("ascii", "binary", "binary_compressed"),
            "unknown data type:" "should be ascii/binary/binary_compressed",
        )
    )
    ok = True
    for check, msg in checks:
        if not check(metadata):
            print("error:", msg)
            ok = False
    return ok


def parse_ascii_pc_data(f, dtype, metadata):
    """Use numpy to parse ascii pointcloud data."""
    return np.loadtxt(f, dtype=dtype, delimiter=" ")


def parse_binary_pc_data(f, dtype, metadata):
    rowstep = metadata["points"] * dtype.itemsize
    # for some reason pcl adds empty space at the end of files
    buf = f.read(rowstep)
    return np.frombuffer(buf, dtype=dtype)


def write_header(metadata, rename_padding=False):
    """Given metadata as dictionary, return a string header."""
    template = """\
VERSION {version}
FIELDS {fields}
SIZE {size}
TYPE {type}
COUNT {count}
WIDTH {width}
HEIGHT {height}
VIEWPOINT {viewpoint}
POINTS {points}
DATA {data}
"""
    str_metadata = metadata.copy()

    if not rename_padding:
        str_metadata["fields"] = " ".join(metadata["fields"])
    else:
        new_fields = []
        for f in metadata["fields"]:
            if f == "_":
                new_fields.append("padding")
            else:
                new_fields.append(f)
        str_metadata["fields"] = " ".join(new_fields)
    str_metadata["size"] = " ".join(map(str, metadata["size"]))
    str_metadata["type"] = " ".join(metadata["type"])
    str_metadata["count"] = " ".join(map(str, metadata["count"]))
    str_metadata["width"] = str(metadata["width"])
    str_metadata["height"] = str(metadata["height"])
    str_metadata["viewpoint"] = " ".join(map(str, metadata["viewpoint"]))
    str_metadata["points"] = str(metadata["points"])
    tmpl = template.format(**str_metadata)
    return tmpl


def build_ascii_fmtstr(pc):
    """Make a format string for printing to ascii.
    Note %.8f is minimum for rgb.
    """
    fmtstr = []
    for t, cnt in zip(pc.type, pc.count):
        if t == "F":
            fmtstr.extend(["%.10f"] * cnt)
        elif t == "I":
            fmtstr.extend(["%d"] * cnt)
        elif t == "U":
            fmtstr.extend(["%u"] * cnt)
        else:
            raise ValueError("don't know about type %s" % t)
    return fmtstr


def point_cloud_to_fileobj(pc, fileobj, data_compression=None):
    """Write pointcloud as .pcd to fileobj.
    If data_compression is not None it overrides pc.data.
    """
    metadata = pc.get_metadata()
    if data_compression is not None:
        data_compression = data_compression.lower()
        assert data_compression in ("ascii", "binary", "binary_compressed")
        metadata["data"] = data_compression

    header = write_header(metadata)
    header = str.encode(header, "utf-8")
    fileobj.write(header)
    if metadata["data"].lower() == "ascii":
        fmtstr = build_ascii_fmtstr(pc)
        np.savetxt(fileobj, pc.pc_data, fmt=fmtstr)
    elif metadata["data"].lower() == "binary":
        fileobj.write(pc.pc_data.tobytes("C"))
    elif metadata["data"].lower() == "binary_compressed":
        # TODO
        # a '_' field is ignored by pcl and breakes compressed point clouds.
        # changing '_' to '_padding' or other name fixes this.
        # admittedly padding shouldn't be compressed in the first place.
        # reorder to column-by-column
        uncompressed_lst = []
        for fieldname in pc.pc_data.dtype.names:
            column = np.ascontiguousarray(pc.pc_data[fieldname]).tobytes("C")
            uncompressed_lst.append(column)
        uncompressed = "".join(uncompressed_lst)
        uncompressed_size = len(uncompressed)
        # print("uncompressed_size = %r"%(uncompressed_size))
        buf = lzf.compress(uncompressed)
        if buf is None:
            # compression didn't shrink the file
            # TODO what do to do in this case when reading?
            buf = uncompressed
            compressed_size = uncompressed_size
        else:
            compressed_size = len(buf)
        fmt = "II"
        fileobj.write(struct.pack(fmt, compressed_size, uncompressed_size))
        fileobj.write(buf)
    else:
        raise ValueError("unknown DATA type")
    # we can't close because if it's stringio buf then we can't get value after


numpy_pcd_type_mappings = [
    (np.dtype("float32"), ("F", 4)),
    (np.dtype("float64"), ("F", 8)),
    (np.dtype("uint8"), ("U", 1)),
    (np.dtype("uint16"), ("U", 2)),
    (np.dtype("uint32"), ("U", 4)),
    (np.dtype("uint64"), ("U", 8)),
    (np.dtype("int16"), ("I", 2)),
    (np.dtype("int32"), ("I", 4)),
    (np.dtype("int64"), ("I", 8)),
]
numpy_type_to_pcd_type = dict(numpy_pcd_type_mappings)


# def point_cloud_to_path(pc, s3_path_name):
#     with smart_open(s3_path_name, "wb") as f:
#         point_cloud_to_fileobj(pc, f)


def point_cloud_to_path(pc, s3_path_name):
    with refile.smart_open(s3_path_name, "wb") as f:
        point_cloud_to_fileobj(pc, f)


class PointCloud(object):
    def __init__(self, metadata, pc_data):
        self.metadata_keys = metadata.keys()
        self.__dict__.update(metadata)
        self.pc_data = pc_data
        self.check_sanity()

    def get_metadata(self):
        """returns copy of metadata"""
        metadata = {}
        for k in self.metadata_keys:
            metadata[k] = copy.copy(getattr(self, k))
        return metadata

    def check_sanity(self):
        # pdb.set_trace()
        md = self.get_metadata()
        assert _metadata_is_consistent(md)
        assert len(self.pc_data) == self.points
        assert self.width * self.height == self.points
        assert len(self.fields) == len(self.count)
        assert len(self.fields) == len(self.type)

    def save(self, fname):
        self.save_pcd(fname, "ascii")

    def save_pcd(self, fname, compression=None, **kwargs):
        if "data_compression" in kwargs:
            warnings.warn("data_compression keyword is deprecated for" " compression")
            compression = kwargs["data_compression"]
        with open(fname, "w") as f:
            point_cloud_to_fileobj(self, f, compression)

    def save_pcd_to_fileobj(self, fileobj, compression=None, **kwargs):
        if "data_compression" in kwargs:
            warnings.warn("data_compression keyword is deprecated for" " compression")
            compression = kwargs["data_compression"]
        point_cloud_to_fileobj(self, fileobj, compression)

    def copy(self):
        new_pc_data = np.copy(self.pc_data)
        new_metadata = self.get_metadata()
        return PointCloud(new_metadata, new_pc_data)

    @staticmethod
    def from_array(arr, pcd_type, attr="xyz"):
        """create a PointCloud object from an array."""
        if attr == "xyz":
            pc_data = arr.view(np.dtype([("x", np.float32), ("y", np.float32), ("z", np.float32)])).squeeze()
        elif attr == "xyzi":
            pc_data = arr.view(
                np.dtype([("x", np.float32), ("y", np.float32), ("z", np.float32), ("intensity", np.float32)])
            ).squeeze()
        else:
            raise NotImplementedError()
        md = {
            "version": 0.7,
            "fields": [],
            "size": [],
            "count": [],
            "width": 0,
            "height": 1,
            "viewpoint": [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
            "points": 0,
            "type": [],
            "data": pcd_type,
        }
        md["fields"] = pc_data.dtype.names
        for field in md["fields"]:
            type_, size_ = numpy_type_to_pcd_type[pc_data.dtype.fields[field][0]]
            md["type"].append(type_)
            md["size"].append(size_)
            # TODO handle multicount
            md["count"].append(1)
        md["width"] = len(pc_data)
        md["points"] = len(pc_data)
        pc = PointCloud(md, pc_data)
        return pc


def to_pcd(nori_id, nori_path, save_path, pcd_type: str = "binary", attr="xyz", lidar_ids=None):
    try:
        with nori_open(nori_path_replace(nori_path), "r") as nf:
            lidar_data = nf.get(nori_id)
    except Exception as e:
        logger.error(e)
        return None
    data = np.load(io.BytesIO(lidar_data))
    if lidar_ids:
        data = lidar_utils.filter_fuser_lidar_by_lidar_ids(data, lidar_ids)
    if attr == "xyz":
        data_xyz = data[np.array(["x", "y", "z"])]
    elif attr == "xyzi":
        data_xyz = data[np.array(["x", "y", "z", "i"])]
    else:
        raise NotImplementedError()
    data_xyz = rfn.structured_to_unstructured(data_xyz)  # Nx3

    # 只取一次回波数据
    data_echo_id = data["echo_id"]  # N
    data_xyz_first = data_xyz[data_echo_id == 1]  # Nx3

    # 去除NAN，按理说应该原始数据已经没有NAN了。这里做double check
    valid_idxs = ~np.isnan(data_xyz_first).any(axis=1)
    data_xyz_first = data_xyz_first[valid_idxs]

    pc = PointCloud.from_array(data_xyz_first, pcd_type, attr=attr)
    point_cloud_to_path(pc, save_path)
    return "success"

def to_pcd_nori_reader(nori_id, nr, save_path, ori_oss_path, pcd_type: str = "binary", attr="xyz", lidar_ids=None):
    try:
        # with nori_open(nori_path_replace(nori_path), "r") as nf:
        lidar_data = nr.get(nori_id)
        with refile.smart_open(ori_oss_path, "wb") as f:
            f.write(lidar_data)
    except Exception as e:
        logger.error(e)
        return None
    data = np.load(io.BytesIO(lidar_data))
    if lidar_ids:
        data = lidar_utils.filter_fuser_lidar_by_lidar_ids(data, lidar_ids)
    if attr == "xyz":
        data_xyz = data[np.array(["x", "y", "z"])]
    elif attr == "xyzi":
        data_xyz = data[np.array(["x", "y", "z", "i"])]
    else:
        raise NotImplementedError()
    data_xyz = rfn.structured_to_unstructured(data_xyz)  # Nx3

    # 只取一次回波数据
    data_echo_id = data["echo_id"]  # N
    data_xyz_first = data_xyz[data_echo_id == 1]  # Nx3

    # 去除NAN，按理说应该原始数据已经没有NAN了。这里做double check
    valid_idxs = ~np.isnan(data_xyz_first).any(axis=1)
    data_xyz_first = data_xyz_first[valid_idxs]

    pc = PointCloud.from_array(data_xyz_first, pcd_type, attr=attr)
    point_cloud_to_path(pc, save_path)
    return "success"

# lidar原始数据直接保存bin文件
def to_bin(nori_id, nori_path, save_path):
    # try:
    with nori_open(nori_path_replace(nori_path), "r") as nf:
        lidar_data = nf.get(nori_id)
    # except Exception as e:
    #     logger.error(e)
    #     return
    data = io.BytesIO(lidar_data)
    with smart_open(save_path, "wb") as f:
        f.write(data.read())


# to_pcd(nori_id="128034314,10007f2150ec",nori_path="s3://tf-23q4-shared-data/parsed_data/car_9/20231001/ppl_bag_20231001_072120_det/v0_231008_173225/all_nori/fuser_lidar.nori",save_path="/data/oss_visualization_fastapi/static/128034314,10007f2150ec.pcd")