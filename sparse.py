"""陸と沿岸 50 km だけを残す（R2 無料枠 10GB の対策 2026-09-30）。

keepmask_0p05_50km.npz（tools/build_keepmask.py で Natural Earth から作成・リポジトリに同梱）を読み、
各モデルの格子点が「残す範囲」かを返す。外洋の格子は書かない。Worker は索引（uint32、外洋は
0xFFFFFFFF）で詰めた位置を引く（multi_weather_api の sparseSlot）。
"""
import os
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
_Z = np.load(os.path.join(_HERE, "keepmask_0p05_50km.npz"))
_RES = float(_Z["res"])
_NLAT, _NLON = int(_Z["nlat"]), int(_Z["nlon"])
MASK = np.unpackbits(_Z["keep"], axis=1)[:, :_NLON].astype(bool)
NONE = np.uint32(0xFFFFFFFF)


def keep_at(lat, lon):
    """lat/lon（度、配列可）が残す範囲か。"""
    iy = np.clip(np.rint((90.0 - np.asarray(lat)) / _RES).astype(np.int64), 0, _NLAT - 1)
    ix = np.rint((np.asarray(lon) + 180.0) / _RES).astype(np.int64) % _NLON
    return MASK[iy, ix]


def keep_regular(nlat, nlon, res):
    """全球の等間隔格子（北 90° から南へ・経度 0° から東へ。Worker の既定の並び）の残す範囲 (nlat, nlon)。"""
    lat = 90.0 - np.arange(nlat) * res
    lon = np.arange(nlon) * res
    LA, LO = np.meshgrid(lat, lon, indexing="ij")
    return keep_at(LA, LO)


def slot_index(keep):
    """残す範囲（任意形の bool）→ 詰めた位置の索引（uint32 LE、外洋は 0xFFFFFFFF）。"""
    flat = keep.ravel()
    idx = np.full(flat.shape, NONE, dtype="<u4")
    idx[flat] = np.arange(int(flat.sum()), dtype="<u4")
    return idx


def publish(s3, bucket, local_bin, local_idx, local_meta, name):
    """詰めた版を別名で上げ、最後にメタ（bin/idx を指す）で切り替え、古い全格子ファイルを消す。"""
    s3.upload_file(local_bin, bucket, f"{name}_s.bin")
    s3.upload_file(local_idx, bucket, f"{name}_s_idx.bin")
    s3.upload_file(local_meta, bucket, f"{name}_meta.json")
    try:
        s3.delete_object(Bucket=bucket, Key=f"{name}.bin")
    except Exception as e:  # 無ければそれでよい
        print(f"note: delete {name}.bin: {e}")
