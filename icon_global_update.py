#!/usr/bin/env python3
"""DWD open data ICON（全球・正二十面体格子）-> packed binary -> R2。

Why (2026-09-29): 東京・北米など ICON-EU の外に ICON を出す（天気比較の「東京向け全球モデル」
AIFS → ICON 全球 → GDPS の 2 本目）。データは CC BY 4.0（DWD の書面 monorepo
10_Planning/ops_dwd_trademark_APP278269-B.md §4.5: "freely available for any purpose"・ICON の
名称可・DWD はアプリ名/サブタイトルに入れない）。出典は Worker が配る。

Source: https://opendata.dwd.de/weather/nwp/icon/grib/{HH}/{param}/
        icon_global_icosahedral_single-level_{YYYYMMDDHH}_{step:03d}_{PARAM}.grib2.bz2
Grid:   正二十面体格子 R03B07（約 13 km）2,949,120 セル。緯度経度格子の版は配布されていない。
        セル中心は time-invariant の CLAT/CLON（度）。
Steps:  00/06/12/18Z。+78h まで毎時・以降 3 時間刻み。ここでは IFS と同じ 3 時間刻み +120h
        （41 ステップ）に揃える（容量）。

★ 補間しない（運搬だけ）: 値は ICON のセルそのもの。点→セルは 0.1° の索引表
  （icong_idx.bin: uint32 LE、[iy][ix]、iy=round((90-lat)/0.05)、ix=round((lon+180)/0.05)%7200、約 104 MB）で
  最寄りのセル中心を引く（球面上の直線距離の最小）。0.1° だと札幌で 2.5 km のセルを外して 10.4 km の
  セルを引いた（2026-09-29 実測）ので 0.05°。さらに Worker は点を囲む索引 4 点のセルを候補にし、
  セル中心座標（icong_ll.bin: float32 LE [lat, lon] × NCELL）で大円距離が最小のものを選ぶ
  （索引 1 点だけだと東京・シドニーで隣のセルを引いた）。
★ 容量: セル連続（NCELL, H, V）を float16 LE で保存（約 1.2 GB）。float16 の分解能は 30℃で 0.016、
  降水 64mm 付近で 0.03 と、Worker の 0.1 丸めより細かい。
単位は GRIB の units を見て、想定外なら止める（AIFS で単位が IFS と違った教訓）。
"""
import bz2
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np
import xarray as xr

BASE = "https://opendata.dwd.de/weather/nwp/icon/grib"
NCELL = 2949120
STEPS = list(range(0, 121, 3))[: int(os.environ.get("STEPS_LIMIT", "999"))]  # 41 steps
VARS = ["temp", "wind", "windDir", "precip", "cloud"]
PARAMS = {"2t": "T_2M", "10u": "U_10M", "10v": "V_10M", "tp": "TOT_PREC", "tcc": "CLCT"}
UNITS = {"T_2M": ("K",), "U_10M": ("m s**-1",), "V_10M": ("m s**-1",),
         "TOT_PREC": ("kg m**-2",), "CLCT": ("%",), "CLAT": ("Degree N",), "CLON": ("Degree E",)}
IDX_RES, IDX_NLAT, IDX_NLON = 0.05, 3601, 7200
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")  # icong.* として同居
OUT = os.environ.get("OUT_DIR", "/tmp")
UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}


def file_url(date: str, hh: str, step: int, param: str) -> str:
    return (f"{BASE}/{hh}/{param.lower()}/icon_global_icosahedral_single-level_"
            f"{date}{hh}_{step:03d}_{param}.grib2.bz2")


def const_url(date: str, hh: str, param: str) -> str:
    return (f"{BASE}/{hh}/{param.lower()}/icon_global_icosahedral_time-invariant_"
            f"{date}{hh}_{param}.grib2.bz2")


def http(url: str, method: str = "GET") -> bytes:
    err = None
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers=dict(UA), method=method)
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            err = e
        except Exception as e:
            err = e
        import time
        time.sleep(5 * (attempt + 1))
    raise err


def find_cycle() -> tuple[str, str] | None:
    """Newest 00/06/12/18Z cycle whose LAST step is published."""
    now = datetime.now(timezone.utc)
    for back in range(0, 12):
        t = now - timedelta(hours=6 * back)
        date, hh = t.strftime("%Y%m%d"), f"{(t.hour // 6) * 6:02d}"
        try:
            for p in ("TOT_PREC", "T_2M", "CLCT"):
                http(file_url(date, hh, STEPS[-1], p), method="HEAD")
            return date, hh
        except Exception:
            continue
    return None


def read_grib(raw_bz2: bytes, tag: str, param: str) -> np.ndarray:
    path = f"{OUT}/ig_{tag}_{param}.grib2"
    with open(path, "wb") as f:
        f.write(bz2.decompress(raw_bz2))
    ds = xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""})
    name = list(ds.data_vars)[0]
    v = ds[name]
    units = v.attrs.get("GRIB_units", "")
    arr = np.asarray(v.values, dtype=np.float32)
    ds.close()
    os.unlink(path)
    if units not in UNITS[param]:
        raise RuntimeError(f"{param}: unexpected units {units!r}")
    if arr.shape != (NCELL,):
        raise RuntimeError(f"{param}: unexpected grid {arr.shape}")
    return arr


def build_index(clat: np.ndarray, clon: np.ndarray) -> np.ndarray:
    from scipy.spatial import cKDTree

    def xyz(la, lo):
        la, lo = np.radians(la), np.radians(lo)
        return np.stack([np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)], axis=-1)

    tree = cKDTree(xyz(clat.astype(np.float64), clon.astype(np.float64)))
    lat = 90.0 - np.arange(IDX_NLAT) * IDX_RES
    lon = -180.0 + np.arange(IDX_NLON) * IDX_RES
    LA, LO = np.meshgrid(lat, lon, indexing="ij")
    _, idx = tree.query(xyz(LA.ravel(), LO.ravel()), workers=-1)
    return idx.astype("<u4").reshape(IDX_NLAT, IDX_NLON)


def main() -> int:
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no ICON global cycle available", file=sys.stderr)
        return 1
    date, hh = cyc
    H, V = len(STEPS), len(VARS)
    print(f"cycle: {date} {hh}Z, {H} steps, {NCELL} cells")

    clat = read_grib(http(const_url(date, hh, "CLAT")), "c", "CLAT")
    clon = read_grib(http(const_url(date, hh, "CLON")), "c", "CLON")
    idx = build_index(clat, clon)
    # 索引の自己点検: 既知の点で、索引で引いたセルが総当たりの最寄りセルより 3 km 以上遠くないこと。
    def km(c, la, lo):
        h = (np.sin(np.radians(clat[c] - la) / 2) ** 2 + np.cos(np.radians(la)) * np.cos(np.radians(clat[c]))
             * np.sin(np.radians(clon[c] - lo) / 2) ** 2)
        return 2 * 6371 * np.arcsin(np.sqrt(h))
    for la, lo in [(35.68, 139.77), (43.06, 141.35), (41.88, -87.63), (-33.87, 151.21), (64.18, -51.72), (0.0, 0.0)]:
        c = idx[round((90 - la) / IDX_RES), round((lo + 180) / IDX_RES) % IDX_NLON]
        best = float(km(np.arange(NCELL), la, lo).min())
        if km(c, la, lo) - best > 3:
            raise RuntimeError(f"index self-check failed at {la},{lo}: {km(c, la, lo):.1f} km vs best {best:.1f} km")
    idx.tofile(f"{OUT}/icong_idx.bin")
    np.stack([clat, clon], axis=1).astype("<f4").tofile(f"{OUT}/icong_ll.bin")

    data = np.full((V, H, NCELL), np.nan, np.float32)

    def one(i_step):
        i, step = i_step
        try:
            d = {k: read_grib(http(file_url(date, hh, step, p)), str(step), p) for k, p in PARAMS.items()}
        except Exception as e:
            print(f"warn step {step}: {e}", file=sys.stderr)
            return
        u, v = d["10u"], d["10v"]
        data[0, i] = d["2t"] - 273.15
        data[1, i] = np.sqrt(u * u + v * v)
        data[2, i] = np.degrees(np.arctan2(-u, -v)) % 360
        data[3, i] = d["tp"]          # kg/m2 == mm、初期時刻からの積算
        data[4, i] = d["tcc"]         # %

    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(one, list(enumerate(STEPS))))

    good = int(np.isfinite(data[0]).any(axis=1).sum())
    print(f"steps with data: {good}/{H}")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    # 脱積算: tp[i] は step[i-1]→step[i] の区間の量（終端置き）。Worker の expandToHourly が
    # 区間の開始側へ付け替えて mm/h に等分する。
    tp = data[3]
    np.nan_to_num(tp, copy=False)
    for i in range(H - 1, 0, -1):
        tp[i] -= tp[i - 1]
    np.clip(tp, 0, None, out=tp)

    # (V, H, N) -> (N, H, V) float16、セル単位のチャンクで書く（メモリを抑える）
    with open(f"{OUT}/icong.bin", "wb") as f:
        CH = 262144
        for a in range(0, NCELL, CH):
            f.write(np.ascontiguousarray(data[:, :, a:a + CH].transpose(2, 1, 0)).astype("<f2").tobytes())

    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "model": "ICON",
        "nCell": NCELL, "dtype": "f2",
        "ll": "icong_ll.bin", "idxRes": IDX_RES, "idxNLat": IDX_NLAT, "idxNLon": IDX_NLON, "idxLat0": 90.0, "idxLon0": -180.0,
        "fhours": STEPS, "vars": VARS,
    }
    with open(f"{OUT}/icong_meta.json", "w") as f:
        json.dump(meta, f)
    print(f"packed {os.path.getsize(f'{OUT}/icong.bin')/1e6:.0f} MB, cell={H*V*2} B")

    if os.environ.get("DRY_RUN"):
        return 0
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    # データ → 索引 → メタの順（メタが最後。Worker はメタの cycle/fhours で読む）
    s3.upload_file(f"{OUT}/icong.bin", BUCKET, "icong.bin")
    s3.upload_file(f"{OUT}/icong_idx.bin", BUCKET, "icong_idx.bin")
    s3.upload_file(f"{OUT}/icong_ll.bin", BUCKET, "icong_ll.bin")
    s3.upload_file(f"{OUT}/icong_meta.json", BUCKET, "icong_meta.json")
    print("uploaded icong.bin + icong_idx.bin + icong_ll.bin + icong_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
