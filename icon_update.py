#!/usr/bin/env python3
"""DWD open data ICON-EU -> packed binary -> R2, mirroring ecmwf_update.py.

Why this exists (2026-09-29): the owner decided to bring the ICON model back into the
weather-comparison app under the written terms DWD gave (monorepo:
10_Planning/ops_dwd_trademark_APP278269-B.md §4.5). The data is CC BY 4.0 and free for
any purpose; the model name "ICON" may be used; attribution is emitted by the Worker
(fetchIcon), never by this script or the app. The names "DWD" / "Deutscher
Wetterdienst" never go into the app name or subtitle.

Source: https://opendata.dwd.de/weather/nwp/icon-eu/grib/{HH}/{param}/
        icon-eu_europe_regular-lat-lon_single-level_{YYYYMMDDHH}_{step:03d}_{PARAM}.grib2.bz2
Grid:   regular lat-lon 0.0625 deg, lat 29.5..70.5 (657 rows), lon -23.5..62.5 (1377 cols).
Steps:  main cycles 00/06/12/18Z: hourly to +78h, then 3-hourly to +120h (93 steps).
        The 03/09/15/21Z cycles only reach +30h and are skipped.

Output layout is cell-contiguous (NLAT, NLON, H, V) float32 LE so the Worker reads one
cell with a byte-range request. The grid is subsampled every 2nd point (0.125 deg,
~14 km) — that is a modification and the Worker's attribution says so. The regional
grid carries lat0/lon0 in the meta so the Worker can refuse points outside it instead
of clamping (packedGridIndex).
"""
import bz2
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import xarray as xr

BASE = "https://opendata.dwd.de/weather/nwp/icon-eu/grib"
SRC_RES = 0.0625
SRC_NLAT, SRC_NLON = 657, 1377
LAT0, LON0 = 70.5, -23.5            # north edge / west edge (source grid)
SUB = 2                             # 0.0625 -> 0.125 deg
RES = SRC_RES * SUB
NLAT = (SRC_NLAT + SUB - 1) // SUB  # 329
NLON = (SRC_NLON + SUB - 1) // SUB  # 689
STEPS = list(range(0, 79)) + list(range(81, 121, 3))  # 93 steps
VARS = ["temp", "wind", "windDir", "precip", "cloud"]
PARAMS = {"2t": "T_2M", "10u": "U_10M", "10v": "V_10M", "tp": "TOT_PREC", "tcc": "CLCT"}
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")  # shares the bucket as icon.*
UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}


def file_url(date: str, hh: str, step: int, param: str) -> str:
    return (f"{BASE}/{hh}/{param.lower()}/icon-eu_europe_regular-lat-lon_single-level_"
            f"{date}{hh}_{step:03d}_{param}.grib2.bz2")


def http(url: str, method: str = "GET") -> bytes:
    req = urllib.request.Request(url, headers=dict(UA), method=method)
    with urllib.request.urlopen(req, timeout=180) as r:
        return r.read()


def find_cycle() -> tuple[str, str] | None:
    """Newest main cycle (00/06/12/18Z) whose LAST step is published (~3-4 h lag)."""
    now = datetime.now(timezone.utc)
    for back in range(0, 12):
        t = now - timedelta(hours=6 * back)
        date, hh = t.strftime("%Y%m%d"), f"{(t.hour // 6) * 6:02d}"
        try:
            http(file_url(date, hh, STEPS[-1], "TOT_PREC"), method="HEAD")
            http(file_url(date, hh, STEPS[-1], "T_2M"), method="HEAD")
            return date, hh
        except Exception:
            continue
    return None


def fetch_field(date: str, hh: str, step: int, param: str) -> np.ndarray:
    raw = bz2.decompress(http(file_url(date, hh, step, param)))
    path = f"/tmp/i_{step:03d}_{param}.grib2"
    with open(path, "wb") as f:
        f.write(raw)
    ds = xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""})
    name = list(ds.data_vars)[0]
    arr = np.asarray(ds[name].values, dtype=np.float32)
    lats = np.asarray(ds["latitude"].values, dtype=np.float64)
    lons = np.asarray(ds["longitude"].values, dtype=np.float64)
    ds.close()
    os.unlink(path)
    if arr.shape != (SRC_NLAT, SRC_NLON):
        raise RuntimeError(f"{param}: unexpected grid {arr.shape}")
    # Worker indexes iy=round((lat0-lat)/res) from the NORTH edge, ix from the WEST edge.
    # Normalise orientation instead of trusting the file (ecmwf_update.py learnt this).
    if lats[0] < lats[-1]:
        arr = arr[::-1, :]
        lats = lats[::-1]
    if abs(lats[0] - LAT0) > 1e-3 or abs(lats[-1] - 29.5) > 1e-3:
        raise RuntimeError(f"{param}: latitude runs {lats[0]}..{lats[-1]}")
    lons = np.where(lons > 180, lons - 360, lons)
    if abs(lons[0] - LON0) > 1e-3 or abs(lons[-1] - 62.5) > 1e-3:
        raise RuntimeError(f"{param}: longitude runs {lons[0]}..{lons[-1]}")
    return arr[::SUB, ::SUB]


def main() -> int:
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no ICON-EU main cycle available", file=sys.stderr)
        return 1
    date, hh = cyc
    print(f"cycle: {date} {hh}Z, {len(STEPS)} steps, grid {NLAT}x{NLON} @ {RES} deg")

    H, V = len(STEPS), len(VARS)
    shape = (H, NLAT, NLON)
    temp = np.full(shape, np.nan, np.float32)
    spd = np.full(shape, np.nan, np.float32)
    wdir = np.full(shape, np.nan, np.float32)
    tp = np.full(shape, np.nan, np.float32)
    cloud = np.full(shape, np.nan, np.float32)

    def one(i_step):
        i, step = i_step
        try:
            d = {k: fetch_field(date, hh, step, p) for k, p in PARAMS.items()}
        except Exception as e:
            print(f"warn step {step}: {e}", file=sys.stderr)
            return
        u, v = d["10u"], d["10v"]
        temp[i] = d["2t"] - 273.15
        spd[i] = np.sqrt(u * u + v * v)
        wdir[i] = np.degrees(np.arctan2(-u, -v)) % 360
        tp[i] = d["tp"]           # kg/m2 == mm, accumulated since step 0
        cloud[i] = d["tcc"]       # already %

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(one, list(enumerate(STEPS))))

    good = int(np.isfinite(temp).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H}")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    # De-accumulate: TOT_PREC is a running total since the run start; the Worker's
    # expandToHourly then splits the 3-hourly tail evenly into mm/h.
    np.nan_to_num(tp, copy=False)
    for i in range(H - 1, 0, -1):
        tp[i] -= tp[i - 1]
    np.clip(tp, 0, None, out=tp)

    with open("/tmp/icon.bin", "wb") as f:
        for iy in range(NLAT):
            # ★ float16（R2 無料枠 10GB の対策 2026-09-29。分解能は Worker の 0.1 丸めより細かい）。
            row = np.empty((NLON, H, V), dtype="<f2")
            row[:, :, 0] = temp[:, iy, :].T
            row[:, :, 1] = spd[:, iy, :].T
            row[:, :, 2] = wdir[:, iy, :].T
            row[:, :, 3] = tp[:, iy, :].T
            row[:, :, 4] = cloud[:, iy, :].T
            row.tofile(f)

    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "model": "ICON-EU",
        "nLat": NLAT, "nLon": NLON, "res": RES, "lat0": LAT0, "lon0": LON0,
        "dtype": "f2", "fhours": STEPS, "vars": VARS,
    }
    with open("/tmp/icon_meta.json", "w") as f:
        json.dump(meta, f)

    size = os.path.getsize("/tmp/icon.bin")
    print(f"packed {size/1e6:.0f} MB, cell={H*V*4} B")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    s3.upload_file("/tmp/icon.bin", BUCKET, "icon.bin")
    s3.upload_file("/tmp/icon_meta.json", BUCKET, "icon_meta.json")
    print("uploaded icon.bin + icon_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
