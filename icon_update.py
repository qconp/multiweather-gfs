#!/usr/bin/env python3
"""DWD open data ICON-EU / ICON-D2 -> packed sparse binary -> R2, mirroring ecmwf_update.py.

    python icon_update.py                 # ICON-EU（icon_*）
    python icon_update.py --model d2      # ICON-D2（icond2_*）
    python icon_update.py --local DIR     # R2 に上げず DIR に書く（手元の検証用）

Why this exists (2026-09-29): the owner decided to bring the ICON model back into the
weather-comparison app under the written terms DWD gave (monorepo:
10_Planning/ops_dwd_trademark_APP278269-B.md §4.5). The data is CC BY 4.0 and free for
any purpose; the model name "ICON" may be used; attribution is emitted by the Worker
(fetchIcon / fetchIconD2), never by this script or the app. The names "DWD" /
"Deutscher Wetterdienst" never go into the app name or subtitle.

ICON-EU  https://opendata.dwd.de/weather/nwp/icon-eu/grib/{HH}/{param}/
         icon-eu_europe_regular-lat-lon_single-level_{YYYYMMDDHH}_{step:03d}_{PARAM}.grib2.bz2
         regular lat-lon 0.0625 deg, lat 29.5..70.5 (657 rows), lon -23.5..62.5 (1377 cols).
         Main cycles 00/06/12/18Z: hourly to +78h, then 3-hourly to +120h (93 steps).
         The 03/09/15/21Z cycles only reach +30h and are skipped.
         ★ 2026-10-30 オーナー承認: 間引き（0.125°）をやめて原解像度 0.0625°（約 7 km）で配る。
           容量は陸と沿岸 50 km だけを書く（sparse.py）ことで抑える。
ICON-D2  https://opendata.dwd.de/weather/nwp/icon-d2/grib/{HH}/{param}/
         icon-d2_germany_regular-lat-lon_single-level_{YYYYMMDDHH}_{step:03d}_2d_{param}.grib2.bz2
         regular lat-lon 0.02 deg（約 2.2 km）, lat 43.18..58.08 (746), lon -3.94..20.34 (1215).
         The lat-lon product is the model's rotated domain written into a bounding box: the
         corners are missing values. We keep only the inner rectangle D2_CROP (all valid, with
         ≥ ~40 km margin to the lateral boundary) so the Worker can decide "inside or not" with
         the same rectangle and no R2 read (isInsideIconD2). Every 3 h (00..21Z), +48h hourly.

Output layout is cell-contiguous (slot, H, V) float16 LE, only the cells of the land+50 km
coast mask, so the Worker reads one cell with a byte-range request after a 4-byte index read
(sparseSlot). The regional grid carries lat0/lon0 in the meta so the Worker refuses points
outside it instead of clamping (packedGridIndex).
"""
import bz2
import json
import os
import sys
import urllib.error
import urllib.request
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

import sparse  # 陸と沿岸 50 km だけを書く（R2 容量対策）
import xarray as xr

UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")  # shares the bucket

# ★ wx = DWD 自身が配る天気コード WW（"Numeric instant weather interpretation (WMO)"・瞬時値）。
#   こちらで数値から天気を作らない（気象業務法・2026-10-01 決定、10_Planning/ops_weather_service_act.md）。
# ★ gust = DWD 自身の VMAX_10M（10 m の最大瞬間風速 m/s・その時刻までの出力間隔の最大）。そのまま運ぶ。
VARS = ["temp", "wind", "windDir", "precip", "cloud", "wx", "gust"]

MODELS = {
    "eu": {
        "name": "ICON-EU", "key": "icon",
        "base": "https://opendata.dwd.de/weather/nwp/icon-eu/grib",
        "file": "icon-eu_europe_regular-lat-lon_single-level_{date}{hh}_{step:03d}_{param}.grib2.bz2",
        "params": {"2t": "T_2M", "10u": "U_10M", "10v": "V_10M", "tp": "TOT_PREC", "tcc": "CLCT"},
        "optional": {"wx": "WW", "gust": "VMAX_10M"},
        "upper": True,
        "res": 0.0625, "src_nlat": 657, "src_nlon": 1377,
        "src_lat0": 70.5, "src_lat1": 29.5, "src_lon0": -23.5, "src_lon1": 62.5,
        "crop": None,
        "steps": list(range(0, 79)) + list(range(81, 121, 3)),  # 93 steps
        "cycle_hours": 6,                                       # 00/06/12/18Z
        "max_age_h": 30,
    },
    "d2": {
        "name": "ICON-D2", "key": "icond2",
        "base": "https://opendata.dwd.de/weather/nwp/icon-d2/grib",
        "file": "icon-d2_germany_regular-lat-lon_single-level_{date}{hh}_{step:03d}_2d_{param}.grib2.bz2",
        "params": {"2t": "t_2m", "10u": "u_10m", "10v": "v_10m", "tp": "tot_prec", "tcc": "clct"},
        "optional": {"wx": "ww", "gust": "vmax_10m"},
        "upper": False,
        "res": 0.02, "src_nlat": 746, "src_nlon": 1215,
        "src_lat0": 58.08, "src_lat1": 43.18, "src_lon0": -3.94, "src_lon1": 20.34,
        # 内側の矩形（北端・南端・西端・東端）。Worker の ICON_D2_DOMAIN と同じ値にすること。
        "crop": (56.5, 44.5, 0.5, 17.0),
        "steps": list(range(0, 49)),                            # 49 steps, hourly
        "cycle_hours": 3,                                       # 00..21Z
        "max_age_h": 12,
    },
}


def file_url(m: dict, date: str, hh: str, step: int, param: str) -> str:
    folder = param.lower()
    return f"{m['base']}/{hh}/{folder}/" + m["file"].format(date=date, hh=hh, step=step, param=param)


def http(url: str, method: str = "GET") -> bytes:
    err = None
    for attempt in range(4):
        req = urllib.request.Request(url, headers=dict(UA), method=method)
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code < 500:
                raise
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        time.sleep(3 * 2 ** attempt)
    raise err


def find_cycle(m: dict) -> tuple[str, str] | None:
    """Newest cycle whose LAST step is published."""
    now = datetime.now(timezone.utc)
    ch = m["cycle_hours"]
    for back in range(0, 24 // ch * 2):
        t = now - timedelta(hours=ch * back)
        date, hh = t.strftime("%Y%m%d"), f"{(t.hour // ch) * ch:02d}"
        try:
            http(file_url(m, date, hh, m["steps"][-1], m["params"]["tp"]), method="HEAD")
            http(file_url(m, date, hh, m["steps"][-1], m["params"]["2t"]), method="HEAD")
            return date, hh
        except Exception:
            continue
    return None


def grid(m: dict):
    """出力格子（crop 後）: (lat0, lon0, nlat, nlon, iy0, ix0)。"""
    res = m["res"]
    if not m["crop"]:
        return m["src_lat0"], m["src_lon0"], m["src_nlat"], m["src_nlon"], 0, 0
    n, s, w, e = m["crop"]
    iy0 = int(round((m["src_lat0"] - n) / res))
    iy1 = int(round((m["src_lat0"] - s) / res))
    ix0 = int(round((w - m["src_lon0"]) / res))
    ix1 = int(round((e - m["src_lon0"]) / res))
    return n, w, iy1 - iy0 + 1, ix1 - ix0 + 1, iy0, ix0


def read_grib(m: dict, path: str, step: int) -> np.ndarray:
    ds = xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""})
    name = list(ds.data_vars)[0]
    da = ds[name]
    # ★ ICON-D2 の tot_prec は 1 ファイルに 15 分ごとの 4 本（例: 005 = +5:00, 5:15, 5:30, 5:45）。
    #   正時（+step h ちょうど）の 1 本を選ぶ。先頭を盲目的に取らない。
    if "step" in da.dims:
        hrs = np.asarray(ds["step"].values).astype("timedelta64[m]").astype(np.int64)
        hit = np.where(hrs == step * 60)[0]
        if not len(hit):
            raise RuntimeError(f"{path}: no message at +{step}h (steps {hrs.tolist()} min)")
        da = da.isel(step=int(hit[0]))
    arr = np.asarray(da.values, dtype=np.float32)
    lats = np.asarray(ds["latitude"].values, dtype=np.float64)
    lons = np.asarray(ds["longitude"].values, dtype=np.float64)
    ds.close()
    if arr.shape != (m["src_nlat"], m["src_nlon"]):
        raise RuntimeError(f"{path}: unexpected grid {arr.shape}")
    # Worker indexes iy=round((lat0-lat)/res) from the NORTH edge, ix from the WEST edge.
    # Normalise orientation instead of trusting the file (ecmwf_update.py learnt this).
    if lats[0] < lats[-1]:
        arr = arr[::-1, :]
        lats = lats[::-1]
    if abs(lats[0] - m["src_lat0"]) > 1e-3 or abs(lats[-1] - m["src_lat1"]) > 1e-3:
        raise RuntimeError(f"{path}: latitude runs {lats[0]}..{lats[-1]}")
    lons = np.where(lons > 180, lons - 360, lons)
    if abs(lons[0] - m["src_lon0"]) > 1e-3 or abs(lons[-1] - m["src_lon1"]) > 1e-3:
        raise RuntimeError(f"{path}: longitude runs {lons[0]}..{lons[-1]}")
    _, _, nlat, nlon, iy0, ix0 = grid(m)
    return arr[iy0:iy0 + nlat, ix0:ix0 + nlon]


def fetch_field(m: dict, work: str, date: str, hh: str, step: int, param: str) -> np.ndarray:
    raw = bz2.decompress(http(file_url(m, date, hh, step, param)))
    path = f"{work}/i_{m['key']}_{step:03d}_{param}.grib2"
    with open(path, "wb") as f:
        f.write(raw)
    try:
        return read_grib(m, path, step)
    finally:
        os.unlink(path)


def main() -> int:
    args = sys.argv[1:]
    which = args[args.index("--model") + 1] if "--model" in args else "eu"
    m = MODELS[which]
    local = sparse.local_dir()
    out = local or "/tmp"
    work = os.path.join(out, "work") if local else "/tmp"
    os.makedirs(work, exist_ok=True)
    key = m["key"]

    cyc = find_cycle(m)
    if not cyc:
        print(f"ERROR: no {m['name']} cycle available", file=sys.stderr)
        return 1
    date, hh = cyc
    # ★ 古すぎる回は上げない（Worker は max_age で捨てるので、上げても列が消えるだけ）。
    _age = (datetime.now(timezone.utc) - datetime.strptime(date + hh, "%Y%m%d%H").replace(tzinfo=timezone.utc)).total_seconds() / 3600
    if _age > m["max_age_h"]:
        print(f"ERROR: newest cycle found {date} {hh}Z is {_age:.0f}h old — refusing to upload", file=sys.stderr)
        return 1
    lat0, lon0, NLAT, NLON, _, _ = grid(m)
    STEPS = m["steps"]
    RES = m["res"]
    print(f"{m['name']} cycle: {date} {hh}Z, {len(STEPS)} steps, grid {NLAT}x{NLON} @ {RES} deg")

    H, V = len(STEPS), len(VARS)
    shape = (H, NLAT, NLON)
    temp = np.full(shape, np.nan, np.float32)
    spd = np.full(shape, np.nan, np.float32)
    wdir = np.full(shape, np.nan, np.float32)
    tp = np.full(shape, np.nan, np.float32)
    cloud = np.full(shape, np.nan, np.float32)
    wx = np.full(shape, np.nan, np.float32)
    gust = np.full(shape, np.nan, np.float32)

    def one(i_step):
        i, step = i_step
        try:
            d = {k: fetch_field(m, work, date, hh, step, p) for k, p in m["params"].items()}
        except Exception as e:
            print(f"warn step {step}: {e}", file=sys.stderr)
            return
        u, v = d["10u"], d["10v"]
        temp[i] = d["2t"] - 273.15
        spd[i] = np.sqrt(u * u + v * v)
        wdir[i] = np.degrees(np.arctan2(-u, -v)) % 360
        tp[i] = d["tp"]           # kg/m2 == mm, accumulated since step 0
        cloud[i] = d["tcc"]       # already %
        # 無い時刻は NaN（他の項目は落とさない）。WW・VMAX_10M は step 0 が無い／0 のことがある。
        for k, arr in (("wx", wx), ("gust", gust)):
            if step == 0 and k == "gust":
                continue  # 初期時刻の最大瞬間風速は区間が無く意味を持たない
            try:
                arr[i] = fetch_field(m, work, date, hh, step, m["optional"][k])
            except Exception as e:
                print(f"note step {step} {k}: {e}", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(one, list(enumerate(STEPS))))

    good = int(np.isfinite(temp).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H}  (gust {int(np.isfinite(gust).any(axis=(1, 2)).sum())}, wx {int(np.isfinite(wx).any(axis=(1, 2)).sum())})")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    # De-accumulate: TOT_PREC is a running total since the run start; the Worker's
    # expandToHourly then splits the 3-hourly tail evenly into mm/h.
    np.nan_to_num(tp, copy=False)
    for i in range(H - 1, 0, -1):
        tp[i] -= tp[i - 1]
    np.clip(tp, 0, None, out=tp)

    # 残す格子 = 陸と沿岸 50 km ∩ モデルの値がある格子（ICON-D2 の四隅の欠損は持たない）。
    la = lat0 - np.arange(NLAT) * RES
    lo = lon0 + np.arange(NLON) * RES
    LA, LO = np.meshgrid(la, lo, indexing="ij")
    KEEP = sparse.keep_at(LA, LO) & np.isfinite(temp).any(axis=0)
    sparse.slot_index(KEEP).tofile(f"{out}/{key}_s_idx.bin")
    with open(f"{out}/{key}.bin", "wb") as f:
        for iy in range(NLAT):
            row = np.empty((NLON, H, V), dtype="<f2")
            for j, a in enumerate((temp, spd, wdir, tp, cloud, wx, gust)):
                row[:, :, j] = a[:, iy, :].T
            row[KEEP[iy]].tofile(f)

    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "model": m["name"],
        "nLat": NLAT, "nLon": NLON, "res": RES, "lat0": lat0, "lon0": lon0,
        "dtype": "f2", "bin": f"{key}_s.bin", "idx": f"{key}_s_idx.bin", "nSlot": int(KEEP.sum()),
        "fhours": STEPS, "vars": VARS,
    }
    with open(f"{out}/{key}_meta.json", "w") as f:
        json.dump(meta, f)

    size = os.path.getsize(f"{out}/{key}.bin")
    print(f"packed {size/1e6:.0f} MB ({int(KEEP.sum())}/{KEEP.size} cells), cell={H*V*2} B, idx {KEEP.size*4/1e6:.1f} MB")
    if local:
        print(f"--local: wrote {out}/{key}.bin, {key}_s_idx.bin, {key}_meta.json (not uploaded)")
        return 0

    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    # bin/idx を先に、meta を最後に（Worker は meta の bin/idx を見る）。旧 icon.bin（全格子）は消す。
    sparse.publish(s3, BUCKET, f"{out}/{key}.bin", f"{out}/{key}_s_idx.bin", f"{out}/{key}_meta.json", key)
    print(f"uploaded {key}_s.bin + {key}_s_idx.bin + {key}_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
