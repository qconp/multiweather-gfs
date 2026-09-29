#!/usr/bin/env python3
"""ECCC MSC Datamart GDPS（カナダ環境・気候変動省の全球モデル・0.15°）-> packed binary -> R2。

Why (2026-09-29): 天気比較の「東京向け全球モデル」AIFS → ICON 全球 → GDPS の 3 本目。
ライセンス: ECCC Data Services End-use Licence 2.1.1（https://eccc-msc.github.io/open-data/licence/readme_en/）
— 商用可・"Data Source: Environment and Climate Change Canada"・公式の名称/紋章/ロゴで公認を示唆しない。
出典は Worker が配る。利用方針（https://eccc-msc.github.io/open-data/usage-policy/readme_en/）:
ディレクトリ一覧で新着を探さない（既知のファイル名に HEAD）・1 日 86,400 リクエスト未満・no-cache を送らない。

Source: https://dd.weather.gc.ca/{YYYYMMDD}/WXO-DD/model_gdps/15km/{HH}/{step:03d}/
        {YYYYMMDD}T{HH}Z_MSC_GDPS_{Var}_LatLon0.15_PT{step:03d}H.grib2
Grid:   0.15° 全球 1201×2400（緯度 −90→90・経度 0→359.85）。00/12Z、毎時 +240h。
        ここでは 3 時間刻み +120h（41 ステップ・IFS/ICON 全球と同じ刻み）。
★ 補間しない（格子点そのまま）。容量のため float16（セル連続・約 1.2 GB）。
★ 降水・雲量は GRIB の units がカナダのローカル表で読めないため、WMO の番号で検査する
  （0/1/8 総降水量 kg m-2・初期時刻からの積算、0/6/1 全雲量 %）。想定外なら止める。
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

import sparse  # 陸と沿岸 50 km だけを書く（R2 容量対策 2026-09-30）
import xarray as xr

RES = 0.15
NLAT, NLON = 1201, 2400
STEPS = list(range(0, 121, 3))[: int(os.environ.get("STEPS_LIMIT", "999"))]
VARS = ["temp", "wind", "windDir", "precip", "cloud"]
# 変数名 → (ファイルの Var, 期待する units か WMO 番号 (discipline, category, number))
PARAMS = {
    "temp": ("AirTemp_AGL-2m", "K"),
    "wind": ("WindSpeed_AGL-10m", "m s**-1"),
    "windDir": ("WindDir_AGL-10m", "Degree true"),
    "precip": ("Precip-Accum_Sfc", (0, 1, 8)),
    "cloud": ("TotalCloudCover_Sfc", (0, 6, 1)),
}
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")  # gdps.* として同居
OUT = os.environ.get("OUT_DIR", "/tmp")
UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}


def file_url(date: str, hh: str, step: int, var: str) -> str:
    return (f"https://dd.weather.gc.ca/{date}/WXO-DD/model_gdps/15km/{hh}/{step:03d}/"
            f"{date}T{hh}Z_MSC_GDPS_{var}_LatLon0.15_PT{step:03d}H.grib2")


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
        time.sleep(5 * (attempt + 1))
    raise err


def find_cycle() -> tuple[str, str] | None:
    """Newest 00/12Z cycle whose LAST step exists (HEAD on known names; no directory listing)."""
    now = datetime.now(timezone.utc)
    for back in range(0, 8):
        t = now - timedelta(hours=12 * back)
        date, hh = t.strftime("%Y%m%d"), f"{(t.hour // 12) * 12:02d}"
        try:
            for v, _ in PARAMS.values():
                http(file_url(date, hh, STEPS[-1], v), method="HEAD")
            return date, hh
        except Exception:
            continue
    return None


def fetch(date: str, hh: str, step: int, key: str) -> np.ndarray:
    var, expect = PARAMS[key]
    path = f"{OUT}/gd_{step:03d}_{key}.grib2"
    with open(path, "wb") as f:
        f.write(http(file_url(date, hh, step, var)))
    if isinstance(expect, tuple):
        import eccodes
        with open(path, "rb") as fh:
            h = eccodes.codes_grib_new_from_file(fh)
            got = tuple(eccodes.codes_get(h, k) for k in ("discipline", "parameterCategory", "parameterNumber"))
            eccodes.codes_release(h)
        if got != expect:
            raise RuntimeError(f"{var}: unexpected parameter {got}, expected {expect}")
    ds = xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""})
    v = ds[list(ds.data_vars)[0]]
    if isinstance(expect, str) and v.attrs.get("GRIB_units") != expect:
        raise RuntimeError(f"{var}: unexpected units {v.attrs.get('GRIB_units')!r}")
    arr = np.asarray(v.values, dtype=np.float32)
    lats = np.asarray(ds["latitude"].values)
    lons = np.asarray(ds["longitude"].values)
    ds.close()
    os.unlink(path)
    if arr.shape != (NLAT, NLON):
        raise RuntimeError(f"{var}: unexpected grid {arr.shape}")
    # Worker は iy=round((90-lat)/res)（北から）、ix=round(lon%360/res)。向きはファイルを信用せず揃える。
    if lats[0] < lats[-1]:
        arr, lats = arr[::-1, :], lats[::-1]
    if abs(lats[0] - 90) > 1e-3 or abs(lats[-1] + 90) > 1e-3:
        raise RuntimeError(f"{var}: latitude runs {lats[0]}..{lats[-1]}")
    if abs(np.mod(lons[0], 360)) > 1e-3 or abs(lons[1] - lons[0] - RES) > 1e-3:
        raise RuntimeError(f"{var}: longitude starts {lons[0]} step {lons[1] - lons[0]}")
    return arr


def main() -> int:
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no GDPS cycle available", file=sys.stderr)
        return 1
    date, hh = cyc
    H, V = len(STEPS), len(VARS)
    print(f"cycle: {date} {hh}Z, {H} steps, grid {NLAT}x{NLON} @ {RES} deg")
    data = np.full((V, H, NLAT, NLON), np.nan, np.float32)

    def one(i_step):
        i, step = i_step
        try:
            for k, key in enumerate(VARS):
                # 初期時刻（step 0）の積算降水ファイルは配布されない（積算 0）。
                data[k, i] = 0.0 if (step == 0 and key == "precip") else fetch(date, hh, step, key)
        except Exception as e:
            print(f"warn step {step}: {e}", file=sys.stderr)
            data[:, i] = np.nan

    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(one, list(enumerate(STEPS))))

    good = int(np.isfinite(data[0]).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H}")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    data[0] -= 273.15
    # 脱積算: tp[i] は step[i-1]→step[i] の区間の量（終端置き）。Worker の expandToHourly が開始側へ。
    tp = data[3]
    np.nan_to_num(tp, copy=False)
    for i in range(H - 1, 0, -1):
        tp[i] -= tp[i - 1]
    np.clip(tp, 0, None, out=tp)

    KEEP = sparse.keep_regular(NLAT, NLON, RES)
    sparse.slot_index(KEEP).tofile(f"{OUT}/gdps_s_idx.bin")
    with open(f"{OUT}/gdps.bin", "wb") as f:
        for iy in range(NLAT):
            f.write(np.ascontiguousarray(data[:, :, iy, :].transpose(2, 1, 0)[KEEP[iy]]).astype("<f2").tobytes())

    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "model": "GDPS",
        "nLat": NLAT, "nLon": NLON, "res": RES, "dtype": "f2", "bin": "gdps_s.bin", "idx": "gdps_s_idx.bin", "nSlot": int(KEEP.sum()),
        "fhours": STEPS, "vars": VARS,
    }
    with open(f"{OUT}/gdps_meta.json", "w") as f:
        json.dump(meta, f)
    print(f"packed {os.path.getsize(f'{OUT}/gdps.bin')/1e6:.0f} MB, cell={H*V*2} B")
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
    sparse.publish(s3, BUCKET, f"{OUT}/gdps.bin", f"{OUT}/gdps_s_idx.bin", f"{OUT}/gdps_meta.json", "gdps")
    print("uploaded gdps.bin + gdps_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
