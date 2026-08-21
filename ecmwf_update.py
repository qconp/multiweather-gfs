#!/usr/bin/env python3
"""ECMWF IFS open data -> packed binary -> R2, mirroring gfs-pipeline/gfs_update.py.

Why this shape: the Worker reads ONE cell with a byte-range request, so the array
must be laid out cell-contiguous: (NLAT, NLON, H, V) float32 little-endian.
Anything else turns a 1 KB read into a 200 MB read.

Why range reads on the source too: each ECMWF step file is ~145 MB but carries 184
messages, and we need six. The sibling .index file is JSONL with _offset/_length per
message, so we pull ~4 MB per step instead of 145 MB. Without this the job would move
7 GB per run.

Licence: ECMWF open data is CC BY 4.0 plus ECMWF's Terms of Use. Attribution is a
condition of the grant and is emitted by the Worker, not here — see fetchEcmwf.
Only the IFS/AIFS open-data products are CC BY; other ECMWF datasets (S2S and
others) are CC BY-NC-SA. Do not point this script at anything outside
data.ecmwf.int/forecasts/.../ifs/.
"""
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import xarray as xr

BASE = "https://data.ecmwf.int/forecasts"
RES = 0.5                      # 0.25° 配信を 1つおきに間引く（GFS 列と粒度を揃える）
NLAT, NLON = 361, 720
STEPS = list(range(0, 145, 3))  # ECMWF oper は 3 時間刻み。+144h まで
VARS = ["temp", "wind", "windDir", "precip", "cloud"]
PARAMS = ["2t", "10u", "10v", "tp", "tcc"]
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")  # 既存バケットに ecmwf.* を同居
UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}


def step_url(date: str, hh: str, step: int, ext: str) -> str:
    return f"{BASE}/{date}/{hh}z/ifs/0p25/oper/{date}{hh}0000-{step}h-oper-fc.{ext}"


def http(url: str, rng: tuple[int, int] | None = None) -> bytes:
    req = urllib.request.Request(url, headers=dict(UA))
    if rng:
        req.add_header("Range", f"bytes={rng[0]}-{rng[1]}")
    with urllib.request.urlopen(req, timeout=180) as r:
        return r.read()


def find_cycle() -> tuple[str, str] | None:
    """Newest cycle whose LAST step is published. Publication lags the run by ~7-9h."""
    now = datetime.now(timezone.utc)
    for back in range(0, 30):
        t = now - timedelta(hours=6 * back)
        date, hh = t.strftime("%Y%m%d"), f"{(t.hour // 6) * 6:02d}"
        try:
            req = urllib.request.Request(
                step_url(date, hh, STEPS[-1], "index"), headers=dict(UA), method="HEAD")
            with urllib.request.urlopen(req, timeout=30) as r:
                if r.status == 200:
                    return date, hh
        except Exception:
            continue
    return None


def fetch_step(date: str, hh: str, step: int) -> dict[str, np.ndarray]:
    """Pull only the messages we need, using the .index byte offsets."""
    wanted = {}
    for line in http(step_url(date, hh, step, "index")).decode().splitlines():
        try:
            j = json.loads(line)
        except Exception:
            continue
        p = j.get("param")
        if p in PARAMS and p not in wanted:
            wanted[p] = (int(j["_offset"]), int(j["_length"]))
    missing = [p for p in PARAMS if p not in wanted]
    if missing:
        raise RuntimeError(f"step {step}: index missing {missing}")

    grib_url = step_url(date, hh, step, "grib2")
    out: dict[str, np.ndarray] = {}
    for p, (off, ln) in wanted.items():
        blob = http(grib_url, (off, off + ln - 1))
        path = f"/tmp/e_{step:03d}_{p}.grib2"
        with open(path, "wb") as f:
            f.write(blob)
        ds = xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""})
        # 1 メッセージなので変数はひとつ。名前は param により異なる（t2m/u10/v10/tp/tcc）。
        name = list(ds.data_vars)[0]
        arr = np.asarray(ds[name].values, dtype=np.float32)

        # ★ 軸の向きを揃える。Worker は iy=round((90-lat)/res), ix=round(lon%360/res) で
        #   引くので、緯度は降順（先頭が北極）、経度は 0 始まりでなければならない。
        #   ズレていても落ちないため、南北反転や経度半周ズレが静かに配信される。
        #   実測: 同じファイルでも tp だけ経度が -180..180 で返る（他は 0..360）。
        #   よって弾かずに正規化する。
        lats = np.asarray(ds["latitude"].values, dtype=np.float64)
        lons = np.asarray(ds["longitude"].values, dtype=np.float64)
        if arr.shape != (721, 1440):
            raise RuntimeError(f"{p}: unexpected grid {arr.shape}, expected (721, 1440)")

        if lats[0] < lats[-1]:                      # 昇順なら反転して降順へ
            arr = arr[::-1, :]
            lats = lats[::-1]
        if not (89.9 < lats[0] <= 90.0 and -90.0 <= lats[-1] < -89.9):
            raise RuntimeError(f"{p}: latitude runs {lats[0]}..{lats[-1]}, expected 90..-90")

        lons360 = np.mod(lons, 360.0)               # -180..180 も 0..360 も同じ土俵に乗せる
        order = np.argsort(lons360, kind="stable")
        arr = arr[:, order]
        lons360 = lons360[order]
        if not (-0.01 < lons360[0] < 0.01 and 359.0 < lons360[-1] < 360.0):
            raise RuntimeError(
                f"{p}: longitude runs {lons360[0]}..{lons360[-1]} after normalising")

        out[p] = arr[::2, ::2]
        ds.close()
        os.unlink(path)
    return out


def main() -> int:
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no ECMWF cycle available", file=sys.stderr)
        return 1
    date, hh = cyc
    print(f"cycle: {date} {hh}Z, {len(STEPS)} steps")

    H = len(STEPS)
    temp = np.full((H, NLAT, NLON), np.nan, np.float32)
    spd = np.full_like(temp, np.nan)
    wdir = np.full_like(temp, np.nan)
    tp_acc = np.full_like(temp, np.nan)   # tp は step 0 からの積算（単位 m）
    cloud = np.full_like(temp, np.nan)

    def one(i_step):
        i, step = i_step
        try:
            d = fetch_step(date, hh, step)
        except Exception as e:
            print(f"warn step {step}: {e}", file=sys.stderr)
            return
        temp[i] = d["2t"] - 273.15
        u, v = d["10u"], d["10v"]
        spd[i] = np.sqrt(u * u + v * v)
        wdir[i] = np.degrees(np.arctan2(-u, -v)) % 360
        tp_acc[i] = d["tp"] * 1000.0            # m -> mm
        cloud[i] = d["tcc"] * 100.0             # 0..1 -> %

    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(one, list(enumerate(STEPS))))

    # 脱積算。ECMWF の tp はバケットではなく step 0 からの通し積算なので単純差分でよい
    # （GFS の 6h バケット扱いをそのまま持ち込むと二重に引いてしまう）。
    precip = np.zeros_like(tp_acc)
    precip[0] = np.nan_to_num(tp_acc[0])
    for i in range(1, H):
        precip[i] = np.nan_to_num(tp_acc[i]) - np.nan_to_num(tp_acc[i - 1])
    precip = np.clip(precip, 0, None)

    arr = np.stack([temp, spd, wdir, precip, cloud], axis=-1)   # (H,NLAT,NLON,V)
    arr = np.transpose(arr, (1, 2, 0, 3)).astype("<f4")         # (NLAT,NLON,H,V)
    with open("/tmp/ecmwf.bin", "wb") as f:
        f.write(arr.tobytes())

    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "nLat": NLAT, "nLon": NLON, "res": RES,
        "fhours": STEPS, "vars": VARS,
    }
    with open("/tmp/ecmwf_meta.json", "w") as f:
        json.dump(meta, f)

    # ★ 中身が空でも「成功」で終えないこと。fetch_step の失敗は警告にして続行する作りなので、
    #   全ステップ落ちても配列が NaN のまま処理が最後まで通ってしまう（初回実行で実際に発生）。
    #   R2 に NaN だらけのファイルを上げると、アプリには列が出るのに値が全部空になる。
    good = int(np.isfinite(temp).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H}")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload",
              file=sys.stderr)
        return 1

    size = os.path.getsize("/tmp/ecmwf.bin")
    print(f"packed {size/1e6:.0f} MB, cell={H*len(VARS)*4} B")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    s3.upload_file("/tmp/ecmwf.bin", BUCKET, "ecmwf.bin")
    s3.upload_file("/tmp/ecmwf_meta.json", BUCKET, "ecmwf_meta.json")
    print("uploaded ecmwf.bin + ecmwf_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
