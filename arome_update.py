#!/usr/bin/env python3
"""Météo-France open data AROME 0.025° (SP1 package) -> packed sparse binary -> R2, mirroring icon2i_update.py.

    python arome_update.py                       # newest complete run -> R2
    python arome_update.py --local DIR           # write to DIR instead of R2 (local verification)
    python arome_update.py --local DIR --gribdir D --run 2026-11-08T21:00:00Z   # use already downloaded files

Why this exists (2026-11-09): owner policy "add each rich country's own open data, after checking
how the source must be credited". France is ~5% of iOS downloads and had no local source.

Licence (primary source: data.gouv.fr dataset "Paquets Arome - Résolution 0,025°",
  slug paquets-arome-resolution-0-025deg, organization Météo-France, license "lov2",
  checked 2026-11-09): Licence Ouverte / Open Licence 2.0 (Etalab) — commercial reuse allowed;
  credit the source and the date of last update; do not suggest endorsement or present modified
  data as Météo-France's own. Attribution (with the run time) is emitted by the Worker (fetchArome),
  never by this script or the app. The name "Météo-France" never goes into the app name, subtitle
  or keywords.

Source  https://meteofrance-pnt.s3.rbx.io.cloud.ovh.net/pnt/{RUN}/arome/0025/SP1/arome__0025__SP1__{GROUP}__{RUN}.grib2
        GROUP = 00H06H … 49H51H (9 files, ~50 MB each, ~470 MB per run, no key).
        Every 3-hourly cycle reaches +51h; a run is complete ~4h20m after its cycle (measured
        2026-11-08: 21Z 00H06H at 00:25Z, 49H51H at 01:18Z).
        GRIB2 regular_ll 1121x717, lat 55.4..37.5, lon -12..16 (stored as 348..16), 0.025°.
        Fields used: 2t, 10u, 10v, tp (kg m-2 accumulated since the run start -> de-accumulated here
        like ICON), max_i10fg (gust, max over the previous hour, carried as-is like ICON's VMAX_10M),
        total cloud cover (discipline 0 / category 6 / number 1, %, 9999 = missing).
        No weather code in the open files: none is written, and the Worker shows no weather symbols
        for this model (we never derive weather from model numbers).

We keep only the inner rectangle CROP (metropolitan France incl. Corsica + a margin) so the Worker
can decide "inside or not" with the same rectangle and no R2 read (isInsideArome).
Output layout is cell-contiguous (slot, H, V) float16 LE, land + 50 km coast cells only (sparse.py).
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import eccodes
import numpy as np

import sparse

UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")
BASE = "https://meteofrance-pnt.s3.rbx.io.cloud.ovh.net/pnt"
KEY = "arome"
VARS = ["temp", "wind", "windDir", "precip", "cloud", "gust"]
GROUPS = ["00H06H", "07H12H", "13H18H", "19H24H", "25H30H", "31H36H", "37H42H", "43H48H", "49H51H"]

SRC = {"nlat": 717, "nlon": 1121, "lat_n": 55.4, "lat_s": 37.5, "lon_w": -12.0, "lon_e": 16.0, "d": 0.025}
# 内側の矩形（北端・南端・西端・東端）。Worker の AROME_DOMAIN と同じ値にすること。
CROP = (51.3, 41.2, -5.3, 9.7)
STEPS = list(range(0, 52))
MAX_AGE_H = 12


def url(run: str, group: str) -> str:
    return f"{BASE}/{run}/arome/0025/SP1/arome__0025__SP1__{group}__{run}.grib2"


def exists(u: str) -> bool:
    try:
        req = urllib.request.Request(u, headers=dict(UA), method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status == 200
    except urllib.error.HTTPError:
        return False


def newest_complete_run(now: datetime) -> str | None:
    """最後の組（49H51H）まで公開済みの、いちばん新しい 3 時間ごとの回。"""
    t = now.replace(minute=0, second=0, microsecond=0)
    t -= timedelta(hours=t.hour % 3)
    for _ in range(6):
        run = t.strftime("%Y-%m-%dT%H:00:00Z")
        if exists(url(run, GROUPS[-1])):
            return run
        t -= timedelta(hours=3)
    return None


def download(u: str, path: str) -> None:
    """Stream to disk (never hold the file in memory)."""
    for attempt in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers=dict(UA)), timeout=300) as r, open(path, "wb") as f:
                while True:
                    b = r.read(8 << 20)
                    if not b:
                        break
                    f.write(b)
            return
        except Exception as e:  # noqa: BLE001
            print(f"warn download attempt {attempt + 1}: {e}", file=sys.stderr)
            time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"download failed: {u}")


def grid():
    n, s, w, e = CROP
    d = SRC["d"]
    iy0 = int(round((SRC["lat_n"] - n) / d))
    iy1 = int(round((SRC["lat_n"] - s) / d))
    ix0 = int(round((w - SRC["lon_w"]) / d))
    ix1 = int(round((e - SRC["lon_w"]) / d))
    return SRC["lat_n"] - iy0 * d, SRC["lon_w"] + ix0 * d, iy1 - iy0 + 1, ix1 - ix0 + 1, iy0, ix0


def _hours(g) -> int:
    """終了時刻（時間）。ステップ 0 は分で返ることがあるので、単位を時間に揃えてから読む。"""
    eccodes.codes_set(g, "stepUnits", "h")
    return int(eccodes.codes_get_long(g, "endStep"))


def _field(g) -> str | None:
    sn = eccodes.codes_get(g, "shortName")
    if sn in ("2t", "10u", "10v", "tp", "max_i10fg"):
        return sn
    if (eccodes.codes_get(g, "discipline"), eccodes.codes_get(g, "parameterCategory"),
            eccodes.codes_get(g, "parameterNumber")) == (0, 6, 1):
        return "tcc"  # total cloud cover（eccodes の表では shortName "unknown"）
    return None


def main() -> int:
    args = sys.argv[1:]
    local = sparse.local_dir()
    out = local or "/tmp"
    gribdir = args[args.index("--gribdir") + 1] if "--gribdir" in args else None
    run = args[args.index("--run") + 1] if "--run" in args else None

    if run is None:
        run = newest_complete_run(datetime.now(timezone.utc))
        if run is None:
            print("ERROR: no complete AROME run found in the last 18 h", file=sys.stderr)
            return 1
    cyc = datetime.strptime(run, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc)
    if gribdir is None:
        age = (datetime.now(timezone.utc) - cyc).total_seconds() / 3600
        if age > MAX_AGE_H:
            print(f"ERROR: newest run {run} is {age:.0f}h old — refusing to upload", file=sys.stderr)
            return 1
    print(f"AROME run {run}")

    lat0, lon0, NLAT, NLON, iy0, ix0 = grid()
    H = len(STEPS)
    shape = (H, NLAT, NLON)
    dest = {k: np.full(shape, np.nan, np.float32) for k in ("2t", "10u", "10v", "tp", "max_i10fg", "tcc")}

    for group in GROUPS:
        path = os.path.join(gribdir, f"{group}.grib2") if gribdir else os.path.join(out, f"arome_{group}.grib2")
        if not gribdir:
            download(url(run, group), path)
        with open(path, "rb") as f:
            while True:
                try:
                    g = eccodes.codes_grib_new_from_file(f)
                except eccodes.PrematureEndOfFileError:
                    print(f"warn: {group} ends mid-message (truncated download?)", file=sys.stderr)
                    break
                if g is None:
                    break
                try:
                    k = _field(g)
                    if k is None:
                        continue
                    step = _hours(g)
                    if step not in STEPS:
                        continue
                    ni, nj = eccodes.codes_get(g, "Ni"), eccodes.codes_get(g, "Nj")
                    la1 = eccodes.codes_get(g, "latitudeOfFirstGridPointInDegrees")
                    la2 = eccodes.codes_get(g, "latitudeOfLastGridPointInDegrees")
                    lo1 = eccodes.codes_get(g, "longitudeOfFirstGridPointInDegrees")
                    lo1 = lo1 - 360 if lo1 > 180 else lo1
                    if (ni, nj) != (SRC["nlon"], SRC["nlat"]) or abs(lo1 - SRC["lon_w"]) > 1e-3:
                        raise RuntimeError(f"{k} +{step}h: unexpected grid {ni}x{nj} lon0 {lo1}")
                    vals = eccodes.codes_get_values(g).astype(np.float32).reshape(nj, ni)
                    miss = eccodes.codes_get(g, "missingValue")
                    vals[vals == miss] = np.nan
                    # Worker は北端から iy、西端から ix で引く。ファイルの向きを信じず揃える。
                    if la1 < la2:
                        vals = vals[::-1, :]
                    if abs(max(la1, la2) - SRC["lat_n"]) > 1e-3 or abs(min(la1, la2) - SRC["lat_s"]) > 1e-3:
                        raise RuntimeError(f"{k} +{step}h: latitude runs {la1}..{la2}")
                    dest[k][STEPS.index(step)] = vals[iy0:iy0 + NLAT, ix0:ix0 + NLON]
                finally:
                    eccodes.codes_release(g)
        if not gribdir:
            os.remove(path)  # 1 組ずつ処理して消す（Actions のディスク対策）
        print(f"  {group} done")

    t2, u10, v10, tp, gust, cloud = (dest[k] for k in ("2t", "10u", "10v", "tp", "max_i10fg", "tcc"))
    good = int(np.isfinite(t2).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H} (tp {int(np.isfinite(tp).any(axis=(1, 2)).sum())}, "
          f"gust {int(np.isfinite(gust).any(axis=(1, 2)).sum())}, cloud {int(np.isfinite(cloud).any(axis=(1, 2)).sum())})")
    if good < H * 0.9:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    temp = t2 - 273.15
    spd = np.sqrt(u10 * u10 + v10 * v10)
    wdir = np.degrees(np.arctan2(-u10, -v10)) % 360
    # 積算（回の始まりから）→ 1 時間ごとの量。icon_update.py と同じ: 時刻 s に (s-1, s] の量を置く。
    # 初期時刻 0 は積算が無い（NaN）→ 0。
    tp[0] = 0
    np.nan_to_num(tp, copy=False)
    for i in range(H - 1, 0, -1):
        tp[i] -= tp[i - 1]
    np.clip(tp, 0, None, out=tp)
    cloud[cloud > 100.5] = np.nan

    la = lat0 - np.arange(NLAT) * SRC["d"]
    lo = lon0 + np.arange(NLON) * SRC["d"]
    LA, LO = np.meshgrid(la, lo, indexing="ij")
    KEEP = sparse.keep_at(LA, LO) & np.isfinite(temp).any(axis=0)
    sparse.slot_index(KEEP).tofile(f"{out}/{KEY}_s_idx.bin")
    with open(f"{out}/{KEY}.bin", "wb") as f:
        for iy in range(NLAT):
            row = np.empty((NLON, H, len(VARS)), dtype="<f2")
            for j, a in enumerate((temp, spd, wdir, tp, cloud, gust)):
                row[:, :, j] = a[:, iy, :].T
            row[KEEP[iy]].tofile(f)

    meta = {
        "cycle": run,
        "model": "AROME",
        "nLat": NLAT, "nLon": NLON, "res": SRC["d"], "lat0": round(lat0, 4), "lon0": round(lon0, 4),
        "dtype": "f2", "bin": f"{KEY}_s.bin", "idx": f"{KEY}_s_idx.bin", "nSlot": int(KEEP.sum()),
        "fhours": STEPS, "vars": VARS,
    }
    with open(f"{out}/{KEY}_meta.json", "w") as f:
        json.dump(meta, f)
    size = os.path.getsize(f"{out}/{KEY}.bin")
    print(f"packed {size/1e6:.0f} MB ({int(KEEP.sum())}/{KEEP.size} cells), cell={H*len(VARS)*2} B")
    if local:
        print(f"--local: wrote {out}/{KEY}.bin, {KEY}_s_idx.bin, {KEY}_meta.json (not uploaded)")
        return 0

    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    sparse.publish(s3, BUCKET, f"{out}/{KEY}.bin", f"{out}/{KEY}_s_idx.bin", f"{out}/{KEY}_meta.json", KEY)
    print(f"uploaded {KEY}_s.bin + {KEY}_s_idx.bin + {KEY}_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
