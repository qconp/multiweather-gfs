#!/usr/bin/env python3
"""ItaliaMeteo-ARPAE open data ICON-2I -> packed sparse binary -> R2, mirroring icon_update.py.

    python icon2i_update.py               # newest run -> R2
    python icon2i_update.py --local DIR   # write to DIR instead of R2 (local verification)
    python icon2i_update.py --local DIR --grib FILE   # use an already downloaded GRIB

Why this exists (2026-11-09): owner policy "add each rich country's own open data, after checking
how the source must be credited". Italy is ~8% of iOS downloads and had no local source.

Licence (primary source: Meteo-Hub API /api/datasets?licenceSpecs=true, 2026-11-09):
  ICON_2I_SURFACE_PRESSURE_LEVELS  license "CCBY4.0", is_public, attribution "ItaliaMeteo-ARPAE"
  ("Agenzia ItaliaMeteo in cooperation with Arpae Emilia-Romagna Idro-Meteo-Clima Service").
  Attribution is emitted by the Worker (fetchIcon2i), never by this script or the app. The agency
  names never go into the app name, subtitle or keywords.

Source  https://meteohub.agenziaitaliameteo.it/api/datasets/ICON_2I_SURFACE_PRESSURE_LEVELS/opendata
        -> [{date, run "00:00"|"12:00", vars, filename}]; file = /api/opendata/<filename> (~760 MB, no key)
        GRIB2 regular_ll 761x761, lat 33.7..48.9 (0.02), lon 3.0..22.0 (0.025), hourly +0..+72h.
        Fields used: 2t, 10u, 10v, clct (%), tp (kg m-2 = mm per HOUR: stepRange "s-1 - s", not a running total).
        No weather code and no gust in the open file: none are written, and the Worker shows no
        weather symbols for this model (we never derive weather from model numbers).

We keep only the inner rectangle CROP (Italy + surroundings, >= 0.5 deg inside the grid edge) so
the Worker can decide "inside or not" with the same rectangle and no R2 read (isInsideIcon2i).
Output layout is cell-contiguous (slot, H, V) float16 LE, land + 50 km coast cells only (sparse.py).
The grid steps differ in lat/lon, so meta carries res (lat) and resLon.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import eccodes
import numpy as np

import sparse

UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")
API = "https://meteohub.agenziaitaliameteo.it/api"
DATASET = "ICON_2I_SURFACE_PRESSURE_LEVELS"
KEY = "icon2i"
VARS = ["temp", "wind", "windDir", "precip", "cloud"]

SRC = {"nlat": 761, "nlon": 761, "lat_s": 33.7, "lat_n": 48.9, "lon_w": 3.0, "lon_e": 22.0,
       "dlat": 0.02, "dlon": 0.025}
# 内側の矩形（北端・南端・西端・東端）。Worker の ICON_2I_DOMAIN と同じ値にすること。
CROP = (47.2, 35.4, 6.5, 18.6)
STEPS = list(range(0, 73))
MAX_AGE_H = 18


def http(url: str) -> bytes:
    err = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=dict(UA)), timeout=120) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code < 500:
                raise
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        time.sleep(3 * 2 ** attempt)
    raise err


def newest_run():
    runs = json.loads(http(f"{API}/datasets/{DATASET}/opendata"))
    runs.sort(key=lambda r: (r["date"], r["run"]))
    r = runs[-1]
    cyc = datetime.strptime(f"{r['date']} {r['run']}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    return cyc, r["filename"]


def download(filename: str, path: str) -> None:
    """Stream to disk (the file is ~760 MB; never hold it in memory)."""
    url = f"{API}/opendata/{filename}"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=dict(UA)), timeout=300) as r, open(path, "wb") as f:
                while True:
                    b = r.read(8 << 20)
                    if not b:
                        break
                    f.write(b)
            return
        except Exception as e:  # noqa: BLE001
            print(f"warn download attempt {attempt + 1}: {e}", file=sys.stderr)
            time.sleep(10 * (attempt + 1))
    raise RuntimeError("download failed")


def grid():
    n, s, w, e = CROP
    iy0 = int(round((SRC["lat_n"] - n) / SRC["dlat"]))
    iy1 = int(round((SRC["lat_n"] - s) / SRC["dlat"]))
    ix0 = int(round((w - SRC["lon_w"]) / SRC["dlon"]))
    ix1 = int(round((e - SRC["lon_w"]) / SRC["dlon"]))
    return n, w, iy1 - iy0 + 1, ix1 - ix0 + 1, iy0, ix0


def _hours(g) -> int:
    """終了時刻（時間）。ステップ 0 は "0m" と分で返るので、単位を時間に揃えてから読む。"""
    eccodes.codes_set(g, "stepUnits", "h")
    return int(eccodes.codes_get_long(g, "endStep"))


def main() -> int:
    args = sys.argv[1:]
    local = sparse.local_dir()
    out = local or "/tmp"
    grib = args[args.index("--grib") + 1] if "--grib" in args else None

    if grib:
        cyc = None  # read from the file
    else:
        cyc, filename = newest_run()
        age = (datetime.now(timezone.utc) - cyc).total_seconds() / 3600
        if age > MAX_AGE_H:
            print(f"ERROR: newest run {cyc:%Y-%m-%d %H}Z is {age:.0f}h old — refusing to upload", file=sys.stderr)
            return 1
        grib = os.path.join(out, "icon2i_src.grib")
        print(f"ICON-2I run {cyc:%Y-%m-%d %H}Z: downloading {filename}")
        download(filename, grib)

    lat0, lon0, NLAT, NLON, iy0, ix0 = grid()
    H = len(STEPS)
    shape = (H, NLAT, NLON)
    t2 = np.full(shape, np.nan, np.float32)
    u10 = np.full(shape, np.nan, np.float32)
    v10 = np.full(shape, np.nan, np.float32)
    clct = np.full(shape, np.nan, np.float32)
    tp = np.full(shape, np.nan, np.float32)
    dest = {"2t": t2, "10u": u10, "10v": v10, "clct": clct, "tp": tp}
    names = {}

    with open(grib, "rb") as f:
        while True:
            try:
                g = eccodes.codes_grib_new_from_file(f)
            except eccodes.PrematureEndOfFileError:
                # 途中で切れたファイル。下の「データのある時刻の数」の検査で上げずに止まる。
                print("warn: GRIB ends mid-message (truncated download?)", file=sys.stderr)
                break
            if g is None:
                break
            try:
                sn = eccodes.codes_get(g, "shortName")
                if sn not in dest:
                    continue
                step = _hours(g)
                if step not in STEPS:
                    continue
                if cyc is None:
                    d, t = eccodes.codes_get(g, "dataDate"), eccodes.codes_get(g, "dataTime")
                    cyc = datetime.strptime(f"{d}{t:04d}", "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                ni, nj = eccodes.codes_get(g, "Ni"), eccodes.codes_get(g, "Nj")
                la1 = eccodes.codes_get(g, "latitudeOfFirstGridPointInDegrees")
                la2 = eccodes.codes_get(g, "latitudeOfLastGridPointInDegrees")
                lo1 = eccodes.codes_get(g, "longitudeOfFirstGridPointInDegrees")
                if (ni, nj) != (SRC["nlon"], SRC["nlat"]) or abs(lo1 - SRC["lon_w"]) > 1e-3:
                    raise RuntimeError(f"{sn} +{step}h: unexpected grid {ni}x{nj} lon0 {lo1}")
                vals = eccodes.codes_get_values(g).astype(np.float32).reshape(nj, ni)
                miss = eccodes.codes_get(g, "missingValue")
                vals[vals == miss] = np.nan
                # Worker は北端から iy、西端から ix で引く。ファイルの向きを信じず揃える。
                if la1 < la2:
                    vals = vals[::-1, :]
                if abs(max(la1, la2) - SRC["lat_n"]) > 1e-3 or abs(min(la1, la2) - SRC["lat_s"]) > 1e-3:
                    raise RuntimeError(f"{sn} +{step}h: latitude runs {la1}..{la2}")
                dest[sn][STEPS.index(step)] = vals[iy0:iy0 + NLAT, ix0:ix0 + NLON]
                names.setdefault(sn, eccodes.codes_get(g, "name"))
            finally:
                eccodes.codes_release(g)

    print("fields:", names)
    if "tp" not in names:
        print("ERROR: no total precipitation (tp) in the file", file=sys.stderr)
        return 1
    print(f"ICON-2I cycle {cyc:%Y-%m-%d %H}Z, {H} steps, grid {NLAT}x{NLON}")

    good = int(np.isfinite(t2).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H} (tp {int(np.isfinite(tp).any(axis=(1, 2)).sum())})")
    if good < H * 0.9:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    temp = t2 - 273.15
    spd = np.sqrt(u10 * u10 + v10 * v10)
    wdir = np.degrees(np.arctan2(-u10, -v10)) % 360
    # ★ ICON-2I の公開ファイルの tp は「1 時間ごとの量」（stepRange "s-1 - s"、kg m-2 = mm）。
    #   DWD の ICON（回の始まりからの積算）と違うので差を取らない。その時刻 s に (s-1, s] の量を置く
    #   ＝ icon_update.py が積算の差で作る値と同じ意味。初期時刻 0 は区間が無いので 0。
    np.nan_to_num(tp, copy=False)
    np.clip(tp, 0, None, out=tp)

    la = lat0 - np.arange(NLAT) * SRC["dlat"]
    lo = lon0 + np.arange(NLON) * SRC["dlon"]
    LA, LO = np.meshgrid(la, lo, indexing="ij")
    KEEP = sparse.keep_at(LA, LO) & np.isfinite(temp).any(axis=0)
    sparse.slot_index(KEEP).tofile(f"{out}/{KEY}_s_idx.bin")
    with open(f"{out}/{KEY}.bin", "wb") as f:
        for iy in range(NLAT):
            row = np.empty((NLON, H, len(VARS)), dtype="<f2")
            for j, a in enumerate((temp, spd, wdir, tp, clct)):
                row[:, :, j] = a[:, iy, :].T
            row[KEEP[iy]].tofile(f)

    meta = {
        "cycle": cyc.strftime("%Y-%m-%dT%H:00:00Z"),
        "model": "ICON-2I",
        "nLat": NLAT, "nLon": NLON, "res": SRC["dlat"], "resLon": SRC["dlon"], "lat0": lat0, "lon0": lon0,
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
