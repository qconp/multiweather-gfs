#!/usr/bin/env python3
"""NOAA HRRR (CONUS 3 km) -> packed sparse binary -> R2, mirroring arome_update.py.

    python hrrr_update.py                         # newest complete synoptic run -> R2
    python hrrr_update.py --local DIR             # write to DIR instead of R2 (local verification)
    python hrrr_update.py --local DIR --run 2026110812

Why this exists (2026-11-09): owner policy "add each rich country's own open data, after checking how
the source must be credited". The US is ~12% of iOS downloads; NWS is already shown, HRRR adds the
local high-resolution model.

Licence (primary source: AWS Open Data registry entry managed by NOAA, noaa-hrrr-pds.yaml, checked
2026-11-09): "NOAA data disseminated through NODD are open to the public and can be used as desired.
NOAA requests attribution ... it is not permissible to state or imply endorsement by or affiliation
with NOAA. If you modify NOAA data, you may not state or imply that it is original, unaltered NOAA data."
Attribution (with "modified") is emitted by the Worker (fetchHrrr), never by this script or the app.
"NOAA" never goes into the app name, subtitle or keywords.

Source  https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.YYYYMMDD/conus/hrrr.tHHz.wrfsfcfFF.grib2 (+ .idx)
        Only the needed messages are fetched with byte ranges from the .idx (~6 x ~1.5 MB per step).
        00/06/12/18Z reach +48h hourly (other cycles +18h only and are skipped).
        Lambert conformal on a sphere R=6371229, Latin1=Latin2=38.5, LoV=262.5, 1799 x 1059, 3 km,
        first point (21.138123N, 237.280472E), rows from the south (jScansPositively=1).
        Fields: TMP 2 m, UGRD/VGRD 10 m, GUST surface (carried as-is), APCP surface "(N-1)-N hour acc"
        (HRRR's own hourly amount; no de-accumulation), TCDC entire atmosphere (%).
        No weather code: none is written, and the Worker shows no weather symbols for this model.

Cells kept = HRRR US mask (hrrr_usmask.npz: Natural Earth US + 6 km, tools/build_hrrr_usmask.py)
AND the global land + 50 km coast mask (sparse.py). The Worker converts lat/lon to (iy, ix) with the
same Lambert formula (meta.proj) and reads one cell; cells outside the mask are 0xFFFFFFFF -> not shown.
Output layout is cell-contiguous (slot, H, V) float16 LE, rows from the south.
"""
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import eccodes
import numpy as np

import sparse

UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")
BASE = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
KEY = "hrrr"
VARS = ["temp", "wind", "windDir", "precip", "cloud", "gust"]
STEPS = list(range(0, 49))
NX, NY, D = 1799, 1059, 3000.0
PROJ = {"type": "lcc", "R": 6371229.0, "lat1": 38.5, "lon0": -97.5, "la1": 21.138123, "lo1": 237.280472 - 360, "d": D}
MAX_AGE_H = 12
_HERE = os.path.dirname(os.path.abspath(__file__))


def url(run: str, fh: int) -> str:
    return f"{BASE}/hrrr.{run[:8]}/conus/hrrr.t{run[8:]}z.wrfsfcf{fh:02d}.grib2"


def http(u: str, rng: tuple[int, int] | None = None) -> bytes:
    h = dict(UA)
    if rng:
        h["Range"] = f"bytes={rng[0]}-{rng[1]}" if rng[1] >= 0 else f"bytes={rng[0]}-"
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers=h), timeout=120) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            err = e
        except Exception as e:  # noqa: BLE001
            err = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"fetch failed {u}: {err}")


def newest_complete_run(now: datetime) -> str | None:
    """+48h まで公開済みの、いちばん新しい 00/06/12/18Z の回。"""
    t = now.replace(minute=0, second=0, microsecond=0)
    t -= timedelta(hours=t.hour % 6)
    for _ in range(6):
        run = t.strftime("%Y%m%d%H")
        try:
            http(url(run, 48) + ".idx")
            return run
        except Exception:  # noqa: BLE001
            t -= timedelta(hours=6)
    return None


WANT = [
    ("2t", re.compile(r":TMP:2 m above ground:")),
    ("10u", re.compile(r":UGRD:10 m above ground:")),
    ("10v", re.compile(r":VGRD:10 m above ground:")),
    ("gust", re.compile(r":GUST:surface:")),
    ("tcc", re.compile(r":TCDC:entire atmosphere:")),
]


def ranges(idx: str, fh: int) -> dict[str, tuple[int, int]]:
    lines = [l for l in idx.strip().split("\n") if l]
    offs = [int(l.split(":")[1]) for l in lines]
    out = {}
    hourly = re.compile(rf":APCP:surface:{fh - 1}-{fh} hour acc fcst:") if fh > 0 else None
    for i, l in enumerate(lines):
        end = offs[i + 1] - 1 if i + 1 < len(lines) else -1
        for k, rx in WANT:
            if k not in out and rx.search(l):
                out[k] = (offs[i], end)
        if hourly and "tp" not in out and hourly.search(l):
            out["tp"] = (offs[i], end)
    return out


def decode(buf: bytes) -> np.ndarray:
    g = eccodes.codes_new_from_message(buf)
    try:
        nx, ny = eccodes.codes_get(g, "Nx"), eccodes.codes_get(g, "Ny")
        if (nx, ny) != (NX, NY) or eccodes.codes_get(g, "jScansPositively") != 1:
            raise RuntimeError(f"unexpected grid {nx}x{ny}")
        v = eccodes.codes_get_values(g).astype(np.float32).reshape(NY, NX)
        miss = eccodes.codes_get(g, "missingValue")
        v[v == miss] = np.nan
        return v
    finally:
        eccodes.codes_release(g)


def lcc_forward(lat, lon):
    p1 = math.radians(PROJ["lat1"])
    n = math.sin(p1)
    F = math.cos(p1) * math.tan(math.pi / 4 + p1 / 2) ** n / n
    r0 = PROJ["R"] * F / math.tan(math.pi / 4 + p1 / 2) ** n
    dl = ((np.asarray(lon) - PROJ["lon0"]) + 540) % 360 - 180
    r = PROJ["R"] * F / np.tan(np.pi / 4 + np.radians(lat) / 2) ** n
    th = n * np.radians(dl)
    return r * np.sin(th), r0 - r * np.cos(th)


def grid_latlon():
    """格子の緯度経度（Lambert の逆変換・tools/build_hrrr_usmask.py と同じ式）。行は南から。"""
    p1 = math.radians(PROJ["lat1"])
    n = math.sin(p1)
    F = math.cos(p1) * math.tan(math.pi / 4 + p1 / 2) ** n / n
    r0 = PROJ["R"] * F / math.tan(math.pi / 4 + p1 / 2) ** n
    x0, y0 = lcc_forward(PROJ["la1"], PROJ["lo1"])
    XX, YY = np.meshgrid(x0 + np.arange(NX) * D, y0 + np.arange(NY) * D)
    dy = r0 - YY
    r = np.sign(n) * np.sqrt(XX * XX + dy * dy)
    lat = np.degrees(2 * np.arctan((PROJ["R"] * F / r) ** (1 / n)) - np.pi / 2)
    lon = PROJ["lon0"] + np.degrees(np.arctan2(XX, dy) / n)
    return lat, lon


def main() -> int:
    args = sys.argv[1:]
    local = sparse.local_dir()
    out = local or "/tmp"
    run = args[args.index("--run") + 1] if "--run" in args else None
    if run is None:
        run = newest_complete_run(datetime.now(timezone.utc))
        if run is None:
            print("ERROR: no complete HRRR synoptic run found", file=sys.stderr)
            return 1
    cyc = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    if "--run" not in args and (datetime.now(timezone.utc) - cyc).total_seconds() / 3600 > MAX_AGE_H:
        print(f"ERROR: newest run {run} is too old — refusing to upload", file=sys.stderr)
        return 1
    print(f"HRRR run {run}")

    lat, lon = grid_latlon()
    z = np.load(os.path.join(_HERE, "hrrr_usmask.npz"))
    us = np.unpackbits(z["keep"], axis=1)[:, :NX].astype(bool)
    KEEP = us & sparse.keep_at(lat, lon)
    nkeep = int(KEEP.sum())
    H, V = len(STEPS), len(VARS)
    data = np.full((nkeep, H, V), np.nan, dtype="<f2")  # 出力の並びそのもの（KEEP の行優先順）

    def step(h_i: int):
        fh = STEPS[h_i]
        u = url(run, fh)
        rg = ranges(http(u + ".idx").decode(), fh)
        need = [k for k, _ in WANT] + (["tp"] if fh > 0 else [])
        miss = [k for k in need if k not in rg]
        if miss:
            raise RuntimeError(f"f{fh:02d}: missing {miss}")
        f = {k: decode(http(u, rg[k]))[KEEP] for k in rg}
        u10, v10 = f["10u"], f["10v"]
        cols = [
            f["2t"] - 273.15,
            np.sqrt(u10 * u10 + v10 * v10),
            np.degrees(np.arctan2(-u10, -v10)) % 360,
            f["tp"] if "tp" in f else np.zeros(nkeep, np.float32),  # 初期時刻は量なし → 0（ICON と同じ）
            f["tcc"],
            f["gust"],
        ]
        for j, c in enumerate(cols):
            data[:, h_i, j] = c
        return fh

    with ThreadPoolExecutor(max_workers=4) as ex:
        for fh in ex.map(step, range(H)):
            if fh % 12 == 0:
                print(f"  f{fh:02d} done")
    good = int(np.isfinite(data[:, :, 0].astype(np.float32)).any(axis=0).sum())
    print(f"steps with data: {good}/{H}")
    if good < H:
        print("ERROR: some steps carry no data — refusing to upload", file=sys.stderr)
        return 1

    sparse.slot_index(KEEP).tofile(f"{out}/{KEY}_s_idx.bin")
    data.tofile(f"{out}/{KEY}.bin")
    x0, y0 = lcc_forward(PROJ["la1"], PROJ["lo1"])
    meta = {
        "cycle": cyc.strftime("%Y-%m-%dT%H:00:00Z"),
        "model": "HRRR",
        "nLat": NY, "nLon": NX, "res": D,
        "proj": {**PROJ, "x0": float(x0), "y0": float(y0)},
        "dtype": "f2", "bin": f"{KEY}_s.bin", "idx": f"{KEY}_s_idx.bin", "nSlot": nkeep,
        "fhours": STEPS, "vars": VARS,
    }
    with open(f"{out}/{KEY}_meta.json", "w") as fo:
        json.dump(meta, fo)
    size = os.path.getsize(f"{out}/{KEY}.bin")
    print(f"packed {size/1e6:.0f} MB ({nkeep}/{KEEP.size} cells), cell={H*V*2} B")
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
