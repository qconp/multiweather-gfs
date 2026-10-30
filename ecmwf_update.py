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
import random
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np

import sparse  # 陸と沿岸 50 km だけを書く（R2 容量対策 2026-09-30）
import xarray as xr

BASE = "https://data.ecmwf.int/forecasts"
# ★ 間引かない。0.5°(約55km)にすると沿岸の格子に海が混ざり、昼夜の温度変化が平均化されて
#   消える（東京湾で実測: met.no/Apple が日較差 6.6/6.8℃ のところ ECMWF/GFS は 3.6/4.1℃）。
#   内陸では4モデルが一致するので、粗さそのものが原因。港湾・測量・ドローンの現場が
#   主要用途である以上、沿岸で潰れるのは実害。容量は増えるがセル読み出しは変わらない。
RES = 0.25
NLAT, NLON = 721, 1440
STEPS = list(range(0, 145, 3))  # ECMWF oper は 3 時間刻み。+144h まで（00/12Z）
# ★ 06/18Z の回は open data では +90h まで（2026-10-30 確認: 06z/ifs/0p25/oper/...-90h-oper-fc.index）。
#   全サイクル化（オーナー承認 2026-10-30）で 06/18Z も取る。その回は +90h までになる（Worker は meta.fhours で読む）。
STEPS_SHORT = list(range(0, 91, 3))
LOCAL = sparse.local_dir()  # --local DIR: R2 に上げず DIR に書く（手元の検証用）
OUT = LOCAL or "/tmp"
VARS = ["temp", "wind", "windDir", "precip", "cloud", "wx"]
PARAMS = ["2t", "10u", "10v", "tp", "tcc", "ptype"]
# ★ wx = ECMWF 自身が配る降水の種類 ptype（WMO Code table 4.201・瞬時値）。こちらで数値から天気を
#   作らない（気象業務法・2026-10-01 決定、10_Planning/ops_weather_service_act.md）。コードをそのまま運ぶ。
#   0=なし 1=雨 3=着氷性の雨 5=雪 6=湿った雪 7=雨と雪 8=凍雨(ice pellets) 12=着氷性の霧雨。
OPTIONAL = {"ptype"}  # 無い時刻があっても他の項目の取り込みは落とさない（NaN のまま）
BUCKET = os.environ.get("R2_BUCKET", "multiweather-gfs")  # 既存バケットに ecmwf.* を同居
UA = {"User-Agent": "multi-weather/1.0 (+https://qconp.com)"}


def step_url(date: str, hh: str, step: int, ext: str) -> str:
    return f"{BASE}/{date}/{hh}z/ifs/0p25/oper/{date}{hh}0000-{step}h-oper-fc.{ext}"


def http(url: str, rng: tuple[int, int] | None = None) -> bytes:
    # ★ data.ecmwf.int は混雑時に 429 を返す（08:30 UTC の回で 0/49 になり失敗通知が続いた）。
    #   429/5xx/通信断は Retry-After（無ければ指数バックオフ＋ゆらぎ）で待って取り直す。
    for attempt in range(8):
        req = urllib.request.Request(url, headers=dict(UA))
        if rng:
            req.add_header("Range", f"bytes={rng[0]}-{rng[1]}")
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                raise
            wait = e.headers.get("Retry-After") if e.headers else None
            delay = float(wait) if wait and wait.isdigit() else min(120, 5 * 2 ** attempt)
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            delay, err = min(120, 5 * 2 ** attempt), e
        time.sleep(delay + random.uniform(0, 3))
    raise err


def _published(url: str) -> bool:
    """HEAD で公開済みかを見る。404 だけが「未公開」。429/5xx/通信断は待って聞き直す
    （2026-09-30: 429 を未公開と取り違えて 2 日前の回まで遡り、Worker が古すぎて AIFS 列が消えた）。"""
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, headers=dict(UA), method="HEAD")
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
        except Exception:
            pass
        time.sleep(min(60, 5 * 2 ** attempt) + random.uniform(0, 3))
    raise RuntimeError(f"cannot tell whether published: {url}")


def steps_for(hh: str) -> list[int]:
    return STEPS if hh in ("00", "12") else STEPS_SHORT


def find_cycle() -> tuple[str, str] | None:
    """Newest cycle whose LAST step is published. Publication lags the run by ~7-9h (06/18Z: ~6h)."""
    now = datetime.now(timezone.utc)
    for back in range(0, 30):
        t = now - timedelta(hours=6 * back)
        date, hh = t.strftime("%Y%m%d"), f"{(t.hour // 6) * 6:02d}"
        if _published(step_url(date, hh, steps_for(hh)[-1], "index")):
            # ★ 古すぎる回は上げない（Worker は 36h で捨てるので、上げても列が消えるだけ）。失敗させて通知で気づく。
            age = (now - datetime.strptime(date + hh, "%Y%m%d%H").replace(tzinfo=timezone.utc)).total_seconds() / 3600
            if age > 30:
                print(f"ERROR: newest published cycle {date} {hh}Z is {age:.0f}h old — refusing to upload", file=sys.stderr)
                return None
            return date, hh
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
    missing = [p for p in PARAMS if p not in wanted and p not in OPTIONAL]
    if missing:
        raise RuntimeError(f"step {step}: index missing {missing}")

    grib_url = step_url(date, hh, step, "grib2")
    out: dict[str, np.ndarray] = {}
    for p, (off, ln) in wanted.items():
        blob = http(grib_url, (off, off + ln - 1))
        path = f"{OUT}/e_{step:03d}_{p}.grib2"
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

        out[p] = arr
        ds.close()
        os.unlink(path)
    return out


def main() -> int:
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no ECMWF cycle available", file=sys.stderr)
        return 1
    date, hh = cyc
    STEPS = steps_for(hh)
    print(f"cycle: {date} {hh}Z, {len(STEPS)} steps")

    H, V = len(STEPS), len(VARS)

    # ★ メモリと書き込み順の両方を成立させる形。
    #   - 変数ごとに (H, NLAT, NLON) で持つ（取得時の書き込みが連続になる）
    #   - 出力は緯度行ごとに (NLON, H, V) を組み立てて逐次書き出す
    #   (NLAT, NLON, H, V) を一度に確保して直接埋めると、書き込みが 980B 間隔の飛び飛びに
    #   なって CI が時間切れになる。逆に np.stack + transpose を挟むと 0.25° では同じ大きさの
    #   配列が2本増えて 3GB 必要になる。行単位のストリーム書き出しなら両方避けられる。
    shape = (H, NLAT, NLON)
    temp = np.full(shape, np.nan, np.float32)
    spd = np.full(shape, np.nan, np.float32)
    wdir = np.full(shape, np.nan, np.float32)
    tp = np.full(shape, np.nan, np.float32)
    cloud = np.full(shape, np.nan, np.float32)
    wx = np.full(shape, np.nan, np.float32)

    def one(i_step):
        i, step = i_step
        try:
            d = fetch_step(date, hh, step)
        except Exception as e:
            print(f"warn step {step}: {e}", file=sys.stderr)
            return
        u, v = d["10u"], d["10v"]
        temp[i] = d["2t"] - 273.15
        spd[i] = np.sqrt(u * u + v * v)
        wdir[i] = np.degrees(np.arctan2(-u, -v)) % 360
        tp[i] = d["tp"] * 1000.0    # m -> mm（この時点では step0 からの積算）
        cloud[i] = d["tcc"] * 100.0  # 0..1 -> %
        if "ptype" in d:
            wx[i] = d["ptype"]         # コードのまま

    with ThreadPoolExecutor(max_workers=3) as ex:
        list(ex.map(one, list(enumerate(STEPS))))

    # 中身が空でも「成功」で終えないこと。fetch_step の失敗は警告にして続行する作りなので、
    # 全ステップ落ちても配列が NaN のまま処理が最後まで通ってしまう（初回実行で実際に発生）。
    good = int(np.isfinite(temp).any(axis=(1, 2)).sum())
    print(f"steps with data: {good}/{H}")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} steps carry data — refusing to upload", file=sys.stderr)
        return 1

    # 脱積算。ECMWF の tp は step 0 からの通し積算なので単純差分でよい（GFS の 6h バケット
    # 扱いを持ち込むと二重に引く）。後ろから引けば作業用の複製が要らない。
    np.nan_to_num(tp, copy=False)
    for i in range(H - 1, 0, -1):
        tp[i] -= tp[i - 1]
    np.clip(tp, 0, None, out=tp)

    # 緯度行ごとに (NLON, H, V) を組み立てて書き出す。1行あたり約1.4MB。
    KEEP = sparse.keep_regular(NLAT, NLON, RES)
    sparse.slot_index(KEEP).tofile(f"{OUT}/ecmwf_s_idx.bin")
    with open(f"{OUT}/ecmwf.bin", "wb") as f:
        for iy in range(NLAT):
            # ★ float16（R2 無料枠 10GB の対策 2026-09-29。分解能は Worker の 0.1 丸めより細かい）。
            row = np.empty((NLON, H, V), dtype="<f2")
            row[:, :, 0] = temp[:, iy, :].T
            row[:, :, 1] = spd[:, iy, :].T
            row[:, :, 2] = wdir[:, iy, :].T
            row[:, :, 3] = tp[:, iy, :].T
            row[:, :, 4] = cloud[:, iy, :].T
            row[:, :, 5] = wx[:, iy, :].T
            row[KEEP[iy]].tofile(f)

    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "nLat": NLAT, "nLon": NLON, "res": RES,
        "dtype": "f2", "bin": "ecmwf_s.bin", "idx": "ecmwf_s_idx.bin", "nSlot": int(KEEP.sum()), "fhours": STEPS, "vars": VARS,
    }
    with open(f"{OUT}/ecmwf_meta.json", "w") as f:
        json.dump(meta, f)

    size = os.path.getsize(f"{OUT}/ecmwf.bin")
    print(f"packed {size/1e6:.0f} MB, cell={H*len(VARS)*4} B")

    if LOCAL:
        print(f"--local: wrote {OUT}/ecmwf.bin, ecmwf_s_idx.bin, ecmwf_meta.json (not uploaded)")
        return 0
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    sparse.publish(s3, BUCKET, f"{OUT}/ecmwf.bin", f"{OUT}/ecmwf_s_idx.bin", f"{OUT}/ecmwf_meta.json", "ecmwf")
    print("uploaded ecmwf.bin + ecmwf_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
