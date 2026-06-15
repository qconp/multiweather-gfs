#!/usr/bin/env python3
"""
GFS -> R2 更新スクリプト（GitHub Actions から6時間ごとに実行）。

NOAA GFS（パブリックドメイン）の最新サイクルを取得し、0.25°を0.5°に間引いて
コンパクトなバイナリ（レイアウト [iy][ix][h][temp,wind,windDir,precip,cloud] float32）に
変換、Cloudflare R2（S3互換API）へ gfs.bin / gfs_meta.json としてアップロードする。
cloud(全雲量%)は Worker 側で天気記号(晴/曇/雨/雪)を導出するために付与する。
Worker はこのバイナリをセル単位の range read で1回だけ読み、比較モデル "gfs" として返す。

必要な環境変数（GitHub Secrets）:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
"""
import os, sys, urllib.request, json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import xarray as xr
import boto3

RES = 0.5
NLAT, NLON = 361, 720
FHOURS = list(range(1, 49))          # f001..f048（1時間刻み・48h）
VARS = ["temp", "wind", "windDir", "precip", "cloud"]
BUCKET = "multiweather-gfs"


def url(date, hh, fh):
    return (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?"
        f"dir=%2Fgfs.{date}%2F{hh}%2Fatmos&file=gfs.t{hh}z.pgrb2.0p25.f{fh:03d}"
        f"&var_TMP=on&var_UGRD=on&var_VGRD=on&var_APCP=on&var_TCDC=on"
        f"&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on"
        f"&lev_entire_atmosphere=on"
        f"&lev_entire_atmosphere_%28considered_as_a_single_layer%29=on"
        f"&leftlon=0&rightlon=360&toplat=90&bottomlat=-90"
    )


def find_cycle():
    """利用可能な最新サイクルを検出（現在UTCから約4hの遅延を見込み、6h刻みで遡る）。"""
    base = datetime.now(timezone.utc) - timedelta(hours=4)
    base = base.replace(minute=0, second=0, microsecond=0, hour=(base.hour // 6) * 6)
    for back in range(0, 4):
        c = base - timedelta(hours=6 * back)
        date, hh = c.strftime("%Y%m%d"), c.strftime("%H")
        try:
            with urllib.request.urlopen(url(date, hh, 1), timeout=90) as r:
                data = r.read()
                if r.status == 200 and len(data) > 1000:
                    open("/tmp/g_001.grib2", "wb").write(data)
                    return date, hh
        except Exception:
            continue
    return None


def main():
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no GFS cycle available", file=sys.stderr)
        sys.exit(1)
    date, hh = cyc
    print(f"cycle: {date} {hh}Z")

    def dl(fh):
        try:
            urllib.request.urlretrieve(url(date, hh, fh), f"/tmp/g_{fh:03d}.grib2")
        except Exception as e:
            print(f"download warn f{fh}: {e}", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(dl, [f for f in FHOURS if f != 1]))

    def rd(fh, keys):
        return xr.open_dataset(
            f"/tmp/g_{fh:03d}.grib2",
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": keys, "indexpath": ""},
        )

    H = len(FHOURS)
    temp = np.full((H, NLAT, NLON), np.nan, np.float32)
    spd = np.full_like(temp, np.nan)
    wdir = np.full_like(temp, np.nan)
    apcp = np.zeros((H, NLAT, NLON), np.float32)
    cloud = np.full_like(temp, np.nan)
    for i, fh in enumerate(FHOURS):
        try:
            temp[i] = (rd(fh, {"typeOfLevel": "heightAboveGround", "level": 2, "shortName": "2t"})["t2m"].values - 273.15)[::2, ::2]
            u = rd(fh, {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10u"})["u10"].values[::2, ::2]
            v = rd(fh, {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10v"})["v10"].values[::2, ::2]
            spd[i] = np.sqrt(u * u + v * v)
            wdir[i] = np.degrees(np.arctan2(-u, -v)) % 360
            try:
                apcp[i] = rd(fh, {"typeOfLevel": "surface", "stepType": "accum", "shortName": "tp"})["tp"].values[::2, ::2]
            except Exception:
                apcp[i] = 0
            # 全雲量(TCDC, %)。天気記号(晴/曇)の導出に使う。
            # GFS の entire-atmosphere TCDC は instant と "0-1 hour ave" の2メッセージがあり、
            # stepType を指定しないと cfgrib が曖昧エラーで落ちる(→全NaN)。instant優先・avgフォールバック。
            # typeOfLevel 名は eccodes 版差で atmosphere / entireAtmosphere の両方を試す。
            for keys in (
                {"typeOfLevel": "atmosphere", "shortName": "tcc", "stepType": "instant"},
                {"typeOfLevel": "entireAtmosphere", "shortName": "tcc", "stepType": "instant"},
                {"typeOfLevel": "atmosphere", "shortName": "tcc", "stepType": "avg"},
                {"typeOfLevel": "entireAtmosphere", "shortName": "tcc", "stepType": "avg"},
            ):
                try:
                    cloud[i] = rd(fh, keys)["tcc"].values[::2, ::2]
                    break
                except Exception:
                    continue
        except Exception as e:
            print(f"parse warn f{fh}: {e}", file=sys.stderr)

    # APCP 脱積算（GFSは6hバケットでリセット）
    precip = np.zeros_like(apcp)
    for i, fh in enumerate(FHOURS):
        precip[i] = apcp[i] if (fh - 1) % 6 == 0 else apcp[i] - apcp[i - 1]
    precip = np.clip(precip, 0, None)

    # 診断: 雲量が何時間分パースできたか（0 なら TCDC 抽出失敗＝Worker側で晴/曇アイコンが出ない）。
    print(f"cloud parsed for {int(np.isfinite(cloud).any(axis=(1, 2)).sum())}/{H} hours", file=sys.stderr)

    arr = np.stack([temp, spd, wdir, precip, cloud], axis=-1)  # (H,NLAT,NLON,5)
    arr = np.transpose(arr, (1, 2, 0, 3)).astype("<f4")     # (NLAT,NLON,H,5) C-order
    open("/tmp/gfs.bin", "wb").write(arr.tobytes())
    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "nLat": NLAT, "nLon": NLON, "res": RES, "fhours": FHOURS, "vars": VARS,
    }
    open("/tmp/gfs_meta.json", "w").write(json.dumps(meta))
    print(f"built gfs.bin {arr.nbytes} bytes, cycle {meta['cycle']}")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    # bin を先に、meta を後に（Worker は meta の cycle を信頼するため整合順）
    s3.upload_file("/tmp/gfs.bin", BUCKET, "gfs.bin")
    s3.upload_file("/tmp/gfs_meta.json", BUCKET, "gfs_meta.json")
    print("uploaded to R2 OK")


if __name__ == "__main__":
    main()
