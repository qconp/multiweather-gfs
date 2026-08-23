#!/usr/bin/env python3
"""
GFS -> R2 更新スクリプト（GitHub Actions から6時間ごとに実行）。

NOAA GFS（パブリックドメイン）の最新サイクルを取得し、0.25°を0.5°に間引いて
コンパクトなバイナリ（レイアウト [iy][ix][h][temp,wind,windDir,precip,cloud] float32）に
変換、Cloudflare R2（S3互換API）へ gfs.bin / gfs_meta.json としてアップロードする。
cloud(全雲量%)は表示用。かつては Worker 側で天気記号を導出する入力だったが、
数値から現象名を作るのは気象業務法 第2条6項の予報にあたるため撤去した
(10_Planning/ops_weather_service_act.md)。いまは % のまま画面に出している。
Worker はこのバイナリをセル単位の range read で1回だけ読み、比較モデル "gfs" として返す。

必要な環境変数（GitHub Secrets）:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
"""
import os, sys, time, urllib.request, json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import xarray as xr
import boto3

# ★ 間引かない。0.5°(約55km)にすると沿岸の格子に海が混ざり、日較差が平均化されて消える。
#   実測(2026-08-22 那覇): 0.5°の GFS は日較差 0.7℃、met.no は 3.5℃。神戸も 2.0℃ 対 5.3℃。
#   内陸では一致するので、モデル差ではなく格子の粗さ。ECMWF を 0.25° にしたら met.no と
#   ほぼ一致したので、GFS も揃える。容量は増えるがセル読み出しは 1 セル分のままで変わらない。
RES = 0.25
NLAT, NLON = 721, 1440
FHOURS = list(range(1, 49))          # f001..f048（1時間刻み・48h）
VARS = ["temp", "wind", "windDir", "precip", "cloud"]
BUCKET = "multiweather-gfs"

# NOMADS の filter CGI は混雑時に接続を掴んだまま応答しないことがある。タイムアウト無しで
# 掴むと1本のハングが8並列の枠を潰し、ジョブが25分の上限で cancel される（2026-08-06 実際に発生）。
# 1本あたりを必ず切り、取り切れなかった時間は落として先へ進む。
DL_TIMEOUT = 90          # 1本あたり秒
DL_ATTEMPTS = 3          # 同一時間の再試行回数
DL_DEADLINE = 15 * 60    # ダウンロード全体の締切（ジョブ上限25分に対する余裕）
CYCLE_TIMEOUT = 20       # サイクル探索は速く諦める（4回遡っても80秒）
MIN_HOURS = 40           # これを下回る時間しか揃わなければアップロードしない


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


def fetch(u, timeout):
    """1本取得。タイムアウト必須・中身が GRIB として小さすぎるものは失敗扱い。"""
    with urllib.request.urlopen(u, timeout=timeout) as r:
        data = r.read()
    if r.status != 200 or len(data) <= 1000:
        raise IOError(f"bad response: status={r.status} len={len(data)}")
    return data


def find_cycle():
    """利用可能な最新サイクルを検出（現在UTCから約4hの遅延を見込み、6h刻みで遡る）。"""
    base = datetime.now(timezone.utc) - timedelta(hours=4)
    base = base.replace(minute=0, second=0, microsecond=0, hour=(base.hour // 6) * 6)
    for back in range(0, 4):
        c = base - timedelta(hours=6 * back)
        date, hh = c.strftime("%Y%m%d"), c.strftime("%H")
        try:
            data = fetch(url(date, hh, 1), CYCLE_TIMEOUT)
            open("/tmp/g_001.grib2", "wb").write(data)
            return date, hh
        except Exception as e:
            print(f"cycle probe {date} {hh}Z: {e}", file=sys.stderr)
    return None


def main():
    cyc = find_cycle()
    if not cyc:
        print("ERROR: no GFS cycle available", file=sys.stderr)
        sys.exit(1)
    date, hh = cyc
    print(f"cycle: {date} {hh}Z")

    started = time.monotonic()

    def dl(fh):
        for attempt in range(1, DL_ATTEMPTS + 1):
            if time.monotonic() - started > DL_DEADLINE:
                print(f"download skip f{fh}: past deadline", file=sys.stderr)
                return False
            try:
                data = fetch(url(date, hh, fh), DL_TIMEOUT)
                open(f"/tmp/g_{fh:03d}.grib2", "wb").write(data)
                return True
            except Exception as e:
                print(f"download warn f{fh} ({attempt}/{DL_ATTEMPTS}): {e}", file=sys.stderr)
                time.sleep(2 * attempt)  # NOMADS は混雑時に絞る。少し待って譲る。
        return False

    with ThreadPoolExecutor(max_workers=8) as ex:
        ok = sum(ex.map(dl, [f for f in FHOURS if f != 1])) + 1  # f001 は find_cycle が取得済み
    print(f"downloaded {ok}/{len(FHOURS)} hours in {int(time.monotonic() - started)}s")
    # 半端なデータで R2 を上書きしない。前サイクルを残したまま失敗させ、CI の通知に出す
    # （黙って穴あきを配ると、アプリ側からは正常なデータに見える）。
    if ok < MIN_HOURS:
        print(f"ERROR: only {ok}/{len(FHOURS)} hours downloaded (need {MIN_HOURS}) — "
              "keeping the previous cycle in R2", file=sys.stderr)
        sys.exit(1)

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
            temp[i] = (rd(fh, {"typeOfLevel": "heightAboveGround", "level": 2, "shortName": "2t"})["t2m"].values - 273.15)
            u = rd(fh, {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10u"})["u10"].values
            v = rd(fh, {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10v"})["v10"].values
            spd[i] = np.sqrt(u * u + v * v)
            wdir[i] = np.degrees(np.arctan2(-u, -v)) % 360
            try:
                apcp[i] = rd(fh, {"typeOfLevel": "surface", "stepType": "accum", "shortName": "tp"})["tp"].values
            except Exception:
                apcp[i] = 0
            # 全雲量(TCDC, %)。そのまま % で表示に出す(記号の導出はしない)。
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
                    cloud[i] = rd(fh, keys)["tcc"].values
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

    # 中身が空でも「成功」で終えない。parse warn は握りつぶして続行する作りなので、
    # 全時刻が落ちても NaN のまま最後まで通ってしまう（ECMWF で実際に起きた）。
    good = int(np.isfinite(temp).any(axis=(1, 2)).sum())
    print(f"hours with data: {good}/{H}")
    if good < H * 0.8:
        print(f"ERROR: only {good}/{H} hours carry data — refusing to upload", file=sys.stderr)
        sys.exit(1)

    # 緯度行ごとに (NLON, H, V) を組み立てて逐次書き出す。
    # 0.25° では stack+transpose が同じ大きさの配列を2本増やし、1GB×3 で CI が落ちる。
    # 出力バイト列は旧方式と一致することをミニチュアで確認済み。
    V = len(VARS)
    with open("/tmp/gfs.bin", "wb") as f:
        for iy in range(NLAT):
            row = np.empty((NLON, H, V), dtype="<f4")
            row[:, :, 0] = temp[:, iy, :].T
            row[:, :, 1] = spd[:, iy, :].T
            row[:, :, 2] = wdir[:, iy, :].T
            row[:, :, 3] = precip[:, iy, :].T
            row[:, :, 4] = cloud[:, iy, :].T
            row.tofile(f)
    meta = {
        "cycle": f"{date[0:4]}-{date[4:6]}-{date[6:8]}T{hh}:00:00Z",
        "nLat": NLAT, "nLon": NLON, "res": RES, "fhours": FHOURS, "vars": VARS,
    }
    open("/tmp/gfs_meta.json", "w").write(json.dumps(meta))
    print(f"built gfs.bin {os.path.getsize('/tmp/gfs.bin')} bytes, cycle {meta['cycle']}")

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
