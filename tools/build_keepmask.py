#!/usr/bin/env python3
"""陸と沿岸 50 km の「残す範囲」表（0.05° 格子・北→南、西→東）を作る。1 回だけ手元で実行し、
結果 keepmask_0p05_50km.npz をリポジトリに置く（パイプラインは読むだけ）。

入力: Natural Earth 10m land + minor islands（パブリックドメイン）
  https://naciscdn.org/naturalearth/10m/physical/ne_10m_land.zip / ne_10m_minor_islands.zip
方法: ポリゴンを 0.05° で塗る＋全頂点の点も塗る（格子より小さい島を落とさない）→
      緯度帯ごとに km 単位の楕円で 50 km 膨らませる（経度方向は cos(緯度) で縮む）。
      |緯度| > 80° は全部残す（面積が小さく、膨らませる核が極端になるため）。
使い方: python build_keepmask.py <ne_dir> [out.npz]
"""
import sys
import numpy as np
import shapefile
from PIL import Image, ImageDraw
from scipy import ndimage

RES, NLAT, NLON, KM = 0.05, 3601, 7200, 50.0
d = sys.argv[1]
out = sys.argv[2] if len(sys.argv) > 2 else "keepmask_0p05_50km.npz"
img = Image.new("1", (NLON, NLAT), 0)
dr = ImageDraw.Draw(img)
X = lambda lon: (lon + 180.0) / RES
Y = lambda lat: (90.0 - lat) / RES
for name in ("ne_10m_land", "ne_10m_minor_islands"):
    for shp in shapefile.Reader(f"{d}/{name}").iterShapes():
        pts, parts = shp.points, list(shp.parts) + [len(shp.points)]
        for a, b in zip(parts[:-1], parts[1:]):
            ring = [(X(x), Y(y)) for x, y in pts[a:b]]
            if len(ring) >= 3:
                dr.polygon(ring, fill=1)
            for x, y in ring:
                dr.point((round(x) % NLON, min(max(round(y), 0), NLAT - 1)), fill=1)
land = np.array(img, dtype=bool)
print("land cells", land.mean())
keep = land.copy()
for b0 in range(0, NLAT, 100):
    b1 = min(b0 + 100, NLAT)
    lat = 90.0 - (b0 + b1) / 2 * RES
    if abs(lat) > 80:
        keep[b0:b1] = True
        continue
    ry = int(np.ceil(KM / (111.2 * RES)))
    rx = int(np.ceil(KM / (111.2 * np.cos(np.radians(abs(lat) + 5)) * RES)))
    yy, xx = np.mgrid[-ry:ry + 1, -rx:rx + 1]
    k = (yy * 111.2 * RES) ** 2 + (xx * 111.2 * np.cos(np.radians(lat)) * RES) ** 2 <= KM ** 2
    s0, s1 = max(b0 - ry, 0), min(b1 + ry, NLAT)
    sub = land[s0:s1]
    sub = np.concatenate([sub[:, -rx:], sub, sub[:, :rx]], axis=1)  # 経度の循環
    dil = ndimage.binary_dilation(sub, structure=k)[:, rx:-rx]
    keep[b0:b1] |= dil[b0 - s0:b0 - s0 + (b1 - b0)]
print("keep cells", keep.mean())
np.savez_compressed(out, keep=np.packbits(keep, axis=1), nlat=NLAT, nlon=NLON, res=RES)
