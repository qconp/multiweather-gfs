#!/usr/bin/env python3
"""HRRR 格子（Lambert 1799×1059）のうち「米国本土＋沿岸 6 km」のセルを示すマスクを作る（2026-11-09）。

    python tools/build_hrrr_usmask.py ne_50m_admin_0_countries.geojson   # → hrrr_usmask.npz

HRRR の計算範囲はカナダ南部・メキシコ北部も含む。米国の地元モデルとして米国内だけに出すため、
Natural Earth 50m の国境（ADM0_A3 == "USA"）の内側を残し、海岸の格子を落とさないよう 2 セル（6 km）
膨らませる（国境の向こうへも 6 km はみ出すのは許容）。格子の緯度経度は Lambert の式で計算
（eccodes の latitudes/longitudes と 2000 点で一致を確認済み）。行は南から（HRRR の並びのまま）。
"""
import json
import math
import sys

import numpy as np
from matplotlib.path import Path
from scipy.ndimage import binary_dilation

NX, NY, D = 1799, 1059, 3000.0
R, LAT1, LON0 = 6371229.0, 38.5, -97.5
LA1, LO1 = 21.138123, 237.280472 - 360


def consts():
    p1 = math.radians(LAT1)
    n = math.sin(p1)
    F = math.cos(p1) * math.tan(math.pi / 4 + p1 / 2) ** n / n
    return n, F, R * F / math.tan(math.pi / 4 + p1 / 2) ** n


def forward(lat, lon):
    n, F, r0 = consts()
    dl = ((lon - LON0) + 540) % 360 - 180
    r = R * F / np.tan(np.pi / 4 + np.radians(lat) / 2) ** n
    th = n * np.radians(dl)
    return r * np.sin(th), r0 - r * np.cos(th)


def inverse(x, y):
    n, F, r0 = consts()
    dy = r0 - y
    r = np.sign(n) * np.sqrt(x * x + dy * dy)
    th = np.arctan2(x, dy)
    lat = np.degrees(2 * np.arctan((R * F / r) ** (1 / n)) - np.pi / 2)
    lon = LON0 + np.degrees(th / n)
    return lat, lon


def grid_latlon():
    x0, y0 = forward(LA1, LO1)
    X = x0 + np.arange(NX) * D
    Y = y0 + np.arange(NY) * D
    XX, YY = np.meshgrid(X, Y)
    return inverse(XX, YY)


def main():
    gj = json.load(open(sys.argv[1]))
    us = [f for f in gj["features"] if f["properties"].get("ADM0_A3") == "USA"][0]
    polys = us["geometry"]["coordinates"]
    if us["geometry"]["type"] == "Polygon":
        polys = [polys]
    lat, lon = grid_latlon()
    pts = np.column_stack([lon.ravel(), lat.ravel()])
    inside = np.zeros(pts.shape[0], bool)
    for poly in polys:
        ring = np.asarray(poly[0])
        if ring[:, 1].max() < 20 or ring[:, 0].min() < -130 and ring[:, 1].min() > 50:
            continue  # ハワイ・アラスカ等（HRRR の範囲外）
        inside |= Path(ring).contains_points(pts)
    mask = inside.reshape(NY, NX)
    mask = binary_dilation(mask, iterations=2)
    np.savez_compressed("hrrr_usmask.npz", keep=np.packbits(mask, axis=1), nx=NX, ny=NY)
    print(f"US cells: {int(mask.sum())}/{mask.size}")


if __name__ == "__main__":
    main()
