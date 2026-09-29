#!/usr/bin/env python3
"""R2 の全バケットの容量を数える（読むだけ）。無料枠 10 GB-月の監視用。wrangler r2 bucket info は 0B を返すことがある。"""
import os
import boto3
s3 = boto3.client("s3", endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
                  aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"], aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
                  region_name="auto")
total = 0
try:
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
except Exception as e:
    print("list_buckets not permitted:", e); buckets = [os.environ.get("R2_BUCKET", "multiweather-gfs")]
for b in buckets:
    n = sz = 0; top = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
        for o in page.get("Contents", []):
            n += 1; sz += o["Size"]; top.append((o["Size"], o["Key"]))
    total += sz
    print(f"{b}: {sz/1e9:.2f} GB, {n} objects")
    for s, k in sorted(top, reverse=True)[:15]:
        print(f"   {s/1e6:9.0f} MB  {k}")
print(f"TOTAL {total/1e9:.2f} GB (free tier 10 GB-month)")
LIMIT = float(os.environ.get("ALERT_GB", "9"))
if total / 1e9 > LIMIT:
    # 失敗させて GitHub の失敗通知メールで気づく（固定費ゼロの監視）。
    raise SystemExit(f"R2 usage {total/1e9:.2f} GB exceeds {LIMIT} GB — free tier is 10 GB")
