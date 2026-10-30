# multiweather-gfs

Multi Weather（天気比較判断）アプリの比較モデルの1つ「GFS」(NOAA・パブリックドメイン・全球) を
6時間ごとに更新するための **専用パイプライン**。GitHub Actions(無料CI)が GFS を取得・0.5°バイナリ化し、
Cloudflare R2 バケット `multiweather-gfs` に `gfs.bin` / `gfs_meta.json` として配信する。
Worker (`multi_weather_api` の `fetchGfs`) が R2 をセル単位 range read で読み、`gfs` モデルとして返す。

> このリポジトリは**ポートフォリオ本体とは分離**した最小構成（秘密情報は含めない）。
> 認証情報は GitHub Actions Secrets で注入する。

## 有効化（初回のみ）
1. **R2 APIトークン作成**（Cloudflareダッシュボード → R2 → Manage R2 API Tokens → Create）
   - 権限: Object Read & Write、バケット: `multiweather-gfs`
   - 取得: Access Key ID / Secret Access Key
2. **GitHub Secrets を登録**（このリポジトリ → Settings → Secrets and variables → Actions）
   | 名前 | 値 |
   | --- | --- |
   | `R2_ACCOUNT_ID` | `ab1d57645bfa05a89d160d3770f8f4d3` |
   | `R2_ACCESS_KEY_ID` | 手順1のAccess Key ID |
   | `R2_SECRET_ACCESS_KEY` | 手順1のSecret Access Key |
3. **Actionを実行**: Actions → 「GFS to R2 (Multi Weather)」→ Run workflow。緑になれば成功。
   以降は cron(05/11/17/23 UTC ≈ 各GFSサイクルの約5h後) で自動更新。

## メモ
- 無料枠内（1回約8分 × 4回/日 ≈ 月960分）。公開リポジトリなら Actions 無制限。
- 解像度 0.5°（約55km）。精度を上げるなら `gfs_update.py` の `RES`/間引きを 0.25° に。
- 停止したい場合は `.github/workflows/gfs-update.yml` を削除（R2の最終データは Worker 側で30h後に自動非表示）。
- Worker 側の読み取り実装・全体像は本体リポジトリの `apps/multi_weather/README.md` 参照。

## 運用メモ

### 定時実行が60日で自動停止する
GitHub は**リポジトリに60日 push が無いと、定時実行(schedule)を自動で無効化する**。
定時実行が回っているだけでは活動と見なされない。止まると GFS / ECMWF の更新が静かに
途絶えるので、**50日に一度は何でもいいので push する**こと（このメモの更新で足りる）。
無効化の直前に GitHub から警告メールが届くので、それが来たら push すれば復帰する。

- 2026-08-23 の push で60日 → 2026-10-22 が期限だった
- 2026-09-10 に push し直した（次の目安 2026-11-09）

恒久的に消すなら、パイプラインを `qconp/dev-monorepo` へ移すのが本筋。あちらは
日常的に push があるので自動停止に当たらない。ただし R2 の secret は移設先に
付いてこず、GitHub は保存済みの値を再表示しないので、Cloudflare で R2 API トークンを
発行し直す必要がある。

### dev-monorepo に実行されないコピーがある
`apps/multi_weather_api/gfs-pipeline/` に GFS 側のコピーがあるが、あちらは secret が
無く実行されない（2026-09-04 に足した workflow は 2026-09-09 に削除済み）。
**本番はこのリポジトリ**。コードは差分があるので、直すときはこちらを正とすること。

### ECMWF の 429（Too Many Requests）
配信元 data.ecmwf.int が混雑時に 429 を返す。当初は月1回程度で「放っておく」と決めていたが、
2026-09-15〜17 に **08:30 UTC の回だけ3日連続**で 0/49 になり失敗メールが続いた。
00Z サイクルの公開直後に世界中が取りに来る時間帯だったのが原因。

対処（2026-09-17）:
- `ecmwf_update.py` の `http()` に 429/5xx/通信断の再試行（Retry-After か指数バックオフ、最大8回）を追加
- 同時接続を 6 → 3 に削減
- 00Z の回を 08:30 → 09:30 UTC にずらした（他の回は変更なし）

失敗しても R2 は前回の内容を保つので配信は途切れない。それでも 09:30 の回が落ち続けるなら、
さらに遅らせる（10:30）か、失敗時に同じ job を1回だけ再実行する仕組みを足す。

### ICON-EU（2026-09-29 追加）
`icon_update.py` + `.github/workflows/icon-update.yml`。DWD open data の ICON-EU 主サイクル
（00/06/12/18Z・+120h、公開まで約 3〜4h）を 04:40/10:40/16:40/22:40 UTC に取り、`icon.bin` /
`icon_meta.json`（0.125° の地域格子、約 420MB）として同じバケットに置く。初回 2m41s・93/93 ステップ。
名称・出典の扱いは monorepo の `10_Planning/ops_dwd_trademark_APP278269-B.md` §4.5（DWD の書面）。
60 日停止の件はこのワークフローにも当てはまる。

### 高解像度化（2026-10-30 オーナー承認・monorepo 10_Planning/multi_weather_highres_proposal_2026-10-30.md §4）
- ICON-EU: 間引きをやめて原解像度 0.0625°。陸と沿岸 50 km だけ詰めた `icon_s.bin` / `icon_s_idx.bin`（旧 `icon.bin` は publish 時に消える）。
  変数に `gust`（VMAX_10M）を追加。1日4回（04:40/10:40/16:40/22:40 UTC）。
- ICON-D2: `python icon_update.py --model d2` + `.github/workflows/icon-d2-update.yml`。0.02°・+48h・3時間ごと（各回の 2h15m 後）。
  内側の矩形（緯度 44.5–56.5・経度 0.5–17.0。Worker の `ICON_D2_DOMAIN` と同じ）だけ。`icond2_s.bin` / `icond2_s_idx.bin` / `icond2_meta.json`。
- GFS: `gust`（GUST surface）・`vis`（VIS surface）を追加（8変数）。1日4回。
- ECMWF IFS / AIFS: 1日4回。IFS の 06/18Z は open data が +90h までなので、その回は ECMWF 列が +90h になる。
- 手元の検証: どのスクリプトも `--local DIR` で R2 に上げず DIR に書く（gfs / ecmwf / icon）。
- 容量（2026-10-30 手元実行の実測）: icon_s.bin 839 MB・icond2_s.bin 302 MB・gfs_s.bin 354 MB・ecmwf 06/18Z 回 172 MB（00/12Z 回は従来どおり約 270 MB）。
