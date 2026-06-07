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
