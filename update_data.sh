#!/bin/bash
# CES Dashboard — データ更新スクリプト
# 使い方: Excelを上書きしたあと ./update_data.sh を実行

set -e

cd "$(dirname "$0")"

TODAY=$(date +%Y-%m-%d)

echo "=== CES Dashboard データ更新 ($TODAY) ==="

git add Mater_PythonDataFromSnowflake.xlsx
git add comments.csv 2>/dev/null || true
git commit -m "Update Excel data to $TODAY"
git push origin main

echo ""
echo "✓ GitHubへのプッシュ完了"
echo "  Streamlit Community Cloud が自動で再デプロイします（数分かかります）"
