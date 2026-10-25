#!/usr/bin/env bash
# Kullanım: source .github/scripts/databranch.sh
#   setup_data_branch            -> ./data klasörüne live-data branch'ini worktree olarak açar
#   commit_push file1 file2 ...  -> ./data içindeki dosyaları commit'ler ve push'lar
# Veri, kodu kirletmesin diye ayrı (orphan) "live-data" branch'inde tutulur.

DATA_BRANCH="${DATA_BRANCH:-live-data}"

setup_data_branch() {
  git config user.name "collar-watch-bot"
  git config user.email "collar-watch-bot@users.noreply.github.com"

  if git ls-remote --exit-code --heads origin "$DATA_BRANCH" >/dev/null 2>&1; then
    git fetch --quiet origin "$DATA_BRANCH"
    git worktree add -B "$DATA_BRANCH" data "origin/$DATA_BRANCH"
  else
    git worktree add --detach data
    (cd data && git checkout --orphan "$DATA_BRANCH" && git rm -rf . >/dev/null 2>&1 || true)
  fi
}

commit_push() {
  (
    cd data || exit 0
    git add "$@" 2>/dev/null || true
    if git diff --cached --quiet; then
      exit 0
    fi
    git commit -q -m "data $(date -u +%FT%TZ)"
    for i in 1 2 3; do
      git pull --rebase -q origin "$DATA_BRANCH" 2>/dev/null || true
      if git push -q origin "$DATA_BRANCH"; then
        exit 0
      fi
      sleep 5
    done
    echo "UYARI: push başarısız (3 deneme)" >&2
    exit 0
  )
}
