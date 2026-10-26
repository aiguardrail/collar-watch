#!/usr/bin/env bash
# Kullanım: source .github/scripts/databranch.sh
#   setup_data_branch            -> $DATA_DIR (varsayılan ./data) içine live-data branch'ini worktree olarak açar
#   commit_push file1 file2 ...  -> $DATA_DIR içindeki dosyaları commit'ler ve push'lar
# Veri, kodu kirletmesin diye ayrı (orphan) "live-data" branch'inde tutulur.

DATA_BRANCH="${DATA_BRANCH:-live-data}"
DATA_DIR="${DATA_DIR:-data}"

setup_data_branch() {
  git config user.name "collar-watch-bot"
  git config user.email "collar-watch-bot@users.noreply.github.com"

  if [ -e "$DATA_DIR" ]; then
    echo "::error::$DATA_DIR zaten var; DATA_DIR env ile başka bir ad ver." >&2
    return 1
  fi

  if git ls-remote --exit-code --heads origin "$DATA_BRANCH" >/dev/null 2>&1; then
    git fetch --quiet origin "+refs/heads/$DATA_BRANCH:refs/remotes/origin/$DATA_BRANCH"
    git worktree add -B "$DATA_BRANCH" "$DATA_DIR" "origin/$DATA_BRANCH"
  else
    git worktree add --detach "$DATA_DIR"
    (cd "$DATA_DIR" && git checkout --orphan "$DATA_BRANCH" && git rm -rf . >/dev/null 2>&1 || true)
  fi
}

commit_push() {
  (
    cd "$DATA_DIR" || exit 0
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
