#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 로컬 검사 — 커밋 전에 "깨진 채로 올라가는 것" 을 막는다. 스택을 띄우지 않고 1분 안에 끝난다.
#
#   bash scripts/check.sh            (make check)
#   CHECK_BUILD=1 bash scripts/check.sh   # 이미지 9개 빌드까지 (Kotlin 컴파일, 커넥터 JAR 다운로드 확인)
#
# 전체 파이프라인 검증은 E2E (bash scripts/e2e.sh) 로 한다.
# ---------------------------------------------------------------------------
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }
skip() { printf '  \033[33mSKIP\033[0m  %s\n' "$1"; }

echo "== 로컬 검사"

# 문서: 상대 링크·이미지·앵커
if python scripts/ci/check_docs.py >/tmp/check_docs.$$ 2>&1; then ok "문서 링크 — $(tail -1 /tmp/check_docs.$$)"
else cat /tmp/check_docs.$$; bad "문서 링크"; fi
rm -f /tmp/check_docs.$$

# Python: 문법 + (있으면) pyflakes
if git ls-files '*.py' | xargs python -m py_compile 2>/dev/null; then ok "Python 문법"; else bad "Python 문법"; fi
find . -name __pycache__ -not -path "./.venv*" -prune -exec rm -rf {} + 2>/dev/null
if python -m ruff --version >/dev/null 2>&1; then
  if python -m ruff check --select F -q $(git ls-files '*.py'); then ok "pyflakes (ruff --select F)"; else bad "pyflakes"; fi
else
  skip "pyflakes — ruff 없음 (pip install ruff==0.6.9)"
fi

# 셸 문법
sh_fail=""
for f in $(git ls-files '*.sh'); do bash -n "$f" 2>/dev/null || sh_fail="$sh_fail $f"; done
if [[ -z "$sh_fail" ]]; then ok "셸 문법"; else bad "셸 문법:$sh_fail"; fi

# 줄바꿈: 저장소에 CRLF 가 들어가지 않았는가 (.gitattributes eol=lf)
crlf=$(git ls-files --eol | awk '$1=="i/crlf" || $1=="i/mixed" {print $NF}')
if [[ -z "$crlf" ]]; then ok "줄바꿈 LF"; else bad "CRLF 가 섞인 파일: $crlf"; fi

# make.ps1: BOM (없으면 Windows PowerShell 5.1 에서 한글이 깨져 실패)
if [[ "$(head -c3 make.ps1 | od -An -tx1 | tr -d ' \n')" == "efbbbf" ]]; then ok "make.ps1 UTF-8 BOM"; else bad "make.ps1 에 BOM 이 없다"; fi

# Compose: .env 없이도 설정이 성립하는가
tmp_env=""
if [[ -f .env ]]; then tmp_env=$(mktemp); mv .env "$tmp_env"; fi
if docker compose config -q 2>/dev/null && docker compose -f docker-compose.yml -f docker-compose.scale.yml config -q 2>/dev/null
then ok "Compose 설정 (.env 없이)"; else bad "Compose 설정"; fi
if [[ -n "$tmp_env" ]]; then mv "$tmp_env" .env; fi

# 이미지 빌드 (선택)
if [[ "${CHECK_BUILD:-0}" == "1" ]]; then
  if docker compose build >/tmp/check_build.$$ 2>&1; then ok "이미지 빌드"; else tail -20 /tmp/check_build.$$; bad "이미지 빌드"; fi
  rm -f /tmp/check_build.$$
else
  skip "이미지 빌드 — CHECK_BUILD=1 로 켠다"
fi

echo
if [[ $fail -eq 0 ]]; then echo "[check] 통과"; else echo "[check] 실패"; fi
exit $fail
