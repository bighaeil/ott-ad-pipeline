"""저장소 안 Markdown 의 상대 링크·이미지·앵커가 살아 있는지 검사한다 (외부 링크는 제외).

    python scripts/ci/check_docs.py        # 문제가 있으면 목록을 찍고 exit 1

- 대상: git 이 추적하는 *.md 전부
- 코드 블록(```) 안은 링크로 보지 않는다
- 앵커는 GitHub 방식으로 만든다: 소문자 -> 문자/숫자/공백/하이픈/밑줄만 남김 -> 공백을 하이픈으로,
  같은 제목이 또 나오면 -1, -2 ...
"""
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

REPO = Path(__file__).resolve().parents[2]
LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HTML_SRC = re.compile(r"<img[^>]+src=\"([^\"]+)\"")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")


def strip_code(text):
    out, fenced = [], False
    for line in text.splitlines():
        if FENCE.match(line):
            fenced = not fenced
            out.append("")
            continue
        out.append("" if fenced else re.sub(r"`[^`]*`", "", line))
    return out


def slug(title):
    t = re.sub(r"<[^>]+>", "", title)                       # 인라인 HTML
    t = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", t)         # 링크는 글자만
    t = t.replace("`", "").strip().lower()
    t = re.sub(r"[^\w\- ]", "", t)
    return t.replace(" ", "-")


def anchors(path, cache={}):
    if path not in cache:
        seen, result = {}, set()
        text = path.read_text(encoding="utf-8")
        fenced = False
        for line in text.splitlines():
            if FENCE.match(line):
                fenced = not fenced
                continue
            m = None if fenced else HEADING.match(line)
            if m:
                s = slug(m.group(2))
                n = seen.get(s, 0)
                result.add(s if n == 0 else f"{s}-{n}")
                seen[s] = n + 1
        result.update(re.findall(r"<a\s+(?:name|id)=\"([^\"]+)\"", text))
        cache[path] = result
    return cache[path]


def main():
    files = subprocess.run(["git", "ls-files", "*.md"], cwd=REPO, capture_output=True,
                           text=True, check=True).stdout.split()
    problems, total = [], 0
    for rel in files:
        src = REPO / rel
        for lineno, line in enumerate(strip_code(src.read_text(encoding="utf-8")), 1):
            for target in LINK.findall(line) + HTML_SRC.findall(line):
                if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I):   # http:, https:, mailto: ...
                    continue
                total += 1
                path_part, _, frag = target.partition("#")
                dest = (src.parent / unquote(path_part)).resolve() if path_part else src
                where = f"{rel}:{lineno}"
                if not dest.exists():
                    problems.append(f"{where}  파일 없음      {target}")
                elif frag and dest.suffix == ".md" and unquote(frag) not in anchors(dest):
                    problems.append(f"{where}  앵커 없음      {target}")
    for p in problems:
        print(p)
    print(f"Markdown {len(files)}개, 상대 링크 {total}개 검사 - 문제 {len(problems)}개")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
