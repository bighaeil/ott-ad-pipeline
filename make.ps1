# ===========================================================================
# Makefile 과 동일한 타깃을 제공하는 Windows 래퍼.
#   powershell -ExecutionPolicy Bypass -File .\make.ps1 up
# Git Bash 가 있으면 bash 스크립트를 그대로 호출한다.
#
# [인코딩] 이 파일은 반드시 UTF-8 "BOM 포함" 으로 저장할 것.
#   Windows PowerShell 5.1 은 BOM 없는 파일을 CP949 로 읽어 한글이 깨지고,
#   깨진 바이트가 따옴표를 먹어 명령이 실패한다 (The term '??' is not recognized).
# ===========================================================================
param(
    [Parameter(Position = 0)]
    [string]$Target = "help",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Find-Bash {
    $c = Get-Command bash -ErrorAction SilentlyContinue
    if ($c) { return $c.Source }
    foreach ($p in @("C:\Program Files\Git\bin\bash.exe", "C:\Program Files (x86)\Git\bin\bash.exe")) {
        if (Test-Path $p) { return $p }
    }
    throw "bash 를 찾지 못했습니다. Git for Windows 를 설치하거나 WSL 을 쓰세요."
}

function Invoke-Sh([string]$script) {
    $bash = Find-Bash
    & $bash $script @Rest
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

switch ($Target.ToLower()) {

    "help" {
        Write-Host ""
        Write-Host "  up        전체 기동 (collector 빌드 포함)"
        Write-Host "  down      정지 (볼륨 유지)"
        Write-Host "  clean     정지 + 볼륨/데이터 삭제"
        Write-Host "  restart   재기동"
        Write-Host "  ps        컨테이너 상태"
        Write-Host "  logs      로그 (예: make.ps1 logs collector)"
        Write-Host "  health    헬스 요약"
        Write-Host "  topics    Kafka 토픽 describe"
        Write-Host "  consume   토픽 소비 (예: make.ps1 consume ad.impression 10)"
        Write-Host "  dash      대시보드 열기 (http://localhost:8088)"
        Write-Host "  trace     이벤트 추적기 열기 (http://localhost:3000)"
        Write-Host "  play      플레이어 화면 열기 (http://localhost:3001)"
        Write-Host "  observe   토픽/카운터 실시간 관찰 (Ctrl+C 종료)"
        Write-Host "  flink     Flink SQL 파이프라인 제출"
        Write-Host "  flink-cancel  실행 중인 Flink 잡 취소"
        Write-Host "  archive   MinIO Parquet 원본 적재 잡 제출"
        Write-Host "  flush     열려 있는 Flink 윈도우 닫기"
        Write-Host "  smoke     Collector 동작 확인"
        Write-Host "  load      트래픽 생성 (예: make.ps1 load live 600 30)"
        Write-Host "  batch     Spark 정산 배치"
        Write-Host "  recon     대사 잡"
        Write-Host ""
        Write-Host "  scenario-live         라이브 피크 + Collector 스케일 아웃"
        Write-Host "  scenario-kafka-down   Kafka 정지 -> fail-open -> 재적재"
        Write-Host "  scenario-flink-kill   TaskManager kill -> 체크포인트 복구"
        Write-Host "  scenario-burst        지연 폭주 -> late.events 급증"
        Write-Host "  scenario-tamper       서명 위조 급증 -> DLQ + alert"
        Write-Host ""
        Write-Host "  psql      psql 접속"
        Write-Host ""
    }

    "up" {
        docker compose up -d --build
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Write-Host "--- 기동 대기 ---"
        Invoke-Sh "scripts/health.sh"
    }

    "down"    { docker compose --profile batch down --remove-orphans }

    "clean"   {
        docker compose --profile batch down -v --remove-orphans
        foreach ($d in @("data/fallback", "data/checkpoints", "data/minio/events")) {
            if (Test-Path $d) { Get-ChildItem -Path $d -Force | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue }
        }
        Write-Host "cleaned."
    }

    "restart" { & $PSCommandPath down; & $PSCommandPath up }

    "ps"      { docker compose ps }

    "logs"    {
        $svc = if ($Rest -and $Rest.Count -gt 0) { $Rest[0] } else { "" }
        if ($svc) { docker compose logs -f --tail=200 $svc } else { docker compose logs -f --tail=200 }
    }

    "health"  { Invoke-Sh "scripts/health.sh" }

    "topics"  { docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe }

    "consume" {
        $t = if ($Rest -and $Rest.Count -gt 0) { $Rest[0] } else { "ad.impression" }
        $n = if ($Rest -and $Rest.Count -gt 1) { $Rest[1] } else { "10" }
        docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
            --bootstrap-server localhost:9092 --topic $t --from-beginning --max-messages $n
    }

    "dash"    { Write-Host "  http://localhost:8088"; Start-Process "http://localhost:8088" }
    "trace"   { Write-Host "  http://localhost:3000"; Start-Process "http://localhost:3000" }
    "play"    { Write-Host "  http://localhost:3001"; Start-Process "http://localhost:3001" }
    "observe" { Invoke-Sh "scripts/observe.sh" }
    "flink"   { Invoke-Sh "scripts/flink-submit.sh" }
    "flink-cancel" { Invoke-Sh "scripts/flink-cancel.sh" }
    "archive" { Invoke-Sh "scripts/flink-archive.sh" }
    "flush"   { Invoke-Sh "scripts/flush-windows.sh" }
    "smoke"   { Invoke-Sh "scripts/smoke.sh" }

    "load"    {
        # 사용법: make.ps1 load [mode] [users] [duration] [추가 인자...]
        if ($Rest -and $Rest.Count -ge 1) { $env:MODE = $Rest[0] }
        if ($Rest -and $Rest.Count -ge 2) { $env:USERS = $Rest[1] }
        if ($Rest -and $Rest.Count -ge 3) { $env:DURATION = $Rest[2] }
        $extra = if ($Rest -and $Rest.Count -gt 3) { $Rest[3..($Rest.Count - 1)] } else { @() }
        $bash = Find-Bash
        & $bash "scripts/load.sh" @extra
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    "batch"   { Invoke-Sh "scripts/batch.sh" }
    "recon"   { Invoke-Sh "scripts/recon.sh" }

    "scenario-live"       { Invoke-Sh "scripts/scenario_live.sh" }
    "scenario-kafka-down" { Invoke-Sh "scripts/scenario_kafka_down.sh" }
    "scenario-flink-kill" { Invoke-Sh "scripts/scenario_flink_kill.sh" }
    "scenario-burst"      { Invoke-Sh "scripts/scenario_burst.sh" }
    "scenario-tamper"     { Invoke-Sh "scripts/scenario_tamper.sh" }

    "psql"    { docker compose exec postgres psql -U ads -d adplatform }

    default   { Write-Host "알 수 없는 타깃: $Target"; exit 1 }
}
