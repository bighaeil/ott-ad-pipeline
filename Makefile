# ===========================================================================
# OTT 광고 파이프라인 로컬 조작
#
# Windows 에 make 가 없으면 동일한 기능의 ./make.ps1 을 쓴다.
#   powershell -ExecutionPolicy Bypass -File .\make.ps1 up
# ===========================================================================
SHELL := /bin/bash
# Git Bash(MSYS) 의 인자 경로 자동변환 차단 (/opt/kafka/... 가 깨진다)
export MSYS_NO_PATHCONV := 1
export MSYS2_ARG_CONV_EXCL := *
DC := docker compose

.DEFAULT_GOAL := help
.PHONY: help up down restart clean ps logs health topics consume dash observe smoke flink flink-cancel archive flush load batch recon scenario-live scenario-kafka-down scenario-flink-kill scenario-burst scenario-tamper psql redis-cli e2e

help: ## 사용 가능한 타깃
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.env: ## (.env 가 없을 때만) .env.example 을 복사
	cp .env.example .env
	@echo ".env 를 .env.example 에서 만들었습니다."

e2e: .env ## E2E 테스트 (기동 -> 이벤트 주입 -> 배치·대사 -> 26개 검사, 약 7분)
	bash scripts/e2e.sh

up: .env ## 전체 기동 (collector 는 빌드 포함)
	$(DC) up -d --build
	@echo "--- 기동 대기 ---"
	@$(MAKE) --no-print-directory health

down: ## 정지 (볼륨 유지)
	$(DC) --profile batch down --remove-orphans

clean: ## 정지 + 볼륨/데이터 삭제
	$(DC) --profile batch down -v --remove-orphans
	rm -rf data/fallback/* data/checkpoints/* data/minio/events/*
	@echo "cleaned."

restart: down up ## 재기동

ps: ## 컨테이너 상태
	$(DC) ps

logs: ## 로그 따라가기 (S=서비스명)
	$(DC) logs -f --tail=200 $(S)

health: ## 각 컴포넌트 헬스 요약
	@bash scripts/health.sh

topics: ## Kafka 토픽 목록
	$(DC) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

consume: ## 토픽 실시간 확인 (T=ad.impression)
	$(DC) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(or $(T),ad.impression) --from-beginning --max-messages $(or $(N),10)

dash: ## 대시보드 / 플레이어 / 이벤트 추적기 URL 안내
	@echo "  플레이어   http://localhost:3001   (사람이 광고를 본다 - 여기서 이벤트가 태어난다)"
	@echo "  대시보드   http://localhost:8088   (전체가 얼마나 흐르나)"
	@echo "  이벤트추적 http://localhost:3000   (한 건이 어디까지 갔나)"
	@curl -fsS http://localhost:8088/api/health >/dev/null 2>&1 	  && echo "  API 정상" || echo "  API 응답 없음 - docker compose up -d dashboard"

observe: ## 토픽/카운터 실시간 관찰 (Ctrl+C 종료, 대시보드 대체용 CLI)
	@bash scripts/observe.sh

smoke: ## Collector 동작 확인 (이벤트 1건 + 트래킹 픽셀 1건)
	@bash scripts/smoke.sh

# --------------------------------------------------------------- 단계 4
flink: ## Flink SQL 파이프라인 제출 (WM=10 IDLE=5 THRESHOLD=0.5)
	@FLINK_WATERMARK_DELAY=$(or $(WM),10) SOURCE_IDLE_TIMEOUT=$(or $(IDLE),5) 		ALERT_THRESHOLD=$(or $(THRESHOLD),0.5) STARTUP_MODE=$(or $(STARTUP),latest-offset) 		bash scripts/flink-submit.sh

flink-cancel: ## 실행 중인 Flink 잡 전부 취소
	@bash scripts/flink-cancel.sh

archive: ## MinIO Parquet 원본 적재 잡 제출 (ROLLOVER='1 min')
	@ROLLOVER="$(or $(ROLLOVER),1 min)" bash scripts/flink-archive.sh

flush: ## 열려 있는 Flink 윈도우 닫기 (부하 종료 후 배치 전에 실행)
	@bash scripts/flush-windows.sh

# --------------------------------------------------------------- 단계 2 이후
load: ## 트래픽 생성 (MODE=normal|live|burst USERS=200 DURATION=60)
	@MODE=$(or $(MODE),normal) USERS=$(or $(USERS),200) DURATION=$(or $(DURATION),60) \
		bash scripts/load.sh $(ARGS)

batch: ## Spark 정산 배치 실행
	@bash scripts/batch.sh

recon: ## 대사(reconciliation) 잡 실행
	@bash scripts/recon.sh

# --------------------------------------------------------------- 단계 7 시나리오
scenario-live: ## 라이브 피크 + Collector 수동 스케일 아웃
	@bash scripts/scenario_live.sh

scenario-kafka-down: ## Kafka 정지 -> fail-open -> 재적재
	@bash scripts/scenario_kafka_down.sh

scenario-flink-kill: ## TaskManager kill -> 체크포인트 복구
	@bash scripts/scenario_flink_kill.sh

scenario-burst: ## 지연 폭주 -> late.events 급증
	@bash scripts/scenario_burst.sh

scenario-tamper: ## 서명 위조 급증 -> DLQ + alert
	@bash scripts/scenario_tamper.sh

# ----------------------------------------------------------------- 디버깅용
psql: ## psql 접속
	$(DC) exec postgres psql -U $${POSTGRES_USER:-ads} -d $${POSTGRES_DB:-adplatform}

redis-cli: ## redis-cli 접속
	$(DC) exec redis redis-cli
