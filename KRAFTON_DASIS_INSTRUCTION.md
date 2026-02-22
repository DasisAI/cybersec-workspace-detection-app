![diagram.png](./diagram.png "diagram.png")

# Databricks Audit 기반 Cybersecurity Findings 파이프라인 구축 요약 (audit_poc)

본 문서는 Databricks `cybersec-workspace-detection-app`(기존 제공 코드)의 **Finding Rule(탐지 룰)** 을 Databricks 상에서 **운영 가능한 파이프라인**으로 실행하고, 결과를 테이블로 저장/조회/대시보드화할 수 있도록 구성한 작업 내용을 “큰 그림” 관점에서 정리한 것입니다.

---

## 1) 목표

- GitHub Repo에 포함된 Finding Rule(기본 제공 + 향후 Custom Rule)을 Databricks에서 **정기 실행**한다.
- 각 룰 결과를 **룰별 테이블**에 저장한다.
- 운영/대시보드/알럿을 위해 **단일 통합 테이블**에도 적재한다.
- 룰 추가/수정이 쉬운 **유지보수 중심 구조**(레지스트리 기반)로 운영한다.
- 실행 이력 및 증분 실행을 위한 **체크포인트/런 로그**를 남긴다.

---

## 2) 전체 아키텍처(요약)

### 데이터 소스
- 고객 환경의 Audit/History 데이터는 Unity Catalog 상 아래 테이블에 존재:
  - `sandbox.audit_poc.audit`
  - `sandbox.audit_poc.history`

### 룰 실행과 결과 저장 구조
- **Rule Registry(설정/메타)** → **Runner(실행기)** → **Findings 저장(룰별 + 통합)** → **로그/체크포인트 갱신**

---

## 3) 핵심 구성요소

### 3.1 관리 테이블(운영 메타)

#### (1) `sandbox.audit_poc.rule_registry`
- “어떤 룰을 언제/어떻게 돌릴지”를 정의하는 **중앙 설정 테이블**
- 주요 역할
  - 룰 목록/활성화(enabled)
  - 룰 그룹(rule_group: binary / behavioral / custom 등)
  - 실행 대상 코드 위치(module_path/callable_name 등)
  - 기본 lookback(필요 시) 및 운영 파라미터 확장 가능

#### (2) `sandbox.audit_poc.rule_checkpoint`
- **증분 실행(incremental run)** 을 위한 체크포인트 테이블
- 주요 역할
  - 마지막 성공 실행의 끝 시각(`last_success_end_ts`)을 기록
  - 다음 실행 시 `window_start_ts` 산출에 활용하여 **중복 조회를 최소화**

#### (3) `sandbox.audit_poc.rule_run_log` (추가 예정/권장)
- 각 룰 실행 결과를 기록하는 **실행 이력 테이블**
- 주요 역할
  - 성공/실패, 처리 row 수, 소요 시간, 에러 메시지 기록
  - 운영 모니터링/장애 대응의 기준 데이터

---

### 3.2 결과 테이블(Findings)

#### (1) 룰별 결과 테이블: `sandbox.audit_poc.findings_{rule_id}`
- 각 룰의 원본 결과를 그대로 저장하는 테이블(룰마다 컬럼이 다를 수 있음)
- 운영 권장 패턴
  - 기본은 **append-only(로그/이력)** 로 저장 (재실행/재처리 이력 보존)
  - 대시보드용 “현재 상태”는 별도 통합/뷰에서 dedup 처리 권장

#### (2) 통합 결과 테이블: `sandbox.audit_poc.findings_unified`
- 룰별 결과를 **공통 스키마**로 표준화하여 적재하는 단일 테이블
- 대시보드/알럿/추이 분석용
- 공통 컬럼 예시
  - `event_ts`, `event_date`, `rule_id`, `run_id`, `window_start_ts`, `window_end_ts`, `observed_at`, `payload_json`, `dedupe_key`
- 룰별 상세 컬럼은 `payload_json`에 원본 row를 JSON으로 저장(추후 drill-down 또는 원본 테이블로 연결)

---

## 4) 운영 노트북(01~03) 역할

### 00/03) Materialize 단계: `00_materialize_rules_as_py`
- 목적: Repo 내 룰 코드(.py, 필요 시 .ipynb)를 **import 가능한 순수 파이썬 모듈 형태**로 변환/정리
- 결과: Repo 루트 하위 `materialized_py/` 패키지 생성
- 이유(핵심)
  - Databricks 노트북 포맷은 일반 `importlib` 방식에서 오류/부작용(위젯 호출 등)이 생길 수 있음
  - 운영 안정성을 위해 **런타임에서 모듈 import 가능한 형태로 표준화**가 필요
- Custom 룰이 `.ipynb`로 들어오는 경우, `.ipynb → .py` 변환 포함하도록 확장 가능

### 01) Registry 등록 단계: `01_register_rules`
- 목적: detections 폴더를 스캔하여 룰 목록을 `rule_registry`에 등록/갱신
- 결과
  - 신규 룰 추가 시 자동 반영(등록)
  - 기존 룰 변경 시 메타 업데이트
  - `rule_checkpoint`에도 신규 룰에 대한 초기 row 생성(있다면)

### 02) 실행기(Runner) 단계: `02_runner`
- 목적: `rule_registry`를 읽고 룰을 실행한 뒤 결과를 테이블에 저장하는 **메인 실행 노트북**
- 주요 기능
  - 위젯/파라미터로 실행 그룹(rule_group), 특정 룰(rule_ids), dry_run(저장 생략) 제어
  - checkpoint 기반으로 룰별 실행 윈도우(window_start/end) 계산
  - 룰 동적 호출 → 결과 DF 획득
  - 룰별 테이블 + 통합 테이블 적재
  - (권장) 실행 결과를 `rule_run_log`에 저장
  - (권장) 성공 시 checkpoint 갱신

---

## 5) 실행 흐름(운영 Runbook)

### 초기 1회(셋업)
1. Repo 클론/동기화(Repos)
2. `00/03_materialize_rules_as_py` 실행 → `materialized_py/` 생성
3. `01_register_rules` 실행 → `rule_registry`, `rule_checkpoint` 초기화/갱신
4. `02_runner`로 smoke test (특정 룰 1개 또는 소량) 후 전체 실행 검증

### 정기 실행(운영)
- `02_runner`를 Job으로 스케줄링
  - 예: `rule_group=binary`는 매일 1회(최근 24h window)
  - 예: `rule_group=behavioral`은 주 1회 또는 월 1회(최근 30d 등)
  - Custom 룰은 룰별 정책에 따라 별도 스케줄 구성 가능
- 실행 로그/체크포인트 관리(권장)
  - Job 실패 알림 설정
  - `rule_run_log` 기반 운영 모니터링

---

## 6) Custom 룰 추가/변경 시 운영 방식

### 기본 원칙
- 룰 코드는 `base/detections/<group>/` 아래에 추가
- 추가 후 다음 2단계를 수행하면 자동으로 파이프라인에 편입
  1) materialize(필요 시): `00/03_materialize_rules_as_py`
  2) registry 갱신: `01_register_rules`

### 스케줄/Lookback/파라미터 정책
- “binary/behavioral/custom” 같은 그룹은 **운영 편의상 ‘스케줄 단위’로 묶기 위한 분류**로 이해하는 것이 좋습니다.
- 커스텀 룰이 3일에 1번 등 개별 스케줄이 필요하면:
  - (권장) `rule_registry`에 `schedule_hint`(예: daily/weekly/3days) 같은 설정 컬럼을 추가하고,
  - Job을 “그룹+필터” 또는 “schedule_hint 기준”으로 나눠 구성하는 방식이 유지보수에 유리합니다.

---

## 7) 중복(duplicate) 처리 Best Practice

- `findings_{rule_id}`는 **append-only(로그)** 로 유지하는 것이 운영/감사 관점에서 안전합니다.
  - 같은 이벤트가 재실행으로 중복 적재될 수 있음(정상)
- 대시보드/알럿은 `findings_unified`를 사용하되, 아래 중 하나로 **dedup된 current view**를 제공하는 것이 일반적입니다.
  - (A) `dedupe_key` 기준 최신 `observed_at`만 남기는 View 생성
  - (B) 통합 테이블을 MERGE(Upsert) 기반으로 유지(운영 난이도↑)
- dedupe_key는 “실행 메타(observed_at/run_id/window)”가 아니라 **이벤트 자체를 대표하는 값(rule_id + event_ts + payload 등)** 으로 생성하도록 개선했습니다.

---

## 8) Job 네이밍(권장)
- 전체 룰을 레지스트리 기준으로 실행하는 Job(대표):
  - `cybersec-findings-runner`
- 필요 시 스케줄 단위로 분리:
  - `cybersec-findings-runner-binary-daily`
  - `cybersec-findings-runner-behavioral-monthly`
  - `cybersec-findings-runner-custom-<policy>`

---

## 9) 다음 권장 작업(남은 작업)

1. `rule_run_log` 적재 로직 추가(운영 필수)
2. 성공 시 `rule_checkpoint` 업데이트 로직 확정(증분 실행 안정화)
3. `findings_unified` 기반 대시보드(추이/Top rule/Top user/일자별 발생량)
4. Slack/Email 알럿(임계치/심각도 기반) 설계
5. severity/fidelity 등 메타데이터를 Repo의 선언(dscc 등)에서 파싱하여 registry에 자동 적재(선택)

---

## 부록) 용어 요약
- **Registry**: 룰 목록과 운영 설정의 단일 소스
- **Materialize**: 노트북/스크립트를 import 가능한 .py 패키지로 정리하는 단계
- **Runner**: registry 기반으로 룰을 실행하고 결과/로그를 적재하는 실행기
- **Checkpoint**: 마지막 성공 지점을 기록하여 증분 실행을 가능하게 하는 상태 테이블
- **Unified**: 대시보드/알럿/집계 편의를 위한 단일 통합 테이블
