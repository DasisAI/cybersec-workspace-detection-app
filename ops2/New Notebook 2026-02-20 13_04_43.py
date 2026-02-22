# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import os
import sys

# 31개의 노트북이 위치한 경로 (Workspace Path)
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_ws_root = "/".join(nb_path.split("/")[:4])          # /Repos/<user>/<repo>
repo_fs_root = f"/Workspace{repo_ws_root}"               # /Workspace/Repos/<user>/<repo>
SOURCE_DIR = "/Workspace/Users/p.andrew@partner.krafton.com/cybersec-workspace-detection-app/base/detections/behavioral"

# 위에서 만든 Runner 노트북의 경로
RUNNER_NOTEBOOK_PATH = "./audit_runner"

# Job 설정
CLUSTER_ID = "your-existing-cluster-id" # 실행할 클러스터 ID (또는 Job Cluster 설정 가능)
DEFAULT_CRON = "0 0 1 * * ?" # 기본 스케줄 (매일 새벽 1시)

w = WorkspaceClient()

# COMMAND ----------

def create_audit_job(file_info):
    file_name = file_info.name
    file_path = file_info.path
    
    # .py 확장자 제거하여 로직 이름 추출 (예: access_token_created)
    logic_name = os.path.splitext(file_name)[0]
    job_name = f"[Audit] {logic_name}"
    
    print(f"Processing: {job_name}...")

    # Job에 넘길 파라미터
    base_parameters = {
        "target_notebook_path": file_path,
        "notebook_name": logic_name,
        "schedule_interval": "24 hours"
    }

    # Job Task 정의
    task = jobs.Task(
        task_key="run_detection",
        description=f"Executes audit logic for {logic_name}",
        notebook_task=jobs.NotebookTask(
            notebook_path=RUNNER_NOTEBOOK_PATH,
            base_parameters=base_parameters
        )
    )

    # 기존 Job 검색 (중복 생성 방지)
    existing_jobs = w.jobs.list(name=job_name)
    existing_job = next(existing_jobs, None)

    if existing_job:
        print(f"  - Job exists (ID: {existing_job.job_id}). Updating settings...")
        
        # Job 업데이트 (Reset하여 설정을 덮어씌움)
        w.jobs.reset(
            job_id=existing_job.job_id,
            new_settings=jobs.JobSettings(
                name=job_name,
                tasks=[task],
                # 스케줄 설정 (필요 시 주석 해제)
                # schedule=jobs.CronSchedule(
                #     quartz_cron_expression=DEFAULT_CRON,
                #     timezone_id="Asia/Seoul"
                # ),
                format=jobs.Format.MULTI_TASK
            )
        )
    else:
        print(f"  - Creating new Job...")
        created_job = w.jobs.create(
            name=job_name,
            tasks=[task],
            # schedule=jobs.CronSchedule(
            #     quartz_cron_expression=DEFAULT_CRON,
            #     timezone_id="Asia/Seoul"
            # )
        )
        print(f"  - Created Job ID: {created_job.job_id}")

# COMMAND ----------

files = dbutils.fs.ls(SOURCE_DIR)

count = 0
for f in files:
    # 파이썬 파일이고, __init__ 등이 아닌 경우만 처리
    if f.name.endswith(".py") and not f.name.startswith("_"):
        create_audit_job(f)
        count += 1

print(f"\nTotal {count} jobs processed.")
