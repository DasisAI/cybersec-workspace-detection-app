# Databricks notebook source
# MAGIC %md
# MAGIC # 00_materialize_rules_as_py
# MAGIC 룰 파일들을 Databricks 인터페이스에서 제거된 순수 Python 모듈(.py)로 머티리얼라이즈(Materialize)합니다.

# COMMAND ----------
# 00_materialize_rules_as_py (generic: recursive)
import os

# repo root
nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
repo_ws_root = "/".join(nb_path.split("/")[:4])   # /Repos/<user>/<repo>
repo_fs_root = f"/Workspace{repo_ws_root}"

out_fs_root = f"{repo_fs_root}/materialized_py"
out_ws_root = f"{repo_ws_root}/materialized_py"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def write_file(path: str, text: str) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def strip_notebook_markers(text: str) -> str:
    # Remove Databricks notebook markers so output is "plain .py"
    out = []
    for ln in text.splitlines():
        if ln.startswith("# Databricks notebook source"):
            continue
        if ln.startswith("# COMMAND ----------"):
            continue
        out.append(ln)
    return "\n".join(out).strip() + "\n"


def ensure_init_py(dst_dir: str) -> None:
    init_path = os.path.join(dst_dir, "__init__.py")
    if not os.path.exists(init_path):
        write_file(init_path, "")


def materialize_tree(rel_src_root: str) -> None:
    src_root = f"{repo_fs_root}/{rel_src_root}"
    dst_root = f"{out_fs_root}/{rel_src_root}"
    ensure_dir(dst_root)

    for cur_dir, _, files in os.walk(src_root):
        rel = os.path.relpath(cur_dir, src_root)  # "." or subdir
        dst_dir = os.path.normpath(os.path.join(dst_root, rel))
        ensure_dir(dst_dir)
        ensure_init_py(dst_dir)

        for fname in sorted(files):
            if fname.startswith("_"):
                continue
            if not fname.endswith(".py"):
                continue

            src_path = os.path.join(cur_dir, fname)
            dst_path = os.path.join(dst_dir, fname)

            with open(src_path, "r", encoding="utf-8") as f:
                raw = f.read()

            clean = strip_notebook_markers(raw)
            write_file(dst_path, clean)


# package root init
ensure_dir(out_fs_root)
ensure_init_py(out_fs_root)

# detections 전체 + lib 전체
materialize_tree("base/detections")  # binary/behavioral/custom 모두 포함
materialize_tree("lib")

print("OK: materialized to", out_ws_root)

# COMMAND ----------
# MAGIC %md
# MAGIC # 01_register_rules
# MAGIC 파싱된 Python 룰 모듈들을 `sandbox.audit_poc.rule_registry` 테이블에 등록합니다.

# COMMAND ----------
import re
from datetime import datetime, timezone

def extract_callable_name(py_file_text: str, fallback: str) -> str:
    """
    룰 파일에서 첫 번째 top-level def 이름을 찾아 callable_name으로 사용.
    못 찾으면 fallback(rule_id) 사용.
    """
    m = re.search(r"(?m)^\s*def\s+([A-Za-z_]\w*)\s*\(", py_file_text)
    return m.group(1) if m else fallback

rows = []
for rule_group, lookback in [("binary", 1440), ("behavioral", 43200), ("custom", 1440)]:
    folder_fs = f"{repo_fs_root}/base/detections/{rule_group}"
    
    if os.path.exists(folder_fs):
        for fname in sorted(os.listdir(folder_fs)):
            if not fname.endswith(".py") or fname.startswith("_"):
                continue

            rule_id = fname[:-3]
            # python import 경로: base.detections.binary.<rule_id>
            module_path = f"base.detections.{rule_group}.{rule_id}"

            with open(f"{folder_fs}/{fname}", "r", encoding="utf-8") as f:
                text = f.read()

            callable_name = extract_callable_name(text, fallback=rule_id)

            rows.append((rule_id, True, rule_group, module_path, callable_name, lookback))

df = spark.createDataFrame(
    rows,
    "rule_id string, enabled boolean, rule_group string, module_path string, callable_name string, lookback_minutes int"
)
df.createOrReplaceTempView("new_rules")

# created_at / updated_at은 현재시각으로 채움(없으면 MERGE에서 NULL 이슈)
spark.sql("""
MERGE INTO sandbox.audit_poc.rule_registry t
USING new_rules s
ON t.rule_id = s.rule_id
WHEN MATCHED THEN UPDATE SET
  t.enabled = s.enabled,
  t.rule_group = s.rule_group,
  t.module_path = s.module_path,
  t.callable_name = s.callable_name,
  t.lookback_minutes = s.lookback_minutes,
  t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  rule_id, enabled, rule_group, module_path, callable_name, lookback_minutes,
  schedule_hint, severity, description, tags, owner, params_json, created_at, updated_at
) VALUES (
  s.rule_id, s.enabled, s.rule_group, s.module_path, s.callable_name, s.lookback_minutes,
  NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp(), current_timestamp()
)
""")

display(spark.sql("SELECT rule_group, COUNT(*) cnt FROM sandbox.audit_poc.rule_registry GROUP BY rule_group ORDER BY rule_group"))
display(spark.sql("SELECT rule_id, rule_group, module_path, callable_name, lookback_minutes, enabled FROM sandbox.audit_poc.rule_registry ORDER BY rule_group, rule_id"))

# COMMAND ----------
# MAGIC %sql
# MAGIC INSERT INTO sandbox.audit_poc.rule_checkpoint (
# MAGIC   rule_id,
# MAGIC   last_success_end_ts,
# MAGIC   last_attempt_start_ts,
# MAGIC   last_attempt_end_ts,
# MAGIC   last_status,
# MAGIC   last_error,
# MAGIC   updated_at
# MAGIC )
# MAGIC SELECT
# MAGIC   r.rule_id,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   current_timestamp()
# MAGIC FROM sandbox.audit_poc.rule_registry r
# MAGIC LEFT ANTI JOIN sandbox.audit_poc.rule_checkpoint c
# MAGIC ON r.rule_id = c.rule_id;

# COMMAND ----------
# MAGIC %md
# MAGIC # 02_job_generator
# MAGIC 등록된 Detection 룰을 바탕으로 각각 고유한 개별 Databricks Job을 생성합니다. (1 Job = 1 Task 구조)

# COMMAND ----------
# Databricks Python SDK를 사용하여 Job 생성
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service import compute
from datetime import datetime

# 위젯으로 keep_history 옵션 받기 (디폴트는 false)
try:
    dbutils.widgets.dropdown("keep_history", "false", ["false", "true"], "Keep previous jobs")
    keep_history_str = dbutils.widgets.get("keep_history").strip().lower()
except Exception:
    keep_history_str = "false"

keep_history = keep_history_str in ["true", "1", "t", "yes", "y"]

# 노트북의 현재 컨텍스트를 사용하여 WorkspaceClient 자동 인증
w = WorkspaceClient()

runner_notebook_path = f"{repo_ws_root}/dasis_notebooks/runners/single_runner"

# 1. Active detection rule 조회 
rules_df = spark.sql("""
    SELECT rule_id, rule_group
    FROM sandbox.audit_poc.rule_registry
    WHERE enabled = true
""")
rules = rules_df.collect()

print(f"Loaded {len(rules)} active rules from registry.")
print(f"Option keep_history: {keep_history}")

# COMMAND ----------
# 2. Iterate through rules and create Jobs
current_dt_str = datetime.now().strftime("%Y%m%d_%H%M%S")

for r in rules:
    rule_id = r["rule_id"]
    rule_group = r["rule_group"]
    
    # Prefix format you can customize
    base_job_name = f"Audit_Detection_{rule_id}"
    
    if keep_history:
        job_name = f"{base_job_name}_{current_dt_str}"
    else:
        job_name = base_job_name
    
    # Define Job payload
    # 단일 Task로 구성되며, 파라미터(window_start_ts, window_end_ts, severity)를 정의합니다.
    try:
        if not keep_history:
            # 기존에 생성된 동일한 이름의 Job을 찾아 삭제하여 replace 효과를 냄
            for existing_job in w.jobs.list(name=job_name):
                print(f"Deleting existing job: {existing_job.job_id} ({job_name})")
                w.jobs.delete(job_id=existing_job.job_id)

        created_job = w.jobs.create(
            name=job_name,
            tags={
                "rule_group": rule_group,
                "pipeline": "audit_poc"
            },
            # Serverless Compute를 사용하기 위해 job_clusters 지정 및 task 내 클러스터 할당을 생략합니다.
            tasks=[
                jobs.Task(
                    task_key=f"run_{rule_id}",
                    notebook_task=jobs.NotebookTask(
                        notebook_path=runner_notebook_path,
                        base_parameters={
                            "rule_id": rule_id,
                            "window_start_ts": "", # 런타임에 주입받거나, 기본값을 설정할 수 있습니다.
                            "window_end_ts": "",
                            "severity": "medium"   # Default severity
                        }
                    )
                )
            ]
        )
        print(f"Created Job for [{rule_id}] - Job ID: {created_job.job_id}")
    except Exception as e:
        print(f"Failed to create job for [{rule_id}]: {e}")

print("Job generation completed.")

# COMMAND ----------
# MAGIC %md
# MAGIC # 03_dashboard_generator
# MAGIC 등록된 Detection 룰 탐지 결과(`sandbox.audit_poc.findings_unified` 등)를 추적하기 위한 모니터링 대시보드를 프로그래밍 방식으로 생성 또는 업데이트합니다.

# COMMAND ----------
from databricks.sdk import WorkspaceClient

try:
    dbutils.widgets.dropdown("create_dashboard", "false", ["false", "true"], "Create/Update monitoring dashboard")
    create_dashboard_str = dbutils.widgets.get("create_dashboard").strip().lower()
except Exception:
    create_dashboard_str = "false"

create_dashboard = create_dashboard_str in ["true", "1", "t", "yes", "y"]

if create_dashboard:
    w = WorkspaceClient()
    dashboard_name = "Audit_POC_Detection_Monitoring_Dashboard"
    
    print(f"Attempting to create or update dashboard: {dashboard_name}")
    
    try:
        import json
        import uuid
        from collections import defaultdict

        dashboard_id = "01f10c1584ee1ee88af7128d04ab5fd2"
        existing_dash = w.lakeview.get(dashboard_id)
        dash_data = json.loads(existing_dash.serialized_dashboard)
        
        # Ensure base structure
        if "pages" not in dash_data: dash_data["pages"] = []
        if "datasets" not in dash_data: dash_data["datasets"] = []
        
        # Find the key used for widgets (can be widgets, layouts, components)
        widget_list_key = "widgets"
        for k in ["widgets", "layouts", "components"]:
            if k in dash_data:
                widget_list_key = k
                break
        if widget_list_key not in dash_data:
            dash_data[widget_list_key] = []
            
        print(f"Found {len(dash_data['pages'])} pages and {len(dash_data['datasets'])} datasets.")

        # Get active rules
        rules_df = spark.sql("SELECT rule_id, rule_group FROM sandbox.audit_poc.rule_registry")
        rules_list = rules_df.collect()
        
        rules_by_group = defaultdict(list)
        for r in rules_list:
            rules_by_group[r["rule_group"]].append(r["rule_id"])
            
        existing_datasets = {d.get("displayName"): d for d in dash_data["datasets"]}
        existing_pages = {p.get("displayName"): p for p in dash_data["pages"]}
        
        added_datasets = 0
        added_widgets = 0
        
        for g_name, r_ids in rules_by_group.items():
            page_obj = existing_pages.get(g_name)
            if not page_obj:
                page_name = str(uuid.uuid4())
                page_obj = {"name": page_name, "displayName": g_name}
                dash_data["pages"].append(page_obj)
                existing_pages[g_name] = page_obj
            else:
                page_name = page_obj.get("name")
                
            page_widgets = [pw for pw in dash_data[widget_list_key] if pw.get("page") == page_name or pw.get("pageName") == page_name]
            
            # Find max Y to place new widgets at the bottom
            max_y = 0
            for pw in page_widgets:
                d = pw.get("display", {})
                bottom = d.get("y", 0) + d.get("height", 0)
                if bottom > max_y: max_y = bottom
                
            for rid in r_ids:
                table_name = f"sandbox.audit_poc.findings_{rid}"
                dataset_obj = existing_datasets.get(table_name)
                
                if not dataset_obj:
                    dataset_name = str(uuid.uuid4())
                    dataset_obj = {
                        "name": dataset_name,
                        "displayName": table_name,
                        "query": f"SELECT * FROM {table_name} ORDER BY event_ts DESC LIMIT 1000"
                    }
                    dash_data["datasets"].append(dataset_obj)
                    existing_datasets[table_name] = dataset_obj
                    added_datasets += 1
                else:
                    dataset_name = dataset_obj.get("name")
                    
                # Check if table widget exists
                has_widget = False
                for pw in page_widgets:
                    spec = pw.get("spec", {})
                    # Lakeview JSON schemas variations
                    if isinstance(spec, dict) and spec.get("dataset") == dataset_name:
                        has_widget = True
                        break
                    if pw.get("dataset") == dataset_name or pw.get("title") == table_name:
                        has_widget = True
                        break
                        
                if not has_widget:
                    wid = str(uuid.uuid4())
                    new_widget = {
                        "name": wid,
                        "page": page_name,
                        "pageName": page_name,
                        "title": table_name,
                        "display": {
                            "x": 0,
                            "y": max_y,
                            "width": 12,
                            "height": 9
                        },
                        "component": {
                            "type": "table",
                            "spec": {
                                "dataset": dataset_name
                            }
                        },
                        "spec": {
                            "type": "table",
                            "dataset": dataset_name
                        },
                        "dataset": dataset_name
                    }
                    dash_data[widget_list_key].append(new_widget)
                    page_widgets.append(new_widget)
                    max_y += 9
                    added_widgets += 1

        if added_datasets > 0 or added_widgets > 0:
            updated_json = json.dumps(dash_data)
            w.lakeview.update(
                dashboard_id=dashboard_id,
                display_name=existing_dash.display_name,
                serialized_dashboard=updated_json
            )
            print(f"Dashboard updated: added {added_datasets} datasets and {added_widgets} widgets.")
        else:
            print("Dashboard is already up-to-date. No new tables to add.")
            
    except Exception as e:
        print(f"Failed to create or update dashboard: {e}")
else:
    print("Skipping dashboard generation (create_dashboard=false).")
