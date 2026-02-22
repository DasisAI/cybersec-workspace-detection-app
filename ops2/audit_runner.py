# Databricks notebook source
# DBTITLE 1,Setup Parameters
dbutils.widgets.text("target_notebook_path", "", "Target Notebook Path")
dbutils.widgets.text("notebook_name", "", "Notebook Name (Logic Name)")
dbutils.widgets.text("schedule_interval", "24 hours", "Lookback Interval")

target_path = dbutils.widgets.get("target_notebook_path")
logic_name = dbutils.widgets.get("notebook_name")
interval = dbutils.widgets.get("schedule_interval")

print(f"Executing Audit Logic: {logic_name} from {target_path}")

# COMMAND ----------

# DBTITLE 1,Load Common Dependencies
# 대상 노트북들이 공통적으로 의존하는 라이브러리가 있다면 여기서 먼저 로드해야 합니다.
# %run ../../../lib/common 
# (주의: exec로 실행 시 magic command(%run)는 동작하지 않으므로, 
#  필요한 공통 함수나 데코레이터(@detect)는 이 Runner 노트북에서 미리 import 되어 있어야 합니다.)
# from lib.common import detect, Output # 예시

# COMMAND ----------

# DBTITLE 1,Dynamic Code Execution
import re

# 대상 노트북 파일 읽기
with open(target_path, "r") as f:
    raw_code = f.read()

# Databricks Magic Command(%run, %md) 및 Widget 관련 코드 제거 (순수 파이썬 로직만 남김)
# 1. MAGIC 제거
clean_code = re.sub(r"^#\s*MAGIC.*", "", raw_code, flags=re.MULTILINE)
# 2. %run, %md 등 라인 제거
clean_code = re.sub(r"^\s*%.*", "", clean_code, flags=re.MULTILINE)
# 3. main 실행 블록 제거 (if __name__ == "__main__": 이후 무시)
if 'if __name__ == "__main__":' in clean_code:
    clean_code = clean_code.split('if __name__ == "__main__":')[0]

# 코드 실행하여 함수 정의 로드
exec(clean_code, globals())

# COMMAND ----------

# DBTITLE 1,Execute Logic & Get DataFrame
# 함수 이름은 파일명과 같다고 가정 (예: access_token_created)
if logic_name in globals():
    target_func = globals()[logic_name]
    
    # 함수 실행 (필요한 경우 earliest, latest 파라미터 계산하여 주입)
    # 예시 코드는 default parameter가 있으므로 바로 호출
    df_result = target_func()
    
    if df_result is not None and df_result.count() > 0:
        display(df_result) # 로그 확인용
    else:
        print("No findings detected.")
        dbutils.notebook.exit("No findings")
else:
    raise ValueError(f"Function {logic_name} not found in {target_path}")

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
target_table = f"sandbox.audit_poc.findings_{logic_name}"

print(f"Writing results to {target_table}...")

# 테이블이 존재하면 Append, 없으면 Create
(df_result.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true") # 스키마 변경 대응
    .saveAsTable(target_table)
)

print("Success.")
