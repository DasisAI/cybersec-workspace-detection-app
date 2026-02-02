# Databricks notebook source
# MAGIC %md
# MAGIC # Threat Model: Supply Chain Attacks
# MAGIC
# MAGIC ## Risk Description
# MAGIC
# MAGIC Historically, supply chain attacks have relied upon injecting malicious code into software libraries. That code is then executed without the knowledge of the unsuspecting target. More recently, however, we have started to see the emergence of AI model and data supply chain attacks, whereby the model, its weights or the data itself is maliciously altered.
# MAGIC
# MAGIC *Source: Databricks Security Best Practices for AWS (Version 2.2 - December 2025)*
# MAGIC
# MAGIC ## Detection Coverage
# MAGIC
# MAGIC Generates investigation notebook containing detections relevant to:
# MAGIC - Credential scanning in code and libraries
# MAGIC - Secret enumeration and harvesting
# MAGIC - Token scanning activity
# MAGIC - Compromised dependencies
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `time_range_days`: Behavioral detection window (default: 30)
# MAGIC - `binary_time_range_hours`: Binary detection window (default: 24)

# COMMAND ----------

dbutils.widgets.text("time_range_days", "30", "Behavioral Window (days)")
dbutils.widgets.text("binary_time_range_hours", "24", "Binary Window (hours)")

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %run ../../../lib/threat_model_mappings

# COMMAND ----------

# MAGIC %run ../../../lib/notebook_generator_base

# COMMAND ----------

# Discover detections for this threat model
threat_model = "supply_chain"
detection_list = THREAT_MODEL_MAPPINGS[threat_model]
all_detections = discover_detections(detection_list=detection_list)

print(f"Found {len(all_detections)} detections for: {threat_model}")

# COMMAND ----------

# Generate the notebook
time_range_days = int(dbutils.widgets.get("time_range_days"))
binary_hours = int(dbutils.widgets.get("binary_time_range_hours"))

notebook_content = generate_threat_model_notebook(
    threat_model=threat_model,
    threat_model_title="Supply Chain Attacks",
    threat_model_description="Detects indicators of supply chain compromise including credential scanning, secret enumeration, and malicious code injection attempts.",
    all_detections=all_detections,
    time_range_days=time_range_days,
    binary_time_range_hours=binary_hours
)

# Save to generated folder
output_path = f"{get_repo_root()}/generated/threat_model_supply_chain_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
w.workspace.upload(output_path, io.BytesIO(notebook_content.encode('utf-8')),
                   format=ImportFormat.SOURCE, language=Language.PYTHON)

print(f"✅ Generated: {output_path}")

# COMMAND ----------
