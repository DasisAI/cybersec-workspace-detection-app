# Databricks notebook source
# MAGIC %md
# MAGIC # Threat Model: Insider Threat
# MAGIC
# MAGIC ## Risk Description
# MAGIC
# MAGIC High-performing engineers and data professionals will generally find the best or fastest way to complete their tasks, but sometimes that may do so in ways that create security impacts to their organizations. One user may think their job would be much easier if they didn't have to deal with security controls, or another might copy some data to a public storage account or other cloud resource to simplify sharing of data. We can provide education for these users, but companies should also consider providing guardrails.
# MAGIC
# MAGIC *Source: Databricks Security Best Practices for AWS (Version 2.2 - December 2025)*
# MAGIC
# MAGIC ## Detection Coverage
# MAGIC
# MAGIC Generates investigation notebook containing detections relevant to:
# MAGIC - Malicious or negligent insider activities
# MAGIC - Data movement and exfiltration
# MAGIC - Administrative abuse and privilege escalation
# MAGIC - Configuration tampering
# MAGIC - Audit evasion
# MAGIC - Destructive activities
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
threat_model = "insider_threat"
detection_list = THREAT_MODEL_MAPPINGS[threat_model]
all_detections = discover_detections(detection_list=detection_list)

print(f"Found {len(all_detections)} detections for: {threat_model}")

# COMMAND ----------

# Generate the notebook
time_range_days = int(dbutils.widgets.get("time_range_days"))
binary_hours = int(dbutils.widgets.get("binary_time_range_hours"))

notebook_content = generate_threat_model_notebook(
    threat_model=threat_model,
    threat_model_title="Insider Threat",
    threat_model_description="Detects malicious or negligent insider activities including data exfiltration, administrative abuse, configuration tampering, audit evasion, and destructive actions.",
    all_detections=all_detections,
    time_range_days=time_range_days,
    binary_time_range_hours=binary_hours
)

# Save to generated folder
output_path = f"{get_repo_root()}/generated/threat_model_insider_threat_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
w.workspace.upload(output_path, io.BytesIO(notebook_content.encode('utf-8')),
                   format=ImportFormat.SOURCE, language=Language.PYTHON)

print(f"✅ Generated: {output_path}")

# COMMAND ----------
