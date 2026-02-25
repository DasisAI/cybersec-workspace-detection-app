# Databricks notebook source
# MAGIC %run ../../../lib/common

# COMMAND ----------

# MAGIC %md
# MAGIC ```yaml
# MAGIC dscc:
# MAGIC   author: Junghoo Kim
# MAGIC   created: '2026-02-12T08:02:41Z'
# MAGIC   modified: '2026-02-12T08:02:41Z'
# MAGIC   uuid: bafa15df-943d-4f66-ac78-d35f0b2e16e3
# MAGIC   content_type: detection
# MAGIC   detection:
# MAGIC     name: User Email Changed
# MAGIC     description: Detect user account updates that likely include an email/username change.
# MAGIC     tags: [accounts, identity, user-management]
# MAGIC     severity: medium
# MAGIC ```

# COMMAND ----------

@detect(output=Output.asDataFrame)
def user_email_changed(earliest: str = None, latest: str = None):
    """
    Detect events where a user's account information is updated, likely including an email/username change.

    Source table: sandbox.audit_poc.audit
    Signal: service_name='accounts' AND action_name='updateUser'
    Refinement: request_params JSON contains likely email fields or '@' token.

    Returns: Spark DataFrame
    """
    from pyspark.sql.functions import col, current_timestamp, expr, to_json, lit, get_json_object, lower

    earliest = earliest or (current_timestamp() - expr("INTERVAL 24 hours"))
    latest = latest or current_timestamp()

    df = spark.table("sandbox.audit_poc.audit")

    req_json = to_json(col("request_params"))

    # Heuristic: email/username updates often carry these keys (implementation varies by deployment/API path)
    new_email = get_json_object(req_json, "$.email")
    new_user_name = get_json_object(req_json, "$.userName")
    new_user_name_alt = get_json_object(req_json, "$.newUserName")
    new_email_alt = get_json_object(req_json, "$.newEmail")

    df_filtered = (
        df.filter(
            (col("service_name") == "accounts")
            & (col("action_name") == "updateUser")
            & (col("event_time") >= earliest)
            & (col("event_time") <= latest)
        )
        # only keep rows that look like they touch email/username fields (best-effort)
        .filter(
            lower(req_json).like("%email%")
            | lower(req_json).like("%username%")
            | lower(req_json).like("%userName%")
            | (req_json.like("%@%"))
        )
        .select(
            col("event_time").alias("EVENT_TIME"),
            col("action_name").alias("ACTION"),
            col("request_params.targetUserName").alias("TARGET_USER"),
            col("user_identity.email").alias("ACTOR_USER"),
            col("source_ip_address").alias("SRC_IP"),
            col("user_agent").alias("USER_AGENT"),
            col("audit_level").alias("AUDIT_LEVEL"),
            col("request_params.endpoint").alias("ENDPOINT"),
            new_email.alias("NEW_EMAIL_JSON"),
            new_email_alt.alias("NEW_EMAIL_ALT_JSON"),
            new_user_name.alias("NEW_USERNAME_JSON"),
            new_user_name_alt.alias("NEW_USERNAME_ALT_JSON"),
            req_json.alias("REQUEST_PARAMS_JSON"),
        )
        .orderBy(col("EVENT_TIME").desc())
    )

    return df_filtered

# COMMAND ----------

# Manual test (safe): only runs when you execute this notebook directly.
# This block is designed to NOT break runner imports.

def _widgets_defined() -> bool:
    try:
        dbutils.widgets.get("window_start_ts")
        dbutils.widgets.get("window_end_ts")
        return True
    except Exception:
        return False

if __name__ == "__main__":
    if not _widgets_defined():
        dbutils.widgets.text("earliest", "")
        dbutils.widgets.text("latest", "")
    earliest, latest = get_time_range_from_widgets()
    display(user_email_changed(
        earliest=dbutils.widgets.get("window_start_ts") or str(earliest),
        latest=dbutils.widgets.get("window_end_ts") or str(latest),
    ))
