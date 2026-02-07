# Databricks notebook source
# MAGIC %md
# MAGIC  **Load into Dataframe**

# COMMAND ----------

students_df = spark.table("exam_analytics.students_realworld_big")
cutoff_df   = spark.table("exam_analytics.category_cutoff")
feedback_df = spark.table("exam_analytics.feedback_big")


# COMMAND ----------

# MAGIC %md
# MAGIC **BRONZE LAYER (Raw Storage)**

# COMMAND ----------

students_df.write.mode("overwrite").saveAsTable("exam_analytics.bronze_students")
cutoff_df.write.mode("overwrite").saveAsTable("exam_analytics.bronze_cutoff")
feedback_df.write.mode("overwrite").saveAsTable("exam_analytics.bronze_feedback")
