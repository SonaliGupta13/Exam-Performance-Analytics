# Databricks notebook source
# MAGIC %md
# MAGIC **SILVER (Clean Data)**

# COMMAND ----------

## Clean students

from pyspark.sql.functions import col

silver_students = (
    spark.table("exam_analytics.bronze_students")
    .dropna()
    .withColumn("marks", col("marks").cast("int"))
)

silver_students.write.mode("overwrite").saveAsTable("exam_analytics.silver_students")


# COMMAND ----------

## Clean feedback
silver_feedback = (
    spark.table("exam_analytics.bronze_feedback")
    .dropna()
)

silver_feedback.write.mode("overwrite").saveAsTable("exam_analytics.silver_feedback")
