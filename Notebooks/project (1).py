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


# COMMAND ----------

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


# COMMAND ----------

# MAGIC %md
# MAGIC **GOLD (Business Logic)**

# COMMAND ----------

from pyspark.sql.functions import col, when

gold_results = (
    spark.table("exam_analytics.silver_students")
    .join(
        spark.table("exam_analytics.category_cutoff"),
        on="category",
        how="left"
    )
    .withColumn(
        "pass_status",
        when(col("marks") >= col("cutoff_percentage"), "Pass")
        .otherwise("Fail")
    )
)

gold_results.write.mode("overwrite").saveAsTable("exam_analytics.gold_results")

display(gold_results.limit(10))


# COMMAND ----------

from pyspark.sql.functions import count

pass_kpi = (
    spark.table("exam_analytics.gold_results")
    .groupBy("category", "pass_status")
    .agg(count("*").alias("student_count"))
)

pass_kpi.write.mode("overwrite").saveAsTable("exam_analytics.gold_pass_kpi")


# COMMAND ----------

# MAGIC %md
# MAGIC **Merit Ranking (Top students)**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

# Filter only present students
present_students = gold_results.filter(col("attendance_status") == "Present")

# Rank only present students (NO skipped ranks)
window_spec = Window.orderBy(col("marks").desc())

ranked_df = present_students.withColumn("rank", dense_rank().over(window_spec))

ranked_df.write.mode("overwrite").saveAsTable("exam_analytics.gold_rankings")

# Show all present students in merit order
display(ranked_df.orderBy(col("rank")))


# COMMAND ----------

# MAGIC %md
# MAGIC **Pass % by Category (KPI)**

# COMMAND ----------

from pyspark.sql.functions import col, avg

pass_stats = (
    gold_results
    .groupBy("category")
    .agg(
        (avg((col("pass_status") == "Pass").cast("int")) * 100)
        .alias("pass_percentage")
    )
)

display(pass_stats)


# COMMAND ----------

# MAGIC %md
# MAGIC **Low Performers (At Risk Students)**

# COMMAND ----------

from pyspark.sql.functions import col

low_performers = (
    gold_results
    .filter(
        (col("attendance_status") == "Present") &
        (col("marks") < col("cutoff_percentage"))
    )
)

display(low_performers)
 

# COMMAND ----------

# MAGIC %md
# MAGIC **Pass % by City**

# COMMAND ----------

from pyspark.sql.functions import col, avg, round, desc

pass_percentage_city = (
    gold_results
    .filter(col("attendance_status") == "Present")   # only present students (best practice)
    .groupBy("city")
    .agg(
        round(
            avg((col("pass_status") == "Pass").cast("int")) * 100,
            2
        ).alias("pass_percentage")
    )
    .orderBy(desc("pass_percentage"))
)

pass_percentage_city.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_pass_percentage_city")

display(pass_percentage_city)
