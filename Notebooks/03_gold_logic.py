# Databricks notebook source
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
# MAGIC **Merit Ranking (Top students)[](url)**

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


# COMMAND ----------

# MAGIC %md
# MAGIC **Pass % by Category**

# COMMAND ----------

from pyspark.sql.functions import col, avg, round, desc

pass_percentage_category = (
    gold_results
    .filter(col("attendance_status") == "Present")
    .groupBy("category")
    .agg(
        round(
            avg((col("pass_status") == "Pass").cast("int")) * 100,
            2
        ).alias("pass_percentage")
    )
    .orderBy(desc("pass_percentage"))
)

pass_percentage_category.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_pass_percentage_category")

display(pass_percentage_category)


# COMMAND ----------

# MAGIC %md
# MAGIC **Attendance % by Category**

# COMMAND ----------

attendance_by_category = (
    gold_results
    .groupBy("category")
    .agg(
        round(
            avg((col("attendance_status") == "Present").cast("int")) * 100,
            2
        ).alias("attendance_percentage")
    )
)

attendance_by_category.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_attendance_category")

display(attendance_by_category)


# COMMAND ----------

# MAGIC %md
# MAGIC **Avg Marks by Category**

# COMMAND ----------

avg_marks_category = (
    gold_results
    .filter(col("attendance_status") == "Present")
    .groupBy("category")
    .agg(round(avg("marks"),2).alias("avg_marks"))
)

avg_marks_category.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_avg_marks_category")

display(avg_marks_category)


# COMMAND ----------

# MAGIC %md
# MAGIC **Attempt Trend**

# COMMAND ----------

attempt_trend = (
    gold_results
    .filter(col("attendance_status") == "Present")
    .groupBy("attempt_number")
    .agg(round(avg("marks"),2).alias("avg_marks"))
    .orderBy("attempt_number")
)

attempt_trend.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_attempt_trend")

display(attempt_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC **Low Performers (Present Only)**

# COMMAND ----------

low_performers = (
    gold_results
    .filter(
        (col("attendance_status") == "Present") &
        (col("marks") < col("cutoff_percentage"))
    )
)

low_performers.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_low_performers")

display(low_performers)

# COMMAND ----------

# MAGIC %md
# MAGIC **Top Students**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

window_spec = Window.orderBy(col("marks").desc())

top_students = (
    gold_results
    .filter(col("attendance_status") == "Present")
    .withColumn("rank", dense_rank().over(window_spec))
    .filter(col("rank") <= 10)
)

top_students.write.mode("overwrite") \
    .saveAsTable("exam_analytics.gold_top_students")

display(top_students)


# COMMAND ----------

