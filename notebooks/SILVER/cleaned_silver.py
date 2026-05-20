from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re

spark = SparkSession.builder.appName("SilverLayerCleaning").getOrCreate()


bronze_df = spark.read.format("delta").load("abfss://bronze@storageacc.dfs.core.windows.net/healthcare/patients")


"""

Sample Raw Data Problems

Typical issues in raw data:

Null values
Duplicate records
Invalid emails
Incorrect phone numbers
Leading/trailing spaces
Mixed casing
Future DOBs
Invalid gender values
Special characters
Wrong date formats
Empty strings
Schema mismatch

"""

#Step 1: Standardize Column Names

def clean_column_name(col_name):
    col_name = col_name.strip().lower()
    col_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)
    col_name = re.sub(r'_+', '_', col_name)
    return col_name

for old_col in bronze_df.columns:
    bronze_df = bronze_df.withColumnRenamed(old_col, clean_column_name(old_col))


#Step 2: Trim Spaces

string_columns = [
    field.name for field in bronze_df.schema.fields
    if isinstance(field.dataType, StringType)
]

for col_name in string_columns:
    bronze_df = bronze_df.withColumn(col_name, trim(col(col_name)))


#Step 3: Convert Empty Strings to Null


for col_name in string_columns:
    bronze_df = bronze_df.withColumn(
        col_name,
        when(col(col_name) == "", None).otherwise(col(col_name))
    )


#Step 4: Standardize Text Columns


bronze_df = bronze_df.withColumn("gender", upper(col("gender")))
bronze_df = bronze_df.withColumn("city", initcap(col("city")))
bronze_df = bronze_df.withColumn("state", upper(col("state")))
bronze_df = bronze_df.withColumn("patient_name", initcap(col("patient_name")))



#Step 5: Remove Special Characters


bronze_df = bronze_df.withColumn(
    "patient_name",
    regexp_replace(col("patient_name"), "[^a-zA-Z ]", "")
)


#Step 6: Validate Email


email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'

bronze_df = bronze_df.withColumn(
    "email_validation_status",
    when(col("email").rlike(email_regex), "VALID")
    .otherwise("INVALID")
)


#Step 7: Validate Phone Number

bronze_df = bronze_df.withColumn(
    "phone_validation_status",
    when(length(col("phone_number")) == 10, "VALID")
    .otherwise("INVALID")
)

#Step 8: Handle Null Values

bronze_df = bronze_df.fillna({
    "billing_amount": 0,
    "insurance_amount": 0
})


#Replace String Nulls


bronze_df = bronze_df.fillna({
    "city": "UNKNOWN",
    "state": "UNKNOWN",
    "gender": "UNKNOWN"
})


#Step 9: Standardize Gender Values

bronze_df = bronze_df.withColumn(
    "gender",
    when(col("gender").isin("M", "MALE"), "MALE")
    .when(col("gender").isin("F", "FEMALE"), "FEMALE")
    .otherwise("OTHER")
)

#Step 10: Convert Date Columns

bronze_df = bronze_df.withColumn(
    "dob",
    to_date(col("dob"), "yyyy-MM-dd")
)

bronze_df = bronze_df.withColumn(
    "admission_date",
    to_timestamp(col("admission_date"), "yyyy-MM-dd HH:mm:ss")
)


Step 12: Remove Duplicate Records
Based on Business Key

window_spec = Window.partitionBy("patient_id").orderBy(col("updated_at").desc())

bronze_df = bronze_df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

bronze_df = bronze_df.filter(col("row_num") == 1)
bronze_df = bronze_df.drop("row_num")


