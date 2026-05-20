


Step 13: Data Quality Checks

invalid_email_count = bronze_df.filter(
    col("email_validation_status") == "INVALID"
).count()

invalid_phone_count = bronze_df.filter(
    col("phone_validation_status") == "INVALID"
).count()

null_patient_id_count = bronze_df.filter(
    col("patient_id").isNull()
).count()

print(f"Invalid Email Count: {invalid_email_count}")
print(f"Invalid Phone Count: {invalid_phone_count}")
print(f"Null Patient ID Count: {null_patient_id_count}")


#Step 14: Reject Bad Records


bad_records_df = bronze_df.filter(
    (col("email_validation_status") == "INVALID") |
    (col("phone_validation_status") == "INVALID") |
    (col("patient_id").isNull())
)

#Step 15: Keep Good Records

good_records_df = bronze_df.filter(
    (col("email_validation_status") == "VALID") &
    (col("phone_validation_status") == "VALID") &
    (col("patient_id").isNotNull())
)

Add audit columns

good_records_df = good_records_df.withColumn(
    "created_timestamp",
    current_timestamp()
)

good_records_df = good_records_df.withColumn(
    "pipeline_name",
    lit("silver_patient_pipeline")
)

good_records_df = good_records_df.withColumn(
    "record_source",
    lit("bronze_layer")
)


#17. write rejected records


bad_records_df.write \
    .format("delta") \
    .mode("append") \
    .save("abfss://reject@storageacc.dfs.core.windows.net/healthcare/patients")


#17. write cleaned records

good_records_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://silver@storageacc.dfs.core.windows.net/healthcare/patients")


