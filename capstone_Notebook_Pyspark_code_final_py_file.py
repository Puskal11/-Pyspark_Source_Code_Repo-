# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "XXXXX")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "XXXXX")


# COMMAND ----------

from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType

df1_claims = spark.read.json("s3://puskalawsbucket/capstone_puskal/claims.json")
# Count null values for each column
null_counts = {}
for col_name in df1_claims.columns:
    null_counts[col_name] = df1_claims.filter(col(col_name).isNull()).count()
# Display null counts
print("Null Value Counts:", null_counts)
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")
# Replace null values with "NA" for string-type columns
for col_name in df1_claims.columns:
    if df1_claims.schema[col_name].dataType == StringType():
        df1_claims = df1_claims.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))
# Drop duplicate and Show DataFrame including the replaced values
df1_claims_dupdropped= df1_claims.dropDuplicates()
df1_claims_dupdropped.printSchema()


df2_disease = spark.read.csv("s3://puskalawsbucket/capstone_puskal/disease.csv",header='True', inferSchema='True')
# Count null values for each column
null_counts = {}
for col_name in df2_disease.columns:
    null_counts[col_name] = df2_disease.filter(col(col_name).isNull()).count()

# Display null counts
print("Null Value Counts:",col_name,null_counts)
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")

# Replace null values with "NA" for string-type columns
for col_name in df2_disease.columns:
    if df2_disease.schema[col_name].dataType == StringType():
        df2_disease = df2_disease.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))

# Drop duplicate and Show DataFrame including the replaced values
df2_disease_dupdropped=df2_disease.dropDuplicates()
df2_disease_dupdropped.printSchema()

df3_group = spark.read.csv("s3://puskalawsbucket/capstone_puskal/group.csv",header='True', inferSchema='True')
# Count null values for each column
null_counts = {}
for col_name in df3_group.columns:
    null_counts[col_name] = df3_group.filter(col(col_name).isNull()).count()
# Display null counts
print("Null Value Counts:")
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")
# Replace null values with "NA" for string-type columns
for col_name in df3_group.columns:
    if df3_group.schema[col_name].dataType == StringType():
        df3_group = df3_group.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))
# Drop duplicate and Show DataFrame including the replaced values
df3_group_dupdropped=df3_group.dropDuplicates()
df3_group_dupdropped.printSchema()

df4_grpsubgrp = spark.read.csv("s3://puskalawsbucket/capstone_puskal/grpsubgrp.csv",header='True', inferSchema='True')
# Count null values for each column
null_counts = {}
for col_name in df4_grpsubgrp.columns:
    null_counts[col_name] = df4_grpsubgrp.filter(col(col_name).isNull()).count()
# Display null counts
print("Null Value Counts:")
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")
# Replace null values with "NA" for string-type columns
for col_name in df4_grpsubgrp.columns:
    if df4_grpsubgrp.schema[col_name].dataType == StringType():
        df4_grpsubgrp = df4_grpsubgrp.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))

# Drop duplicate and Show DataFrame including the replaced values
df4_grpsubgrp_dupdropped=df4_grpsubgrp.dropDuplicates()
df4_grpsubgrp_dupdropped.printSchema()

df5_hospital = spark.read.csv("s3://puskalawsbucket/capstone_puskal/hospital.csv",header='True', inferSchema='True')
# Count null values for each column
null_counts = {}
for col_name in df5_hospital.columns:
    null_counts[col_name] = df5_hospital.filter(col(col_name).isNull()).count()
# Display null counts
print("Null Value Counts:")
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")
# Replace null values with "NA" for string-type columns
for col_name in df5_hospital.columns:
    if df5_hospital.schema[col_name].dataType == StringType():
        df5_hospital = df5_hospital.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))
# Drop duplicate and Show DataFrame including the replaced values
df5_hospital_dupdropped=df5_hospital.dropDuplicates()
df5_hospital_dupdropped.printSchema()

df6_patient_record = spark.read.csv("s3://puskalawsbucket/capstone_puskal/Patient_records.csv",header='True', inferSchema='True')
# Count null values for each column
null_counts = {}
for col_name in df6_patient_record.columns:
    null_counts[col_name] = df6_patient_record.filter(col(col_name).isNull()).count()

# Display null counts
print("Null Value Counts:")
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")

# Replace null values with "NA" for string-type columns
for col_name in df6_patient_record.columns:
    if df6_patient_record.schema[col_name].dataType == StringType():
        df6_patient_record = df6_patient_record.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))

# Drop duplicate and Show DataFrame including the replaced values
df6_patient_record_dupdropped=df6_patient_record.dropDuplicates()
df6_patient_record_dupdropped.printSchema()

df7_subgroup = spark.read.csv("s3://puskalawsbucket/capstone_puskal/subgroup.csv",header='True', inferSchema='True')
# Count null values for each column using dict
null_counts = {}
for col_name in df7_subgroup.columns:
    null_counts[col_name] = df7_subgroup.filter(col(col_name).isNull()).count()
# Display null counts
print("Null Value Counts:",null_counts) #debugging using print
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")
# Replace null values with "NA" for string-type columns
for col_name in df7_subgroup.columns:
    if df7_subgroup.schema[col_name].dataType == StringType():
        df7_subgroup = df7_subgroup.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))
        #Can also use inbuilt function named fillNA
df7_subgroup.show() #debuggging using print
# Drop duplicate and Show DataFrame including the replaced values
df7_subgroup_dupdropped=df7_subgroup.dropDuplicates()
df7_subgroup_dupdropped.printSchema()

df8_subscriber = spark.read.csv("s3://puskalawsbucket/capstone_puskal/subscriber.csv",header='True', inferSchema='True')
# Count null values for each column
null_counts = {}
for col_name in df8_subscriber.columns:
    null_counts[col_name] = df8_subscriber.filter(col(col_name).isNull()).count()
# Display null counts
print("Null Value Counts:")
for col_name, count in null_counts.items():
    print(f"{col_name}: {count}")
# Replace null values with "NA" for string-type columns
for col_name in df8_subscriber.columns:
    if df8_subscriber.schema[col_name].dataType == StringType():
        df8_subscriber = df8_subscriber.withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))

# Drop duplicate and Show DataFrame including the replaced values
# df8_subscriber.createOrReplaceTempView("df1")
# df.createOrReplaceTempView("df1")
# df1 = spark.sql("""
#     SELECT 
#         `df`.`sub _id` as sub_id,
#         `df`.`first_name` as first_name,
#         `df`.`last_name` as last_name,
#         `df`.`Street` as Street,
#         `df`.`Birth_date` as Birth_date,
#         `df`.`Gender` as Gender,
#         `df`.`Phone` as Phone,
#         `df`.`Country` as Country,
#         `df`.`City` as City,
#         `df`.`Zip Code` as Zip_Code,
#         `df`.`Subgrp_id` as Subgrp_id,
#         `df`.`Elig_ind` as Elig_ind,
#         `df`.`eff_date` as eff_date,
#         `df`.`term_date` as term_date
#     FROM df
# """)
df8_subscriber_dupdropped=df8_subscriber.dropDuplicates()
df8_subscriber_dupdropped.printSchema()
df8_subscriber_dupdropped.show()

# df1_claims_dupdropped.printSchema()
# df2_disease_dupdropped.printSchema()
# df3_group_dupdropped.printSchema()
# df4_grpsubgrp_dupdropped.printSchema()
# df5_hospital_dupdropped.printSchema()
# df6_patient_record_dupdropped.printSchema()
# df7_subgroup_dupdropped.printSchema()
# df8_subscriber_dupdropped.printSchema()

# COMMAND ----------

# Which disease has a maximum number of claims
df1_claims_dupdropped.createOrReplaceTempView("df_claims")
df1=spark.sql("select disease_name, count(disease_name) as max_num_claims from df_claims group by disease_name order by max_num_claims desc limit 1")

df1.show()
df2 = df1.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxxxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.disease_claims").\
   option("aws_iam_role", "arn:aws:iam::xxxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "XXXX").save()



# COMMAND ----------

# Find those Subscribers having age less than 30 and they subscribe any subgroup 
from pyspark.sql.functions import current_date, datediff, col,round
df = df8_subscriber_dupdropped.withColumn("age", round(datediff(current_date(), col("Birth_date")) / 365.25, 1)) #the one here rounds the result to 1 decimal place
df.createOrReplaceTempView("df1")
#or
# df = df8_subscriber_dupdropped.withColumn("age", (year(current_date()) - (year(col("Birth_date")))) #the one here rounds the result to 1 decimal place
df.createOrReplaceTempView("df1")
df1=spark.sql("select concat(first_name,last_name) as subscriber_name,age from df1 where age <30")
#remove show fn in real code
df1.show()
df2 = df1.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.Sub_les_than_30").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()

# COMMAND ----------

# Find out which group has maximum subgroups. 
df4_grpsubgrp_dupdropped.createOrReplaceTempView("df1")
df1=spark.sql("select Grp_id, count(SubGrp_ID) as num_subgrp from df1 group by Grp_Id")
# Find the maximum count of subgroups
max_num_subgrp = df1.agg({"num_subgrp": "max"}).collect()[0][0] 
# Filter the DataFrame to include only rows with the maximum count
df2 = df1.filter(df1.num_subgrp == max_num_subgrp)

#instead TRY RANK

# Show the result
df2.show()
df2 = df2.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.max_subgrp").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()

# COMMAND ----------

#Find out hospital which serve most number of patients 
df5_hospital_dupdropped.createOrReplaceTempView("df")
df6_patient_record_dupdropped.createOrReplaceTempView("df1")
df2=spark.sql("select df.Hospital_name,count(df1.Patient_id) as most_num_patient from df join df1 on df.Hospital_id =df1.hospital_id group by df.Hospital_name order by most_num_patient desc limit 1")
df2.show()
df2 = df2.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxx.us-east-2.redshift-serverless.amazonaws.com:xxxx/dev").\
   option("dbtable", "test.max_patient").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()


# COMMAND ----------

# Find out which subgroups subscribe most number of times
#Note to self: the column name as space in them so, use backtik; eg:count(`df1`.`sub _id`)as max_subcriber
df7_subgroup_dupdropped.createOrReplaceTempView("df")
df8_subscriber_dupdropped.createOrReplaceTempView("df1")

df2=spark.sql("select df.SubGrp_Name, count(`df1`.`sub _id`) as max_subcriber from df join df1 on df.SubGrp_id = df1.Subgrp_id group by df.SubGrp_Name order by max_subcriber desc limit 1")
df2.show()
df2 = df2.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxx.us-east-2.redshift-serverless.amazonaws.com:xxx/dev").\
   option("dbtable", "test.most_subcriber").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxx").save()

# COMMAND ----------

#Find out total number of claims which were rejected
df1_claims_dupdropped.createOrReplaceTempView("df")
df1= spark.sql("select count(claim_id) as Rejected_Claims from df where Claim_Or_rejected == 'Y'")
df1.show()
df2 = df1.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.Rejected_Claims").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxx").save()

# COMMAND ----------

#From where most claims are coming (city) 
df = df1_claims_dupdropped.withColumn("patient_id",col("patient_id").cast("int"))
df.createOrReplaceTempView("df")

df6_patient_record_dupdropped.createOrReplaceTempView("df1")

df2 = spark.sql("""
    SELECT df1.city, COUNT(df.claim_id) AS max_claim_by_city
    FROM df1
    JOIN df ON df1.patient_id = df.patient_id
    GROUP BY df1.city
    ORDER BY max_claim_by_city DESC
    LIMIT 6
""")
df2.show()
df2 = df2.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.Max_claims_by_city").\
   option("aws_iam_role", "arn:aws:iam::xxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()

# COMMAND ----------

#Which groups of policies subscriber subscribe mostly Government or private
df3_group_dupdropped.createOrReplaceTempView("df")

df1 =spark.sql("select Grp_Name, Grp_Type from df where Grp_Type like 'Gov%' order by Grp_Name ")
df1.show()
df2 = df1.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.govt_groups").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()

# COMMAND ----------

#Average monthly premium subscriber pay to insurance company(note:use AVG fn)
df7_subgroup_dupdropped.createOrReplaceTempView("df")
df8_subscriber_dupdropped.createOrReplaceTempView("df1")
df1 = spark.sql("""
    SELECT avg(df.Monthly_Premium) AS avg_monthly_premium
    FROM df
    JOIN df1 ON df.SubGrp_id = df1.Subgrp_id GROUP BY df1.`sub _id` ORDER BY avg_monthly_premium desc limit 1
""")
df1.show()
df2 = df1.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.Avg_MonthyPremium").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()

# COMMAND ----------


#Find out Which group is most profitable
df3_group_dupdropped.createOrReplaceTempView("df")
df4_grpsubgrp_dupdropped.createOrReplaceTempView("df1")
df7_subgroup_dupdropped.createOrReplaceTempView("df2")


df3=spark.sql("select df.Grp_Name,df1.SubGrp_ID from df join df1 on df.Grp_Id=df1.Grp_Id")
df3.createOrReplaceTempView("df3")
df4=spark.sql("select df3.Grp_Name,df2.Monthly_Premium from df3 join df2 on df3.SubGrp_ID=df2.SubGrp_Id order by df2.Monthly_Premium desc limit 1")
df5 = df4.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.Most_Profitable_Grp").\
   option("aws_iam_role", "arn:aws:iam::xxxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()



# COMMAND ----------

#List all the patients below age of 18 who admit for cancer
#No patient with age less than 18 with cancer so, looking for patient below 45
from pyspark.sql.functions import *
df6_patient_record_dupdropped.createOrReplaceTempView("df")
# Adding a column 'age' to the DataFrame
df1 = df6_patient_record_dupdropped.withColumn("age", (datediff(current_date(), col("patient_birth_date")) / 365).cast("int"))
# Filtering rows based on age and disease name pattern
result_df = df1.filter((col("age") < 45) & ((lower(col("disease_name")).like("%can%")) | (upper(col("disease_name")).like("CAN%")))).select("Patient_name")
result_df.show()

df2 = df1.write.mode("overwrite").format("redshift").option("url", "jdbc:redshift://default-workgroup.xxxx.us-east-2.redshift-serverless.amazonaws.com:5439/dev").\
   option("dbtable", "test.Cancer_patient_below_45").\
   option("aws_iam_role", "arn:aws:iam::xxx:role/redshiftadmin").\
   option("user", "admin").\
   option("tempdir", "s3a://puskalawsbucket/tempdir/").\
   option("password", "xxxx").save()




# COMMAND ----------

#List patients who have cashless insurance and have total charges greater than or equal for Rs. 50,000
#there are no subgroup with Monthly_premium= 0 so, the question asking for Cashles Insurance is invalid.

df = df1_claims_dupdropped.withColumn("patient_id", col("patient_id").cast("int")).withColumn("claim_amount", col("claim_amount").cast("int"))
df.createOrReplaceTempView("df")
df8_subscriber_dupdropped.createOrReplaceTempView("df1")
df7_subgroup_dupdropped.createOrReplaceTempView("df2")
df6_patient_record_dupdropped.createOrReplaceTempView("df3")


spark.sql("select df.Claim_Or_Rejected,  df3.Patient_name,df.claim_amount from df join df3 on df.patient_id = df3.Patient_id where Claim_Or_Rejected like 'Na%' and claim_amount >= 50000 order by claim_amount desc ").show()
spark.sql("select * from df1 limit 10").show()
spark.sql("select * from df2 where `Monthly_Premium`<10 limit 10").show()


# COMMAND ----------

#List female patients over the age of 40 that have undergone knee surgery in the past year
# # No discease name with "knee Surgery" found so, No soln for this use case 
